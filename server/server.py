import argparse
import collections
import os
import socket
import threading
import time

store = {}
expiry = {}  # key -> unix timestamp (float) when the key dies
store_lock = threading.Lock()

MODE = "standalone"  # set once in main() before threads start

AOF_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "aof.log")
aof_lock = threading.Lock()

write_queue = collections.deque()
queue_lock = threading.Lock()
replication_event = threading.Event()


def is_expired(key):
    deadline = expiry.get(key)
    return deadline is not None and time.time() >= deadline


def _evict(key):
    """Delete key from both dicts. Caller must hold store_lock."""
    store.pop(key, None)
    expiry.pop(key, None)


def append_to_aof(line):
    with aof_lock:
        with open(AOF_PATH, "a") as f:
            f.write(line + "\n")


def replay_aof():
    if not os.path.exists(AOF_PATH):
        return
    with open(AOF_PATH, "r") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            parts = line.split()
            cmd = parts[0].upper()
            if cmd == "SET" and len(parts) >= 3:
                key, value = parts[1], parts[2]
                if len(parts) >= 5 and parts[3].upper() == "EX":
                    try:
                        abs_ts = float(parts[4])
                    except ValueError:
                        continue
                    if time.time() >= abs_ts:
                        continue  # already expired
                    with store_lock:
                        store[key] = value
                        expiry[key] = abs_ts
                else:
                    with store_lock:
                        store[key] = value
                        expiry.pop(key, None)
            elif cmd == "DELETE" and len(parts) >= 2:
                with store_lock:
                    _evict(parts[1])
            elif cmd == "FLUSH":
                with store_lock:
                    store.clear()
                    expiry.clear()


def active_eviction_loop():
    while True:
        time.sleep(1)
        with store_lock:
            for key in list(expiry.keys()):
                if is_expired(key):
                    _evict(key)
                    print(f"[evict] {key}")


def handle_command(line):
    parts = line.split()
    if not parts:
        return "-ERR empty command"

    cmd = parts[0].upper()
    args = parts[1:]

    if cmd == "SET":
        if len(args) < 2:
            return "-ERR wrong number of arguments"
        key, value = args[0], args[1]
        abs_ts = None
        if len(args) >= 4 and args[2].upper() == "EX":
            try:
                ex_val = float(args[3])
            except ValueError:
                return "-ERR EX value is not a number"
            # Values > 1e9 are absolute Unix timestamps (from replication/AOF);
            # smaller values are relative seconds from a regular client.
            if ex_val > 1e9:
                abs_ts = ex_val
            else:
                abs_ts = time.time() + ex_val
        with store_lock:
            store[key] = value
            if abs_ts is not None:
                expiry[key] = abs_ts
            else:
                expiry.pop(key, None)
        aof_line = f"SET {key} {value} EX {abs_ts}" if abs_ts is not None else f"SET {key} {value}"
        if MODE != "replica":
            append_to_aof(aof_line)
        if MODE == "primary":
            with queue_lock:
                write_queue.append(aof_line)
            replication_event.set()
        return "+OK"

    if cmd == "GET":
        if len(args) != 1:
            return "-ERR wrong number of arguments"
        with store_lock:
            if is_expired(args[0]):
                _evict(args[0])
                return "$nil"
            value = store.get(args[0])
        return f"${value}" if value is not None else "$nil"

    if cmd == "DELETE":
        if len(args) != 1:
            return "-ERR wrong number of arguments"
        with store_lock:
            _evict(args[0])
        aof_line = f"DELETE {args[0]}"
        if MODE != "replica":
            append_to_aof(aof_line)
        if MODE == "primary":
            with queue_lock:
                write_queue.append(aof_line)
            replication_event.set()
        return "+OK"

    if cmd == "EXISTS":
        if len(args) != 1:
            return "-ERR wrong number of arguments"
        with store_lock:
            if is_expired(args[0]):
                _evict(args[0])
                return "0"
            found = args[0] in store
        return "1" if found else "0"

    if cmd == "FLUSH":
        if args:
            return "-ERR wrong number of arguments"
        with store_lock:
            store.clear()
            expiry.clear()
        if MODE != "replica":
            append_to_aof("FLUSH")
        if MODE == "primary":
            with queue_lock:
                write_queue.append("FLUSH")
            replication_event.set()
        return "+OK"

    if cmd == "KEYS":
        if args:
            return "-ERR wrong number of arguments"
        with store_lock:
            live_keys = []
            for key in list(store.keys()):
                if is_expired(key):
                    _evict(key)
                else:
                    live_keys.append(key)
        if not live_keys:
            return "+none"
        return "*" + "\n*".join(live_keys) + "\n+done"

    return f"-ERR unknown command '{cmd}'"


def handle_client(conn, addr):
    print(f"[+] Connection opened: {addr}")
    try:
        with conn:
            buf = b""
            while True:
                chunk = conn.recv(1024)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    message = line.decode(errors="replace").rstrip("\r")
                    print(f"[{addr}] {message}")
                    response = handle_command(message)
                    conn.sendall((response + "\n").encode())
    finally:
        print(f"[-] Connection closed: {addr}")


def replication_loop(replica_host, replica_port):
    while True:
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((replica_host, replica_port))
            rfile = sock.makefile("r")
            print(f"[repl] Connected to replica at {replica_host}:{replica_port}")
            replication_event.clear()
            while True:
                with queue_lock:
                    cmd = write_queue[0] if write_queue else None
                if cmd is not None:
                    # Send and wait for +OK before removing from queue so the
                    # command survives a disconnect and gets retried on reconnect.
                    sock.sendall((cmd + "\n").encode())
                    response = rfile.readline()
                    if not response:  # EOF — replica closed the connection
                        raise ConnectionError("replica disconnected")
                    with queue_lock:
                        if write_queue and write_queue[0] == cmd:
                            write_queue.popleft()
                else:
                    replication_event.wait(timeout=1)
                    replication_event.clear()
        except Exception as e:
            print(f"[repl] {e}, reconnecting in 2s...")
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass
            time.sleep(2)


def main():
    global MODE

    parser = argparse.ArgumentParser(description="Kache server")
    parser.add_argument("--mode", default="standalone",
                        choices=["standalone", "primary", "replica"])
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replica-host", default="127.0.0.1")
    parser.add_argument("--replica-port", type=int, default=6380)
    args = parser.parse_args()
    MODE = args.mode

    threading.Thread(target=active_eviction_loop, daemon=True).start()

    if MODE != "replica":
        replay_aof()
        print(f"[*] Replay complete. {len(store)} key(s) loaded.")

    if MODE == "primary":
        threading.Thread(
            target=replication_loop,
            args=(args.replica_host, args.replica_port),
            daemon=True,
        ).start()

    host, port = "0.0.0.0", args.port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen()
        print(f"[*] Kache ({MODE}) listening on {host}:{port}")
        while True:
            conn, addr = server.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    main()
