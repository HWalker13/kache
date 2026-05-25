import os
import socket
import threading
import time

store = {}
expiry = {}  # key -> unix timestamp (float) when the key dies
store_lock = threading.Lock()

AOF_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "aof.log")
aof_lock = threading.Lock()


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
        ex_seconds = None
        if len(args) >= 4 and args[2].upper() == "EX":
            try:
                ex_seconds = float(args[3])
            except ValueError:
                return "-ERR EX value is not a number"
        with store_lock:
            store[key] = value
            if ex_seconds is not None:
                abs_ts = time.time() + ex_seconds
                expiry[key] = abs_ts
            else:
                abs_ts = None
                expiry.pop(key, None)
        if abs_ts is not None:
            append_to_aof(f"SET {key} {value} EX {abs_ts}")
        else:
            append_to_aof(f"SET {key} {value}")
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
        append_to_aof(f"DELETE {args[0]}")
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
        append_to_aof("FLUSH")
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
        return "*" + "\n*".join(live_keys)

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


def main():
    eviction_thread = threading.Thread(target=active_eviction_loop, daemon=True)
    eviction_thread.start()

    replay_aof()
    print(f"[*] Replay complete. {len(store)} key(s) loaded.")

    host, port = "0.0.0.0", 6379
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen()
        print(f"[*] Kache listening on {host}:{port}")
        while True:
            conn, addr = server.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()


if __name__ == "__main__":
    main()
