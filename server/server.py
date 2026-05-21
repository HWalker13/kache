import socket
import threading
import time

store = {}
expiry = {}  # key -> unix timestamp (float) when the key dies
store_lock = threading.Lock()


def is_expired(key):
    deadline = expiry.get(key)
    return deadline is not None and time.time() >= deadline


def _evict(key):
    """Delete key from both dicts. Caller must hold store_lock."""
    store.pop(key, None)
    expiry.pop(key, None)


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
                expiry[key] = time.time() + ex_seconds
            else:
                expiry.pop(key, None)
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
