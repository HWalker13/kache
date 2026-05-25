import socket


class KacheError(Exception):
    pass


class KacheClient:
    def __init__(self, host="127.0.0.1", port=6379):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((host, port))
        self._file = self._sock.makefile("r")

    def _send(self, command):
        self._sock.sendall((command + "\n").encode())

    def _read_line(self):
        return self._file.readline().rstrip("\r\n")

    def _parse(self, line):
        if line.startswith("+"):
            return line[1:]
        if line.startswith("$"):
            val = line[1:]
            return None if val == "nil" else val
        if line.startswith("-"):
            raise KacheError(line[1:])
        return line

    def set(self, key, value, ttl=None):
        if ttl is not None:
            self._send(f"SET {key} {value} EX {ttl}")
        else:
            self._send(f"SET {key} {value}")
        self._parse(self._read_line())
        return True

    def get(self, key):
        self._send(f"GET {key}")
        return self._parse(self._read_line())

    def delete(self, key):
        self._send(f"DELETE {key}")
        self._parse(self._read_line())
        return True

    def exists(self, key):
        self._send(f"EXISTS {key}")
        line = self._read_line()
        if line.startswith("-"):
            raise KacheError(line[1:])
        return line == "1"

    def flush(self):
        self._send("FLUSH")
        self._parse(self._read_line())
        return True

    def keys(self):
        self._send("KEYS")
        result = []
        while True:
            line = self._read_line()
            if line.startswith("*"):
                result.append(line[1:])
            elif line.startswith("-"):
                raise KacheError(line[1:])
            else:
                break  # +none or +done
        return result

    def close(self):
        self._file.close()
        self._sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


if __name__ == "__main__":
    with KacheClient("127.0.0.1", 6379) as c:
        print("flush()          :", c.flush())
        print("set(foo, bar)    :", c.set("foo", "bar"))
        print("set(tmp, 42, 10) :", c.set("tmp", "42", ttl=10))
        print("get(foo)         :", c.get("foo"))
        print("get(missing)     :", c.get("missing"))
        print("exists(foo)      :", c.exists("foo"))
        print("exists(missing)  :", c.exists("missing"))
        print("keys()           :", c.keys())
        print("delete(foo)      :", c.delete("foo"))
        print("keys() after del :", c.keys())
        print("flush()          :", c.flush())
        print("keys() after flush:", c.keys())
