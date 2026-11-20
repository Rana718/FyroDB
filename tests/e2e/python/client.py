import os
import socket

PORT = int(os.environ.get("FYRODB_PORT", "8000"))

class Client:
    def __init__(self, *, port=None, timeout=None, binary=False):
        self.binary = binary
        self.s = socket.create_connection(("127.0.0.1", PORT if port is None else port), timeout=timeout)
        self.f = self.s.makefile("rb")

    @staticmethod
    def _frame(args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        return out

    def send(self, *args):
        self.s.sendall(self._frame(args))

    def send_pipeline(self, commands):
        self.s.sendall(b"".join(self._frame(args) for args in commands))

    def read(self):
        line = self.f.readline()
        if len(line) < 3 or not line.endswith(b"\r\n"):
            raise ValueError("incomplete RESP header")
        t, body = line[:1], line[1:-2]
        if t == b"*":
            n = int(body)
            return None if n < 0 else [self.read() for _ in range(n)]
        if t == b"$":
            n = int(body)
            if n < 0:
                return None
            d = self.f.read(n + 2)
            if len(d) != n + 2 or d[-2:] != b"\r\n":
                raise ValueError(f"incomplete {n}-byte bulk reply")
            return d[:-2] if self.binary else d[:-2].decode()
        if t == b":":
            return int(body)
        if t == b"-":
            return Exception(body.decode())
        if t == b"+":
            return body.decode()
        raise ValueError(f"unexpected RESP type {t!r}")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.f.close()
        self.s.close()

    def call(self, *args):
        self.send(*args)
        return self.read()


def check(cond, msg):
    if not cond:
        raise SystemExit("FAIL: " + msg)
    print("ok  ", msg)


