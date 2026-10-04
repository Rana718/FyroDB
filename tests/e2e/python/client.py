import os
import socket

PORT = int(os.environ.get("FYRODB_PORT", "8000"))

class Client:
    def __init__(self):
        self.s = socket.create_connection(("127.0.0.1", PORT))
        self.f = self.s.makefile("rb")

    def send(self, *args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.s.sendall(out)

    def read(self):
        line = self.f.readline()
        t, body = line[:1], line[1:-2]
        if t == b"*":
            n = int(body)
            return None if n < 0 else [self.read() for _ in range(n)]
        if t == b"$":
            n = int(body)
            if n < 0:
                return None
            d = self.f.read(n + 2)
            return d[:-2].decode()
        if t == b":":
            return int(body)
        if t == b"-":
            return Exception(body.decode())
        return body.decode()

    def call(self, *args):
        self.send(*args)
        return self.read()


def check(cond, msg):
    if not cond:
        raise SystemExit("FAIL: " + msg)
    print("ok  ", msg)


