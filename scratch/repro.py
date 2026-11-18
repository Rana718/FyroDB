import socket
import threading
import time
import sys

# Simplified client
class Client:
    def __init__(self, port):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect(("127.0.0.1", port))
        self.f = self.s.makefile("rwb")
    def call(self, *args):
        self.f.write(f"*{len(args)}\r\n".encode())
        for arg in args:
            arg_bytes = str(arg).encode()
            self.f.write(f"${len(arg_bytes)}\r\n".encode() + arg_bytes + b"\r\n")
        self.f.flush()
        # Very simple reader
        line = self.f.readline()
        if line.startswith(b"+"): return line[1:-2].decode()
        if line.startswith(b"$"): 
            length = int(line[1:-2])
            res = self.f.read(length)
            self.f.readline() # \r\n
            return res
        return line

port = int(sys.argv[1])
stop = False

def writer():
    c = Client(port)
    i = 0
    while not stop:
        c.call("SET", "K", str(i))
        i += 1

def reader():
    c = Client(port)
    while not stop:
        # Request same key twice
        res = c.call("MGET", "K", "K")
        if isinstance(res, list) and len(res) == 2:
            if res[0] != res[1]:
                print(f"INCONSISTENT: {res}")
                sys.exit(1)

t1 = threading.Thread(target=writer)
t2 = threading.Thread(target=reader)
t1.start()
t2.start()
time.sleep(5)
stop = True
t1.join()
t2.join()
print("Passed")
