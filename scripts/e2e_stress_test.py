import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests" / "e2e" / "python"))
from client import Client

port = int(os.environ.get("FYRODB_PORT", "8000"))

try:
    with Client(port=port, timeout=5) as r:
        r.send("SET", "foo", "bar")
        assert r.read() == "OK"
        r.send("GET", "foo")
        assert r.read() == "bar"
        print("Functional test passed.")

        start = time.time()
        for i in range(1000):
            r.send("SET", f"key{i}", f"val{i}")
            assert r.read() == "OK"
        end = time.time()
        print(f"Stress test passed: 1000 SETs in {end - start:.2f} seconds.")
except Exception as e:
    print(f"Test failed: {e}")
    sys.exit(1)
