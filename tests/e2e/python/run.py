import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

root = Path(__file__).resolve().parents[3]
binary = Path(sys.argv[1] if len(sys.argv) > 1 else os.environ.get("FYRODB_BIN", str(Path(os.environ.get("CARGO_TARGET_DIR", root / "target")) / "release/fyro_db"))).resolve()
if not binary.is_file():
    raise SystemExit(f"Server binary does not exist: {binary}")

for name in ["blocking", "stream", "search", "text"]:
    with tempfile.TemporaryDirectory(prefix="fyrodb-e2e-") as work:
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        env = dict(os.environ, FYRODB_PORT=str(port), FYRODB_RDB_PATH=str(Path(work) / "snapshot.rdb"), PYTHONDONTWRITEBYTECODE="1")
        server = None
        with open(Path(work) / "server.log", "wb") as log:
            try:
                if name in ["blocking", "stream"]:
                    server = subprocess.Popen([str(binary)], env=env, stdout=log, stderr=log)
                    for _ in range(100):
                        if server.poll() is not None:
                            raise RuntimeError("server exited during startup")
                        try:
                            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                                break
                        except OSError:
                            time.sleep(0.05)
                    else:
                        raise RuntimeError("server did not become ready")
                subprocess.run([sys.executable, str(Path(__file__).with_name(f"{name}_e2e.py")), str(binary)], env=env, cwd=root, check=True, timeout=180)
            finally:
                if server is not None:
                    server.terminate()
                    try:
                        server.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        server.kill()
                        server.wait()
print("All Python end-to-end suites passed")
