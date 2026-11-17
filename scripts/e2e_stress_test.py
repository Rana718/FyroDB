import redis
import time
import sys

# Connect to the docker container on localhost:8000 (mapped port)
try:
    r = redis.Redis(host='localhost', port=8000, db=0)
    # Simple functional test
    r.set('foo', 'bar')
    assert r.get('foo') == b'bar'
    print("Functional test passed.")

    # Simple stress test
    start = time.time()
    for i in range(1000):
        r.set(f'key{i}', f'val{i}')
    end = time.time()
    print(f"Stress test passed: 1000 SETs in {end - start:.2f} seconds.")

except Exception as e:
    print(f"Test failed: {e}")
    sys.exit(1)
