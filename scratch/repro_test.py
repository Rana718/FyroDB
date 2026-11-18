import subprocess
import time
import json
import os
import sys

# Test configuration
WORKERS = 6
TEST_DURATION = 10
KEY = b"key"

def run_test():
    # Start the server (assuming it's already running or we can start it)
    # The previous messages suggest it's running.
    
    # Run the race reproduction script (assumed to exist in workspace)
    # Based on the summary, it was "temp/safety-benchmark-docs-20261118-2105-multi/failure-report.json"
    
    # For now, let's just try running the existing reproduction script
    cmd = [sys.executable, "reproduce_race.py"]
    
    # We want to see if it fails.
    try:
        subprocess.run(cmd, check=True, timeout=TEST_DURATION + 5)
        print("Test passed!")
        return True
    except subprocess.CalledProcessError:
        print("Test failed!")
        return False
    except subprocess.TimeoutExpired:
        print("Test timed out!")
        return False

if __name__ == "__main__":
    if run_test():
        exit(0)
    else:
        exit(1)
