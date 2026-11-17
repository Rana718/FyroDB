import redis
import time
import threading

def run():
    r = redis.Redis(host='localhost', port=8000)
    
    def blocked_client():
        # Using pipeline to send BLPOP then EVAL
        pipe = r.pipeline()
        pipe.blpop("before-script", 0)
        pipe.eval("return 123", 0)
        try:
            results = pipe.execute()
            print(f"Blocked client got: {results}")
        except Exception as e:
            print(f"Blocked client failed: {e}")

    t = threading.Thread(target=blocked_client)
    t.start()
    
    time.sleep(0.5)
    
    # Push item to wake BLPOP
    r.rpush("before-script", "item")
    
    t.join(timeout=5)

if __name__ == "__main__":
    run()
