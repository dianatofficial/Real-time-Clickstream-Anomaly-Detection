import time
def retry_13(fn, attempts=3):
    for i in range(attempts):
        try:
            return fn()
        except Exception:
            time.sleep(1)
