import time

class StateStore:
    def __init__(self):
        # pipeline -> (value, expiry_timestamp or None)
        self.checkpoints = {}

    def set_checkpoint(self, pipeline, value, ttl_seconds=None):
        expiry = None
        if ttl_seconds is not None:
            expiry = time.time() + ttl_seconds

        self.checkpoints[pipeline] = (value, expiry)

    def get_checkpoint(self, pipeline):
        entry = self.checkpoints.get(pipeline)
        if not entry:
            return None

        value, expiry = entry
        print("DEBUG TTL:", pipeline, "expiry =", expiry, "now =", time.time())

        if expiry is not None and time.time() > expiry:
            # expired → delete
            del self.checkpoints[pipeline]
            return None
    
        return value
    
    def dump_checkpoints(self):
    #Returns a snapshot of non-expired checkpoints.
        snapshot = {}
        for pipeline in list(self.checkpoints.keys()):
            value = self.get_checkpoint(pipeline)
            if value is not None:
                snapshot[pipeline] = value
        return snapshot

