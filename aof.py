import os
import tempfile

class AppendOnlyLog:
    def __init__(self, filename):
        self.filename = filename

    def append(self, command: str):
        with open(self.filename, "a") as f:
            f.write(command + "\n")

    def replay(self):
        try:
            with open(self.filename, "r") as f:
                return f.readlines()
        except FileNotFoundError:
            return []
        
    def rewrite(self, state_snapshot: dict):
    #Rewrite AOF using current state only. 
        dir_name = os.path.dirname(self.filename) or "."
        fd, temp_path = tempfile.mkstemp(dir=dir_name)

        with os.fdopen(fd, "w") as f:
            for pipeline, value in state_snapshot.items():
                f.write(f"SET_CHECKPOINT {pipeline} {value}\n")

        os.replace(temp_path, self.filename)
