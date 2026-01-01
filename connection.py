class Connection:
    def __init__(self, sock):
        self.sock = sock
        self.buffer = bytearray()
