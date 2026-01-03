import socket

"""
Open a TCP connection tos server
Send a single command
Read a single response
Close connection
"""
HOST = "127.0.0.1"
PORT = 6379


class StateClient:
    def __init__(self, host=HOST, port=PORT):
        self.host = host
        self.port = port

    def _send_command(self, command: str) -> str:
        """
        Sends one command to the state store and returns the response.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.host, self.port))

            # protocol: one command per line
            sock.sendall((command + "\n").encode())

            # read response (single line)
            data = sock.recv(4096)

            return data.decode().strip()

    def get_checkpoint(self, pipeline_name: str) -> str | None:
        response = self._send_command(f"GET_CHECKPOINT {pipeline_name}")
        return None if response == "NULL" else response

    def set_checkpoint(self, pipeline_name: str, value: str, ttl: int | None = None):
        if ttl is not None:
            cmd = f"SET_CHECKPOINT {pipeline_name} {value} {ttl}"
        else:
            cmd = f"SET_CHECKPOINT {pipeline_name} {value}"

        response = self._send_command(cmd)
        if response != "OK":
            raise RuntimeError(f"Failed to set checkpoint: {response}")
