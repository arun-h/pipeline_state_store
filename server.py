import socket
import selectors
from connection import Connection
from state import StateStore
from aof import AppendOnlyLog


selector = selectors.DefaultSelector()

state_store = StateStore()
aof = AppendOnlyLog("checkpoint.aof")

HOST = "127.0.0.1"
PORT = 6379

"""
A single-threaded TCP server that:
-accepts multiple clients
-does not block
-can receive raw bytes
-prints what it receives
Usage: in terminal execute "python3 server.py" 
in other terminal execute " nc 127.0.0.1 6379 or Test-NetConnection 127.0.0.1 -Port 6379(windows OS) " and type message it will be recieved on server 
"""

def handle_command(command: str,replaying=False) -> str:
    parts = command.split()

    if not parts:
        return "ERROR empty command"

    cmd = parts[0]

    if cmd == "SET_CHECKPOINT":
        if len(parts) not in (3, 4):
            return "ERROR usage: SET_CHECKPOINT <pipeline> <value> [ttl_seconds]"

        pipeline = parts[1]
        value = parts[2]
        try:
            ttl = int(parts[3]) if len(parts) == 4 else None
        except ValueError:
            return "ERROR ttl must be integer"
        
        if not replaying:
            aof.append(command)
        state_store.set_checkpoint(pipeline, value,ttl)
        return "OK"

    if cmd == "GET_CHECKPOINT":
        if len(parts) != 2:
            return "ERROR usage: GET_CHECKPOINT <pipeline>"

        pipeline = parts[1]
        value = state_store.get_checkpoint(pipeline)

        return value if value is not None else "NULL"

    if cmd == "COMPACT":
        snapshot = state_store.dump_checkpoints()
        aof.rewrite(snapshot)
        return "OK"

    return "ERROR unknown command"

# REBUILD STATE FROM DISK
for line in aof.replay():
    cmd = line.strip()
    if cmd:
        handle_command(cmd, replaying=True)

def accept_connection(server_socket):
    client_socket, address=server_socket.accept()
    client_socket.setblocking(False)

    print("Accepted connection from", address)

    conn = Connection(client_socket)
    selector.register(client_socket, selectors.EVENT_READ, conn)

def read_from_client(conn):
    sock = conn.sock
    try:
        data = sock.recv(4096)
    except ConnectionResetError:
        data = b""

    if not data:
        print("Client disconnected")
        selector.unregister(sock)
        sock.close()
        return

    conn.buffer.extend(data)

    # Process complete lines
    while b"\n" in conn.buffer:
        line, _, remainder = conn.buffer.partition(b"\n")
        conn.buffer = remainder
        command = line.decode().strip()
        response = handle_command(command)
        sock.sendall((response + "\n").encode())

#server socket
def start_server():
    server_socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # allow quick restart
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server_socket.bind((HOST,PORT))
    server_socket.listen()

    server_socket.setblocking(False)

    selector.register(server_socket, selectors.EVENT_READ, accept_connection)

    print(f"Server listenting on {HOST}:{PORT}")

    while True:
        events=selector.select()
        for key,_ in events:
            #callback=key.data
            #callback(key.fileobj)
            data = key.data
            # Case 1: server socket → accept connection
            if callable(data):
                data(key.fileobj)
            # Case 2: client socket → read data
            else:
                read_from_client(data)

if __name__ == "__main__":
    start_server()
