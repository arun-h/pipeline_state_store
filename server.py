import socket
import selectors
from connection import Connection
"""
A single-threaded TCP server that:
-accepts multiple clients
-does not block
-can receive raw bytes
-prints what it receives
Usage: in terminal execute "python3 server.py" 
in other terminal execute " nc 127.0.0.1 6379 or Test-NetConnection 127.0.0.1 -Port 6379(windows OS) " and type message it will be recieved on server 
"""
HOST = "127.0.0.1"
PORT = 6379

selector = selectors.DefaultSelector()

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

        print("Complete message:", line)


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
