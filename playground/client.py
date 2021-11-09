import asyncio
import socket
import struct
from timeit import default_timer as timer


class NetworkingManager:

    def __init__(self):
        self.open_socket_connections: dict[tuple[str, int], socket.socket] = {}

    def create_socket_connection(self, host: str, port):
        s = None
        for res in socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM):
            af, socktype, proto, canonname, sa = res
            try:
                s = socket.socket(af, socktype, proto)
            except OSError:
                s = None
                continue
            try:
                s.connect(sa)
            except OSError:
                s.close()
                s = None
                continue
            break
        if s is None:
            print(f'Could not open socket for host: {host}:{port}')
        else:
            self.open_socket_connections[(host, port)] = s

    def close_socket_connection(self, host: str, port: int):
        if (host, port) in self.open_socket_connections:
            self.open_socket_connections[(host, port)].close()

    def send_message(self, host, port, msg: bytes):
        sock = self.open_socket_connections[(host, port)]
        # Prefix each message with a 4-byte length (network byte order)
        msg = struct.pack('>I', len(msg)) + msg
        sock.sendall(msg)

    def receive_message(self, host, port):
        sock = self.open_socket_connections[(host, port)]
        # Read message length and unpack it into an integer
        raw_message_len = self.__receive_all(sock, 4)
        if not raw_message_len:
            return None
        message_len = struct.unpack('>I', raw_message_len)[0]
        # Read the message data
        return self.__receive_all(sock, message_len)

    @staticmethod
    def __receive_all(sock, n):
        # Helper function to receive n bytes or return None if EOF is hit
        data = bytearray()
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data


class EchoServerProtocol(asyncio.Protocol):

    def connection_made(self, transport):
        self.peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(self.peername))
        self.transport = transport

    def data_received(self, data):
        message = data.decode()
        if message == 'SOC':
            s = None
            for res in socket.getaddrinfo('127.0.0.1', 8888, socket.AF_UNSPEC, socket.SOCK_STREAM):
                af, socktype, proto, canonname, sa = res
                try:
                    s = socket.socket(af, socktype, proto)
                except OSError as msg:
                    s = None
                    continue
                try:
                    s.connect(sa)
                except OSError as msg:
                    s.close()
                    s = None
                    continue
                break
            if s is None:
                print('could not open socket')

            start = timer()
            s.sendall(b'Hello, world')
            end = timer()
            print(f'The time it took {(end - start) * 10e3} ms')
            data = s.recv(1024)  # Erxete edw kai oxi se neo protocol
            print('Received', repr(data))
            s.sendall(b'Hello, world')
            data = s.recv(1024)
            print('Received', repr(data))
            s.close()
        elif message == 'PROTOCOL':
            networking_manager.send_message('127.0.0.1', 8888, b'Hello, world')
        else:
            print('Data received: {!r}'.format(message))


    def connection_lost(self, exc) -> None:
        print('Connection lost from {}'.format(self.peername))


async def main():
    # Get a reference to the event loop as we plan to use
    # low-level APIs.
    loop = asyncio.get_running_loop()

    server = await loop.create_server(
        lambda: EchoServerProtocol(),
        '127.0.0.1', 8887)

    async with server:
        await server.serve_forever()

networking_manager = NetworkingManager()
networking_manager.create_socket_connection('127.0.0.1', 8888)
asyncio.run(main())
