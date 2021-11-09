import socket
from timeit import default_timer as timer

s = None
for res in socket.getaddrinfo('127.0.0.1', 8887, socket.AF_UNSPEC, socket.SOCK_STREAM):
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
s.sendall(b'PROTOCOL')
end = timer()
s.close()
