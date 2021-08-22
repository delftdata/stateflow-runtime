from utils import transmit_tcp_no_response

# containers = [("0.0.0.0", 8886), ("0.0.0.0", 8887), ("0.0.0.0", 8888), ("0.0.0.0", 8889)]
containers = [("0.0.0.0", 8887), ("0.0.0.0", 8888), ("0.0.0.0", 8889)]
peers = [("worker-0", 8888), ("worker-1", 8888), ("worker-2", 8888), ("worker-3", 8888)]

# for host, port in containers:
#     transmit_tcp_no_response(host, port, peers, com_type="GET_PEERS")

for host, port in containers:
    transmit_tcp_no_response(host, port, peers, com_type="SC1")
