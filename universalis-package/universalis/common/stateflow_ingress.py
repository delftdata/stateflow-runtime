from dataclasses import dataclass


@dataclass
class StateflowIngress:
    host: str
    port: int
    ext_host: str
    ext_port: int
