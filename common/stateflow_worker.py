from dataclasses import dataclass


@dataclass
class StateflowWorker:
    host: str
    port: int
