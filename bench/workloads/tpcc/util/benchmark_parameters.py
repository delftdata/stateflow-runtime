class BenchmarkParameters:
    def __init__(
            self,
            loader_batch_size: int = 1000,
            executor_batch_size: int = 50,
            benchmark_duration: float = 60.0,
    ):
        self.loader_batch_size: int = loader_batch_size
        self.loader_batch_wait_time: float = 0.1
        self.executor_batch_size: int = executor_batch_size
        self.executor_batch_wait_time: float = 0.5
        self.benchmark_duration: float = benchmark_duration
