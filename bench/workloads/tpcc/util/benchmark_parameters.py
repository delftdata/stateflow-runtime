class BenchmarkParameters:
    def __init__(self, loader_batch_size: int = 5000, executor_batch_size: int = 100, benchmark_duration: float = 10.0):
        self.loader_batch_size: int = loader_batch_size
        self.executor_batch_size: int = executor_batch_size
        self.benchmark_duration: float = benchmark_duration
