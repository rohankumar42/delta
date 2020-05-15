from collections import defaultdict
from queue import Queue

from utils import avg


class RuntimePredictor(object):

    def __init__(self, endpoints):
        self.endpoints = endpoints

    def predict(self, func, endpoint, *args, **kwargs):
        raise NotImplementedError

    def update(self, task_info, new_runtime):
        raise NotImplementedError

    def __call__(self, *args, **kwargs):
        return self.predict(*args, **kwargs)


class RollingAverage(RuntimePredictor):

    def __init__(self, endpoints, last_n=3):
        super().__init__(endpoints)
        self.last_n = last_n
        self.runtimes = defaultdict(lambda: defaultdict(Queue))
        self.avg_runtime = defaultdict(lambda: defaultdict(float))
        self.num_executions = defaultdict(lambda: defaultdict(int))

    def predict(self, func, endpoint, *args, **kwargs):
        return self.avg_runtime[func][endpoint]

    def update(self, task_info, new_runtime):
        func = task_info['function_id']
        end = task_info['endpoint_id']

        while len(self.runtimes[func][end].queue) > self.last_n:
            self.runtimes[func][end].get()
        self.runtimes[func][end].put(new_runtime)
        self.avg_runtime[func][end] = avg(self.runtimes[func][end])

        self.num_executions[func][end] += 1


def init_runtime_predictor(predictor, *args, **kwargs):
    predictor = predictor.strip().lower()
    if predictor == 'rolling-average':
        return RollingAverage(*args, **kwargs)
    else:
        raise NotImplementedError(f"Predictor: {predictor}")
