from collections import defaultdict
from queue import Queue
import numpy as np

from utils import avg


class RuntimePredictor(object):

    def __init__(self, endpoints):
        self.endpoints = endpoints

    def predict(self, func, endpoint, payload):
        raise NotImplementedError

    def update(self, task_info, new_runtime):
        raise NotImplementedError

    def __call__(self, *args, **kwargs):
        return self.predict(*args, **kwargs)


class RollingAverage(RuntimePredictor):

    def __init__(self, endpoints, last_n=3, *args, **kwargs):
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


class InputLength(RuntimePredictor):

    def __init__(self, endpoints, train_every=1, *args, **kwargs):
        super().__init__(endpoints)
        self.lengths = defaultdict(lambda: defaultdict(list))
        self.runtimes = defaultdict(lambda: defaultdict(list))
        self.weights = defaultdict(lambda: defaultdict(lambda: np.zeros(4)))

        self.train_every = train_every
        self.updates_since_train = defaultdict(lambda: defaultdict(int))

    def predict(self, func, endpoint, payload, *args, **kwargs):
        pred = self.weights[func][endpoint].T @ self._preprocess(len(payload))
        return pred.item()

    def update(self, task_info, new_runtime):
        func = task_info['function_id']
        end = task_info['endpoint_id']

        self.lengths[func][end].append(len(task_info['payload']))
        self.runtimes[func][end].append(new_runtime)

        self.updates_since_train[func][end] += 1
        if self.updates_since_train[func][end] >= self.train_every:
            self._train(func, end)
            self.updates_since_train[func][end] = 0

    def _train(self, func, end):
        lengths = np.array([self._preprocess(x)
                            for x in self.lengths[func][end]]).reshape((-1, 4))
        runtimes = np.array([self.runtimes[func][end]]).reshape((-1, 1))
        self.weights[func][end] = np.linalg.pinv(lengths) @ runtimes

    def _preprocess(self, x):
        '''Create features that are easy to learn from.'''
        return np.array([1, x, x ^ 2, 2 ** x])


def init_runtime_predictor(predictor, *args, **kwargs):
    predictor = predictor.strip().lower()
    if predictor == 'rolling-average':
        return RollingAverage(*args, **kwargs)
    elif predictor == 'input-length':
        return InputLength(*args, **kwargs)
    else:
        raise NotImplementedError(f"Predictor: {predictor}")
