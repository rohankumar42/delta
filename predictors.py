from collections import defaultdict
from queue import Queue
import numpy as np

from utils import avg


class RuntimePredictor(object):

    def __init__(self, endpoint_group):
        self.endpoint_group = endpoint_group

    def predict(self, func, group, payload):
        raise NotImplementedError

    def update(self, task_info, new_runtime):
        raise NotImplementedError

    def __call__(self, *args, **kwargs):
        return self.predict(*args, **kwargs)


class RollingAverage(RuntimePredictor):

    def __init__(self, endpoint_group, last_n=3, *args, **kwargs):
        super().__init__(endpoint_group)
        self.last_n = last_n
        self.runtimes = defaultdict(lambda: defaultdict(Queue))
        self.avg_runtime = defaultdict(lambda: defaultdict(float))
        self.num_executions = defaultdict(lambda: defaultdict(int))

    def predict(self, func, group, *args, **kwargs):
        return self.avg_runtime[func][group]

    def update(self, task_info, new_runtime):
        func = task_info['function_id']
        end = task_info['endpoint_id']
        group = self.endpoint_group[end]

        while len(self.runtimes[func][group].queue) > self.last_n:
            self.runtimes[func][group].get()
        self.runtimes[func][group].put(new_runtime)
        self.avg_runtime[func][group] = avg(self.runtimes[func][group])

        self.num_executions[func][group] += 1


class InputLength(RuntimePredictor):

    def __init__(self, endpoint_group, train_every=1, *args, **kwargs):
        # TODO: ensure that the number of data points stored stays under some
        # threshold, to guarantee low memory usage and fast training
        super().__init__(endpoint_group)
        self.lengths = defaultdict(lambda: defaultdict(list))
        self.runtimes = defaultdict(lambda: defaultdict(list))
        self.weights = defaultdict(lambda: defaultdict(lambda: np.zeros(4)))

        self.train_every = train_every
        self.updates_since_train = defaultdict(lambda: defaultdict(int))

    def predict(self, func, group, payload, *args, **kwargs):
        pred = self.weights[func][group].T @ self._preprocess(len(payload))
        return pred.item()

    def update(self, task_info, new_runtime):
        func = task_info['function_id']
        end = task_info['endpoint_id']
        group = self.endpoint_group[end]

        self.lengths[func][group].append(len(task_info['payload']))
        self.runtimes[func][group].append(new_runtime)

        self.updates_since_train[func][group] += 1
        if self.updates_since_train[func][group] >= self.train_every:
            self._train(func, group)
            self.updates_since_train[func][group] = 0

    def _train(self, func, group):
        lengths = np.array([self._preprocess(x)
                            for x in self.lengths[func][group]])
        lengths = lengths.reshape((-1, 4))
        runtimes = np.array([self.runtimes[func][group]]).reshape((-1, 1))
        self.weights[func][group] = np.linalg.pinv(lengths) @ runtimes

    def _preprocess(self, x):
        '''Create features that are easy to learn from.'''
        return np.array([1, x, x ** 2, 2 ** x])


def init_runtime_predictor(predictor, *args, **kwargs):
    predictor = predictor.strip().lower()
    if predictor == 'rolling-average':
        return RollingAverage(*args, **kwargs)
    elif predictor == 'input-length':
        return InputLength(*args, **kwargs)
    else:
        raise NotImplementedError(f"Predictor: {predictor}")
