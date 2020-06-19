import json
import numpy as np
from queue import Queue
from collections import defaultdict

from utils import avg, ENDPOINTS


class RuntimePredictor(object):

    def __init__(self, endpoints):
        self.endpoints = endpoints

    def predict(self, func, group, payload):
        raise NotImplementedError

    def update(self, task_info, new_runtime):
        raise NotImplementedError

    def __call__(self, *args, **kwargs):
        return self.predict(*args, **kwargs)

    def __str__(self):
        return type(self).__name__


class RollingAverage(RuntimePredictor):

    def __init__(self, endpoints, last_n=3, *args, **kwargs):
        super().__init__(endpoints)
        self.last_n = last_n
        self.runtimes = defaultdict(lambda: defaultdict(Queue))
        self.avg_runtime = defaultdict(lambda: defaultdict(float))
        self.num_executions = defaultdict(lambda: defaultdict(int))

    def predict(self, func, group, *args, **kwargs):
        return self.avg_runtime[func][group]

    def update(self, task_info, new_runtime):
        func = task_info['function_id']
        end = task_info['endpoint_id']
        group = self.endpoints[end]['group']

        while len(self.runtimes[func][group].queue) > self.last_n:
            self.runtimes[func][group].get()
        self.runtimes[func][group].put(new_runtime)
        self.avg_runtime[func][group] = avg(self.runtimes[func][group])

        self.num_executions[func][group] += 1


class InputLength(RuntimePredictor):

    def __init__(self, endpoints, train_every=1, *args, **kwargs):
        # TODO: ensure that the number of data points stored stays under some
        # threshold, to guarantee low memory usage and fast training
        super().__init__(endpoints)
        self.lengths = defaultdict(lambda: defaultdict(list))
        self.runtimes = defaultdict(lambda: defaultdict(list))
        self.weights = defaultdict(lambda: defaultdict(lambda: np.zeros(4)))

        self.train_every = train_every
        self.updates_since_train = defaultdict(lambda: defaultdict(int))

    def predict(self, func, group, payload, *args, **kwargs):
        pred = self.weights[func][group].T.dot(self._preprocess(len(payload)))
        return pred.item()

    def update(self, task_info, new_runtime):
        func = task_info['function_id']
        end = task_info['endpoint_id']
        group = self.endpoints[end]['group']

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
        self.weights[func][group] = np.linalg.pinv(lengths).dot(runtimes)

    def _preprocess(self, x):
        '''Create features that are easy to learn from.'''
        return np.array([1, x, x ** 2, 2.0 * x])


def init_runtime_predictor(predictor, *args, **kwargs):
    predictor = predictor.strip().lower()
    if predictor.endswith('average') or predictor.endswith('avg'):
        return RollingAverage(*args, **kwargs)
    elif predictor.endswith('length') or predictor.endswith('size'):
        return InputLength(*args, **kwargs)
    else:
        raise NotImplementedError("Predictor: {}".format(predictor))


class TransferPredictor(object):

    MAX_CONCURRENT_TRANSFERS = 3

    def __init__(self, endpoints=None, train_every=1, state_file=None):
        self.endpoints = endpoints or ENDPOINTS
        self.sizes = defaultdict(lambda: defaultdict(list))
        self.times = defaultdict(lambda: defaultdict(list))
        self.weights = defaultdict(lambda: defaultdict(lambda: np.zeros(3)))

        self.train_every = train_every
        self.updates_since_train = defaultdict(lambda: defaultdict(int))

        if state_file is not None:
            self._load_state_from_file(state_file)

    def predict_one(self, src, dst, size):
        if src == dst:
            return 0.0

        src_grp = self.endpoints[src]['transfer_group']
        dst_grp = self.endpoints[dst]['transfer_group']

        pred = self.weights[src_grp][dst_grp].T.dot(self._preprocess(size))
        return pred.item()

    def predict(self, files_by_src, dst):
        '''Predict the time for transfers from each source, and return
        the maximum. Assumption: all transfers will happen concurrently.'''

        assert(len(files_by_src) <= self.MAX_CONCURRENT_TRANSFERS)

        if len(files_by_src) == 0:
            return 0.0

        times = []
        for src, pairs in files_by_src.items():
            _, sizes = zip(*pairs)
            times.append(self.predict_one(src, dst, sum(sizes)))

        return max(times)

    def update(self, src, dst, size, transfer_time):
        src_grp = self.endpoints[src]['transfer_group']
        dst_grp = self.endpoints[dst]['transfer_group']

        self.sizes[src_grp][dst_grp].append(size)
        self.times[src_grp][dst_grp].append(transfer_time)

        self.updates_since_train[src_grp][dst_grp] += 1
        if self.updates_since_train[src_grp][dst_grp] >= self.train_every:
            self._train(src_grp, dst_grp)
            self.updates_since_train[src_grp][dst_grp] = 0

    def _train(self, src_grp, dst_grp):
        sizes = np.array([self._preprocess(x)
                          for x in self.sizes[src_grp][dst_grp]])
        sizes = sizes.reshape((-1, 3))
        times = np.array([self.times[src_grp][dst_grp]]).reshape((-1, 1))
        self.weights[src_grp][dst_grp] = np.linalg.pinv(sizes).dot(times)

    def _preprocess(self, x):
        '''Create features that are easy to learn from.'''
        return np.array([1, x, np.log(x)])

    def to_file(self, file_name):
        sizes = {k: dict(vs) for (k, vs) in self.sizes.items()}
        times = {k: dict(vs) for (k, vs) in self.times.items()}
        weights = {s: {d: w.tolist() for (d, w) in vs.items()}
                   for (s, vs) in self.weights.items()}

        state = {
            'sizes': sizes,
            'times': times,
            'weights': weights,
        }

        with open(file_name, 'w') as fh:
            json.dump(state, fh)

    def _load_state_from_file(self, file_name):
        with open(file_name) as fh:
            state = json.load(fh)

        for s, vs in state['sizes'].items():
            for d, xs in vs.items():
                self.sizes[s][d] = xs

        for s, vs in state['times'].items():
            for d, xs in vs.items():
                self.times[s][d] = xs

        for s, vs in state['weights'].items():
            for d, xs in vs.items():
                self.weights[s][d] = np.array(xs)

        return self

    def __call__(self, *args, **kwargs):
        return self.predict(*args, **kwargs)

    def __str__(self):
        return type(self).__name__
