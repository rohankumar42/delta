import time
from collections import defaultdict
from predictors import RuntimePredictor


FUNCX_LATENCY = 0.5  # Estimated overhead of executing task


class Strategy(object):

    def __init__(self, endpoints, runtime_predictor: RuntimePredictor,
                 queue_predictor, launch_predictor):
        if len(endpoints) == 0:
            raise ValueError("List of endpoints cannot be empty")
        assert(callable(runtime_predictor))
        assert(callable(queue_predictor))
        assert(callable(launch_predictor))

        self.endpoints = endpoints
        self.runtime = runtime_predictor
        self.queue_predictor = queue_predictor
        self.launch_predictor = launch_predictor

    def choose_endpoint(self, func, payload, exclude=None):
        raise NotImplementedError

    def add_endpoint(self, endpoint, group):
        # TODO: explore new endpoints
        self.endpoints[endpoint] = group

    def remove_endpoint(self, endpoint):
        if endpoint in self.endpoints:
            del self.endpoints[endpoint]

    def predict_ETA(self, func, endpoint, payload):
        # TODO: better task ETA prediction by including data movement,
        # latency, and other costs

        t_launch = self.launch_predictor(endpoint)
        t_pending = self.queue_predictor(endpoint)
        t_run = self.runtime(func=func,
                             group=self.endpoints[endpoint]['group'],
                             payload=payload)

        return t_launch + t_pending + t_run + FUNCX_LATENCY

    def __str__(self):
        return type(self).__name__


class RoundRobin(Strategy):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.next = 0

    def choose_endpoint(self, func, payload, exclude=None):
        exclude = exclude or set()
        assert(len(exclude) < len(self.endpoints))
        endpoints = list(self.endpoints.keys())
        while True:
            endpoint = endpoints[self.next % len(endpoints)]
            self.next += 1
            if endpoint not in exclude:
                break
        return {'endpoint': endpoint}


class FastestEndpoint(Strategy):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.next_group = defaultdict(int)
        self.next_endpoint = defaultdict(lambda: defaultdict(int))
        self.groups = set(x['group'] for x in self.endpoints.values())
        self.group_to_endpoints = {
            g: [e for (e, x) in self.endpoints.items() if x['group'] == g]
            for g in self.groups
        }

    def choose_endpoint(self, func, payload, exclude=None):
        exclude = exclude or set()
        assert(len(exclude) < len(self.endpoints))
        excluded_groups = {g for (g, ends) in self.group_to_endpoints.items()
                           if all(e in exclude for e in ends)}
        groups = list(self.groups - excluded_groups)

        times = [(g, self.runtime(func=func, group=g, payload=payload))
                 for g in groups]
        # Ignore groups which don't have predictions yet
        times = [(g, t) for (g, t) in times if t > 0.0]

        # Try each group once, and then start choosing the best one
        if len(times) < len(groups):
            group = groups[self.next_group[func]]
            self.next_group[func] += 1
            self.next_group[func] %= len(groups)
        else:
            group, runtime = min(times, key=lambda x: x[1])

        # Round-robin between endpoints in the same group
        while True:
            i = self.next_endpoint[func][group]
            endpoint = self.group_to_endpoints[group][i]
            self.next_endpoint[func][group] += 1
            self.next_endpoint[func][group] %= \
                len(self.group_to_endpoints[group])
            if endpoint not in exclude:
                break

        return {'endpoint': endpoint}


class SmallestETA(Strategy):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.next_group = defaultdict(int)
        self.next_endpoint = defaultdict(lambda: defaultdict(int))
        self.groups = set(x['group'] for x in self.endpoints.values())
        self.group_to_endpoints = {
            g: [e for (e, x) in self.endpoints.items() if x['group'] == g]
            for g in self.groups
        }

    def choose_endpoint(self, func, payload, exclude=None):
        exclude = exclude or set()
        assert(len(exclude) < len(self.endpoints))
        excluded_groups = {g for (g, ends) in self.group_to_endpoints.items()
                           if all(e in exclude for e in ends)}
        groups = list(self.groups - excluded_groups)

        times = [(g, self.runtime(func=func, group=g, payload=payload))
                 for g in groups]
        # Ignore groups which don't have predictions yet
        times = [(g, t) for (g, t) in times if t > 0.0]

        # Try each group once, and then start choosing the endpoint with
        # the smallest predicted ETA
        res = {}
        if len(times) < len(groups):
            group = groups[self.next_group[func]]
            self.next_group[func] += 1
            self.next_group[func] %= len(groups)

            # Round-robin between endpoints in the same group
            while True:
                i = self.next_endpoint[func][group]
                res['endpoint'] = self.group_to_endpoints[group][i]
                self.next_endpoint[func][group] += 1
                self.next_endpoint[func][group] %= \
                    len(self.group_to_endpoints[group])
                if res['endpoint'] not in exclude:
                    break

        else:
            ETAs = [(ep, self.predict_ETA(func, ep, payload))
                    for ep in self.endpoints.keys() if ep not in exclude]
            res['endpoint'], res['ETA'] = min(ETAs, key=lambda x: x[1])

        return res


def init_strategy(strategy, *args, **kwargs):
    strategy = strategy.strip().lower()
    if strategy in ['round-robin', 'rr']:
        return RoundRobin(*args, **kwargs)
    elif strategy.startswith('fastest'):
        return FastestEndpoint(*args, **kwargs)
    elif strategy.endswith('eta'):
        return SmallestETA(*args, **kwargs)
    else:
        raise NotImplementedError(f"Strategy: {strategy}")
