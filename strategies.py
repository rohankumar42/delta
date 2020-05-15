from collections import defaultdict


class BaseStrategy(object):

    def __init__(self, endpoints):
        if len(endpoints) == 0:
            raise ValueError("List of endpoints cannot be empty")
        self.endpoints = endpoints

    def choose_endpoint(self, function_id, *args, **kwargs):
        raise NotImplementedError

    def add_endpoint(self, endpoint):
        self.endpoints.append(endpoint)

    def remove_endpoint(self, endpoint):
        if endpoint in self.endpoints:
            self.endpoints.remove(endpoint)


class RoundRobin(BaseStrategy):

    def __init__(self, endpoints, *args, **kwargs):
        super().__init__(endpoints=endpoints)
        self.next = 0

    def choose_endpoint(self, function_id):
        endpoint = self.endpoints[self.next % len(self.endpoints)]
        self.next += 1
        return {'endpoint': endpoint}


class FastestEndpoint(BaseStrategy):

    def __init__(self, endpoints, runtimes, *args, **kwargs):
        super().__init__(endpoints=endpoints)
        self.runtimes = runtimes
        self.next_endpoint = defaultdict(int)

    def choose_endpoint(self, function_id, *args, **kwargs):
        times = list(self.runtimes[function_id].items())

        # Try each endpoint once, and then start choosing the best one
        if len(times) < len(self.endpoints):
            endpoint = self.endpoints[self.next_endpoint[function_id]]
            self.next_endpoint[function_id] += 1
            self.next_endpoint[function_id] %= len(self.endpoints)
        else:
            endpoint, _ = min(times, key=lambda x: x[1])

        return {'endpoint': endpoint}


class SmallestETA(BaseStrategy):

    def __init__(self, endpoints, runtimes, ETA_predictor, *args, **kwargs):
        super().__init__(endpoints)
        assert(callable(ETA_predictor))
        self.predict_ETA = ETA_predictor
        self.runtimes = runtimes
        self.next_endpoint = defaultdict(int)

    def choose_endpoint(self, function_id, *args, **kwargs):
        res = {}
        times = list(self.runtimes[function_id].items())

        # Try each endpoint once, and then start choosing the one with
        # the smallest predicted ETA
        if len(times) < len(self.endpoints):
            res['endpoint'] = self.endpoints[self.next_endpoint[function_id]]
            self.next_endpoint[function_id] += 1
            self.next_endpoint[function_id] %= len(self.endpoints)
        else:
            ETAs = [(ep, self.predict_ETA(function_id, ep))
                    for ep in self.endpoints]
            res['endpoint'], res['ETA'] = min(ETAs, key=lambda x: x[1])

        return res


def init_strategy(strategy, *args, **kwargs):
    strategy = strategy.strip().lower()
    if strategy == 'round-robin':
        return RoundRobin(*args, **kwargs)
    elif strategy == 'fastest-endpoint':
        return FastestEndpoint(*args, **kwargs)
    elif strategy == 'smallest-eta':
        return SmallestETA(*args, **kwargs)
    else:
        raise NotImplementedError(f"Strategy: {strategy}")
