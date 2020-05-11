from collections import defaultdict


class BaseStrategy(object):

    def __init__(self, endpoints, runtimes=None):
        if len(endpoints) == 0:
            raise ValueError("List of endpoints cannot be empty")
        self.endpoints = endpoints
        self.runtimes = runtimes if runtimes is not None else {}

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
        return endpoint


class Fastest(BaseStrategy):

    def __init__(self, endpoints, runtimes, *args, **kwargs):
        super().__init__(endpoints=endpoints, runtimes=runtimes)
        self.next_endpoint = defaultdict(int)

    def choose_endpoint(self, function_id, *args, **kwargs):
        times = list(self.runtimes[function_id].items())
        print(sorted(times, key=lambda x: x[1]))

        # Try each endpoint once, and then start choosing the best one
        if len(times) < len(self.endpoints):
            endpoint = self.endpoints[self.next_endpoint[function_id]]
            self.next_endpoint[function_id] += 1
            self.next_endpoint[function_id] %= len(self.endpoints)
        else:
            endpoint, _ = min(times, key=lambda x: x[1])

        return endpoint


def init_strategy(strategy, *args, **kwargs):
    if strategy == 'round-robin':
        return RoundRobin(*args, **kwargs)
    elif strategy == 'fastest':
        return Fastest(*args, **kwargs)
    else:
        raise NotImplementedError(f"Strategy: {strategy}")
