
class BaseStrategy(object):

    def __init__(self, endpoints):
        if len(endpoints) == 0:
            raise ValueError("List of endpoints cannot be empty")
        self.endpoints = endpoints

    def choose_endpoint(self, function_id, *args, **kwargs):
        raise NotImplementedError

    def log_status(self, task_id, status):
        pass

    def add_endpoint(self, endpoint):
        self.endpoints.append(endpoint)

    def remove_endpoint(self, endpoint):
        if endpoint in self.endpoints:
            self.endpoints.remove(endpoint)


class RoundRobin(BaseStrategy):

    def __init__(self, endpoints):
        super().__init__(endpoints=endpoints)
        self.next = 0

    def choose_endpoint(self, function_id):
        endpoint = self.endpoints[self.next % len(self.endpoints)]
        self.next += 1
        return endpoint


def init_strategy(strategy, *args, **kwargs):
    if strategy == 'round-robin':
        return RoundRobin(*args, **kwargs)
    else:
        raise NotImplementedError(f"Strategy: {strategy}")
