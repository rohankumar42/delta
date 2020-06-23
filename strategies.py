import time
from collections import defaultdict
from predictors import RuntimePredictor, TransferPredictor


FUNCX_LATENCY = 0.3  # Estimated overhead of executing task


class Strategy(object):

    def __init__(self, endpoints,
                 runtime_predictor: RuntimePredictor,
                 queue_predictor, cold_start_predictor,
                 transfer_predictor: TransferPredictor):
        if len(endpoints) == 0:
            raise ValueError("List of endpoints cannot be empty")
        assert(callable(runtime_predictor))
        assert(callable(queue_predictor))
        assert(callable(cold_start_predictor))
        assert(callable(transfer_predictor))

        self.endpoints = endpoints
        self.runtime = runtime_predictor
        self.queue_predictor = queue_predictor
        self.cold_start_predictor = cold_start_predictor
        self.transfer_predictor = transfer_predictor

    def choose_endpoint(self, func, payload, files=None, exclude=None,
                        *args, **kwargs):
        raise NotImplementedError

    def add_endpoint(self, endpoint, group):
        # TODO: explore new endpoints
        self.endpoints[endpoint] = group

    def remove_endpoint(self, endpoint):
        if endpoint in self.endpoints:
            del self.endpoints[endpoint]

    def predict_ETA(self, func, endpoint, payload, files=None):
        # TODO: a latency predictor

        t_cold = self.cold_start_predictor(endpoint, func)
        t_pending = self.queue_predictor(endpoint)
        t_transfer = time.time()
        if files is not None:
            t_transfer += self.transfer_predictor(files, endpoint)
        t_run = self.runtime(func=func,
                             group=self.endpoints[endpoint]['group'],
                             payload=payload)

        # Transfer and pending tasks happen concurrently, so we only take into
        # account the slower of the two
        return t_cold + max(t_pending, t_transfer) + t_run + FUNCX_LATENCY

    def __str__(self):
        return type(self).__name__


class RoundRobin(Strategy):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.next = 0

    def choose_endpoint(self, func, exclude=None, *args, **kwargs):
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

    def choose_endpoint(self, func, payload, exclude=None, *args, **kwargs):
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
        if self.next_group[func] < len(groups) or len(times) == 0:
            group = groups[self.next_group[func] % len(groups)]
            self.next_group[func] += 1
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

    def choose_endpoint(self, func, payload, files=None, exclude=None,
                        transfer_ETAs=None):
        exclude = exclude or set()
        assert(len(exclude) < len(self.endpoints))
        excluded_groups = {g for (g, ends) in self.group_to_endpoints.items()
                           if all(e in exclude for e in ends)}
        groups = list(self.groups - excluded_groups)

        times = [(g, self.runtime(func=func, group=g, payload=payload))
                 for g in groups]
        # Ignore groups which don't have predictions yet
        times = dict((g, t) for (g, t) in times if t > 0.0)

        # Try each group once, and then start choosing the endpoint with
        # the smallest predicted ETA
        res = {}
        if self.next_group[func] < len(groups) or len(times) == 0:
            group = groups[self.next_group[func] % len(groups)]
            self.next_group[func] += 1

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
            # Choose the smallest ETA from groups we have predictions for
            ETAs = [(ep, self.predict_ETA(func, ep, payload, files=files))
                    for ep in self.endpoints.keys() if ep not in exclude
                    and self.endpoints[ep]['group'] in times]

            # TODO: do backfilling properly, if at all
            # # Filter out endpoints which have a max-ETA allowed for scheduling
            # if transfer_ETAs is not None:
            #     new_ETAs = [(ep, eta) for (ep, eta) in ETAs
            #                 if len(transfer_ETAs[ep]) == 0
            #                 or eta <= max(transfer_ETAs[ep])]
            #     if len(new_ETAs) == 0:
            #         print('No endpoints left to choose from! '
            #               'Ignoring transfer ETAs.')
            #     else:
            #         ETAs = new_ETAs

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
