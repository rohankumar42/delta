import time
import random
from funcx.sdk.smart_client import FuncXSmartClient
from funcx.sdk.client import FuncXClient


def f(x):
    import time
    time.sleep(1)
    return x / 0 if x == 0 else 2 * x


def loop(n):
    for i in range(n):
        pass
    return i


def loop_length(s):
    for i in range(10 ** len(s)):
        pass
    return i+1


if __name__ == "__main__":
    client = FuncXSmartClient(funcx_service_address='http://localhost:5000',
                              force_login=False, log_level='WARN')
    random.seed(100)

    func = client.register_function(loop, function_name='loop')

    # INPUTS = ['1' * 1, '1' * 4, '1' * 7]
    INPUTS = [1, 10 ** 4, 10 ** 8]
    random.shuffle(INPUTS)
    NUM_TASKS = 100
    NUM_ENDPOINTS = 4

    start = time.time()
    for _ in range(NUM_ENDPOINTS):
        for x in INPUTS:
            task = client.run(x, function_id=func)
            client.get_result(task, block=True)
    print('Warmed up in {:.3f} s'.format(time.time() - start))

    tasks = []
    overall_start = time.time()
    for i in range(NUM_TASKS):
        start = time.time()
        task = client.run(INPUTS[i % len(INPUTS)], function_id=func)
        print('Time to schedule: {:.3f} s'.format(time.time() - start))
        tasks.append(task)

        # time.sleep(0.3)

    for task in tasks:
        print('Waiting on task:', task)
        res = client.get_result(task, block=True)
        print('Got res:', res)

    print('Total time: {:.3f}'.format(time.time() - overall_start))

    client.stop()
