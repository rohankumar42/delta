import time
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


if __name__ == "__main__":
    client = FuncXSmartClient(funcx_service_address='http://localhost:5000',
                              force_login=False, log_level='WARN')

    func = client.register_function(loop, function_name='loop')

    SIZE = 2 * 10 ** 1
    NUM_TASKS = 100
    NUM_ENDPOINTS = 4

    start = time.time()
    for _ in range(NUM_ENDPOINTS):
        task = client.run(SIZE, function_id=func)
        client.get_result(task, block=True)
    print('Warmed up in {:.3f} s'.format(time.time() - start))

    tasks = []
    overall_start = time.time()
    for i in range(NUM_TASKS):
        start = time.time()
        task = client.run(SIZE, function_id=func)
        print('Time to schedule: {:.3f} s'.format(time.time() - start))
        tasks.append(task)

        # time.sleep(0.3)

    for task in tasks:
        print('Waiting on task:', task)
        res = client.get_result(task, block=True)
        print('Got res:', res)

    print('Total time: {:.3f}'.format(time.time() - overall_start))

    client.stop()
