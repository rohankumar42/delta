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


def parallel_count(n):
    import multiprocessing as mp
    num_cpus = mp.cpu_count()

    def loop_worker(k):
        for i in range(k):
            pass
        return k

    processes = [
        mp.Process(target=loop_worker, args=(n // num_cpus,))
        for _ in range(num_cpus)
    ]
    for p in processes:
        p.start()
    for p in processes:
        p.join()

    return n


def get_env(*args):
    return {'globals': str(globals()), 'locals': str(locals())}


def read_file(name, sleep=0.2):
    import os
    import time
    time.sleep(sleep)
    path = os.path.join(os.path.expanduser('~/.globus_funcx/'), name)
    with open(path) as fh:
        return fh.read()


def import_tensorflow():
    import tensorflow as tf
    return tf.config.list_physical_devices('GPU')


def import_module(module):
    exec('import ' + module)


if __name__ == "__main__":
    client = FuncXSmartClient(funcx_service_address='http://localhost:5000',
                              force_login=False, log_level='INFO',
                              batch_status=True, local=False)
    random.seed(100)

    func = client.register_function(import_module)

    # INPUTS = ['1' * 1, '1' * 4, '1' * 7]
    # INPUTS = ['test.txt', 'does_not_exist.jpg']
    # INPUTS = [10 ** 4]
    INPUTS = ['funcx_aws_1_size_1.txt'] * 2
    random.shuffle(INPUTS)
    NUM_TASKS = 0
    NUM_GROUPS = 1

    start = time.time()
    task_ids = []
    csil4 = '576ab4d8-64b9-44cd-9c77-f2e8ea7877ae'
    aws1 = '7ff9c62e-9cc7-4b30-ba63-56229e490f48'
    # client.block(func, aws1)
    warmup_batch = client.create_batch()
    for _ in range(NUM_GROUPS):
        for x in INPUTS:
            task = warmup_batch.add('tensorflow', function_id=func)
            # task = warmup_batch.add(x, function_id=func,
            # files=[(aws1, x, 10)])
    task_ids = client.batch_run(warmup_batch)
    for task_id in task_ids:
        try:
            res = client.get_result(task_id, block=True)
            print(res)
        except Exception as e:
            print('Script got exception:', e)
    print('Warmed up in {:.3f} s'.format(time.time() - start))

    tasks = []
    overall_start = time.time()
    for i in range(NUM_TASKS):
        start = time.time()
        task = client.run(INPUTS[i % len(INPUTS)], function_id=func)
        print('Time to schedule: {:.3f} s'.format(time.time() - start))
        tasks.append(task)

        # time.sleep(0.3)

    for i, task in enumerate(tasks, 1):
        print(f'Waiting on task {i}/{NUM_TASKS}:', task)
        res = client.get_result(task, block=True)
        print('Got res:', res)

    print('Total time: {:.3f}'.format(time.time() - overall_start))

    client.stop()
