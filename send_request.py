from funcx.sdk.smart_client import FuncXSmartClient


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
                              force_login=False)
    func = client.register_function(loop, function_name='loop')

    tasks = []
    for i in range(10):
        task = client.run(10 ** 5, function_id=func)
        # i, function_id='6669e9f2-cc6b-4e7d-b117-bd0aaf0dbc78')
        tasks.append(task)

    for task in tasks:
        res = client.get_result(task, block=True)
        print('Got res:', res)

    client.stop()
