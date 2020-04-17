from funcx.sdk.smart_client import FuncXSmartClient


def f(x):
    return 2 * x


if __name__ == "__main__":
    client = FuncXSmartClient(funcx_service_address='http://localhost:5000',
                              force_login=False)
    func = client.register_function(f, function_name='double')

    tasks = []
    for i in range(4):
        task = client.run(100, function_id=func)
        tasks.append(task)

    for task in tasks:
        res = client.get_result(task, block=True)
        print('Got res:', res)

    client.stop()
