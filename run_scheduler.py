import yaml
import json
import logging
import argparse
import requests
from flask import Flask, request

try:
    from termcolor import colored
except ImportError:
    def colored(x, *args, **kwargs):
        return x

from central_scheduler import CentralScheduler, FUNCX_API

funcx_app = Flask(__name__)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter(colored("%(message)s", 'yellow')))
funcx_app.logger.addHandler(ch)
funcx_app.logger.setLevel('DEBUG')
logging.getLogger('werkzeug').setLevel('ERROR')


def forward_request(request, route=None, headers=None, data=None):
    url = f'{FUNCX_API}{route or request.path}'
    headers = headers or request.headers
    data = data or request.data

    return requests.request(request.method, url=url, headers=headers,
                            data=data)


@funcx_app.route('/', methods=['GET'])
def base():
    return 'OK'


@funcx_app.route('/<task_id>/status', methods=['GET'])
def status(task_id):
    real_task_ids = SCHEDULER.translate_task_id(task_id)
    for real_task_id in real_task_ids:
        res = forward_request(request, route=f'/{real_task_id}/status')
        SCHEDULER.log_status(real_task_id, json.loads(res.text))

    return SCHEDULER.get_status(task_id)


@funcx_app.route('/batch_status', methods=['POST'])
def batch_status():
    task_ids = json.loads(request.data)['task_ids']
    real_task_ids = set()
    for task_id in task_ids:
        real_task_ids |= SCHEDULER.translate_task_id(task_id)

    if len(real_task_ids) > 0:
        real_data = json.dumps({'task_ids': list(real_task_ids)})
        res = forward_request(request, data=real_data)
        try:
            for real_task_id, status in json.loads(res.text)['results'].items():
                SCHEDULER.log_status(real_task_id, status)
        except json.decoder.JSONDecodeError as e:
            funcx_app.logger.warn(
                f'Could not get batch result from {res.text}', e)

    res_data = {'response': 'batch', 'results': {}}
    for task_id in task_ids:
        status = SCHEDULER.get_status(task_id)
        if status.get('status') == 'PENDING':
            continue
        res_data['results'][task_id] = status

    return json.dumps(res_data)


@funcx_app.route('/register_function', methods=['POST'])
def reg_function():
    res = forward_request(request)
    return res.text


@funcx_app.route('/submit', methods=['POST'])
def batch_submit():
    # Note: This route is not forwarded to the main FuncX service here.
    # The tasks will be sent to the FuncX service by the SCHEDULER object.
    # This is to allow for delayed-task and backup-task submission.
    headers = request.headers
    data = json.loads(request.data)

    if not all(t[1] == 'UNDECIDED' for t in data['tasks']):
        return json.dumps({
            'status': 'Failed',
            'reason': 'Endpoints should be \'UNDECIDED\''
        })

    tasks = [(func, payload) for (func, _, payload) in data['tasks']]
    task_uuids, endpoints = SCHEDULER.batch_submit(tasks, headers)
    return json.dumps({
        'status': 'Success',
        'task_uuids': task_uuids,
        'endpoints': endpoints
    })


@funcx_app.route('/block/<func>/<endpoint>', methods=['GET'])
def block(func, endpoint):
    return SCHEDULER.block(func, endpoint)


@funcx_app.route('/execution_log', methods=['GET'])
def execution_log():
    log = SCHEDULER.execution_log
    SCHEDULER.execution_log = []
    return {'log': log}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, default=5000)
    parser.add_argument('-d', '--debug', action='store_true', default=False)
    parser.add_argument('--endpoints', type=str, default='endpoints.yaml')
    parser.add_argument('-s', '--strategy', type=str, default='round-robin')
    parser.add_argument('-rp', '--predictor', type=str,
                        default='rolling-average')
    parser.add_argument('--last-n', type=int, default=3)
    parser.add_argument('--train-every', type=int, default=1)
    parser.add_argument('-b', '--max-backups', type=int, default=0)
    parser.add_argument('--log-level', type=str, default='INFO')
    args = parser.parse_args()

    with open(args.endpoints) as fh:
        endpoints = yaml.safe_load(fh)

    global SCHEDULER
    SCHEDULER = CentralScheduler(endpoints=endpoints,
                                 strategy=args.strategy,
                                 runtime_predictor=args.predictor,
                                 last_n=args.last_n,
                                 train_every=args.train_every,
                                 max_backups=args.max_backups,
                                 log_level=args.log_level)

    funcx_app.run(host='0.0.0.0', port=args.port, debug=args.debug,
                  threaded=True,
                  extra_files=['central_scheduler.py', 'strategies.py',
                               'endpoints.yaml'])
