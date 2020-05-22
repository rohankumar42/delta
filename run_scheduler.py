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

from central_scheduler import CentralScheduler

funcx_app = Flask(__name__)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter(colored("%(message)s", 'yellow')))
funcx_app.logger.addHandler(ch)
funcx_app.logger.setLevel('DEBUG')
logging.getLogger('werkzeug').setLevel('ERROR')


FUNCX_API = 'https://dev.funcx.org/api/v1'


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
    res = forward_request(request)
    SCHEDULER.log_status(task_id, json.loads(res.text))
    return res.text


@funcx_app.route('/batch_status', methods=['POST'])
def batch_status():
    res = forward_request(request)
    for task_id, status in json.loads(res.text)['results'].items():
        SCHEDULER.log_status(task_id, status)
    return res.text


@funcx_app.route('/register_function', methods=['POST'])
def reg_function():
    res = forward_request(request)
    return res.text


@funcx_app.route('/submit', methods=['POST'])
def submit():
    data = json.loads(request.data)
    assert (data['endpoint'] == 'UNDECIDED')
    choice = SCHEDULER.choose_endpoint(data['func'], data['payload'])
    data['endpoint'] = choice['endpoint']

    res_str = forward_request(request, data=json.dumps(data))
    res = json.loads(res_str.text)
    SCHEDULER.log_submission(data['func'], data['payload'],
                             choice, res['task_uuid'])
    res['endpoint'] = data['endpoint']
    return json.dumps(res)


@funcx_app.route('/batch_run', methods=['POST'])
def batch_submit():
    data = json.loads(request.data)
    assert(all(e == 'UNDECIDED' for e in data['endpoints']))
    n = len(data['functions'])
    data['endpoints'] = []
    choices = []

    # TODO: smarter scheduling for batch submissions
    for i in range(n):
        choice = SCHEDULER.choose_endpoint(data['functions'][i],
                                           data['payloads'][i])
        choices.append(choice)
        data['endpoints'].append(choice['endpoint'])

    res_str = forward_request(request, data=json.dumps(data))
    res = json.loads(res_str.text)

    for i in range(n):
        SCHEDULER.log_submission(data['functions'][i], data['payloads'][i],
                                 choices[i], res['task_uuids'][i])

    res['endpoints'] = data['endpoints']
    return json.dumps(res)


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
                                 log_level=args.log_level)

    funcx_app.run(host='0.0.0.0', port=args.port, debug=args.debug,
                  threaded=True,
                  extra_files=['central_scheduler.py', 'strategies.py',
                               'endpoints.yaml'])
