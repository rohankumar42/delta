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

from strategies import init_strategy

funcx_app = Flask(__name__)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter(colored("%(message)s", 'yellow')))
funcx_app.logger.addHandler(ch)
funcx_app.logger.setLevel('DEBUG')


FUNCX_API = 'https://funcx.org/api/v1'


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


@funcx_app.route('/register_function', methods=['POST'])
def reg_function():
    res = forward_request(request)
    return res.text


@funcx_app.route('/submit', methods=['POST'])
def submit():
    data = json.loads(request.data)
    if 'endpoint' not in data or data['endpoint'] == 'UNDECIDED':
        data['endpoint'] = SCHEDULER.choose_endpoint(data['func'])

    res = forward_request(request, data=json.dumps(data))
    funcx_app.logger.debug('Sent function {} to endpoint {} with task_id {}'
                           .format(data['func'], data['endpoint'],
                                   json.loads(res.text)['task_uuid']))
    return res.text


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, default=5000)
    parser.add_argument('-d', '--debug', action='store_true', default=True)
    parser.add_argument('--endpoints', type=str, default='endpoints.yaml')
    parser.add_argument('-s', '--strategy', type=str, default='round-robin')
    args = parser.parse_args()

    with open(args.endpoints) as fh:
        endpoints = yaml.safe_load(fh)

    global SCHEDULER
    SCHEDULER = init_strategy(args.strategy, endpoints=endpoints)

    funcx_app.run(host='0.0.0.0', port=args.port, debug=args.debug,
                  threaded=False)
