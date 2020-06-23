import os
import uuid
import time
import logging
from threading import Thread

import globus_sdk
from fair_research_login import NativeClient, JSONTokenStorage

from utils import colored, endpoint_name, MAX_CONCURRENT_TRANSFERS


logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter(
    colored("[TRANSFER]  %(message)s", 'red')))
logger.addHandler(ch)


TOKEN_LOC = os.path.expanduser('~/.funcx/credentials/scheduler_tokens.json')
CLIENT_ID = 'f06739da-ad7d-40bd-887f-abb1d23bbd6f'


class TransferManager(object):

    # TODO: move TransferPredictor into this class and update prediction model
    # every time a tranfer finishes

    def __init__(self, endpoints, sync_level='exists', log_level='INFO'):

        transfer_scope = 'urn:globus:auth:scope:transfer.api.globus.org:all'
        native_client = NativeClient(client_id=CLIENT_ID,
                                     app_name="FuncX Continuum Scheduler",
                                     token_storage=JSONTokenStorage(TOKEN_LOC))
        native_client.login(requested_scopes=[transfer_scope], no_browser=True,
                            no_local_server=True, refresh_tokens=True)
        all_authorizers = native_client.get_authorizers_by_scope(
            requested_scopes=[transfer_scope])
        transfer_authorizer = all_authorizers[transfer_scope]
        self.transfer_client = globus_sdk.TransferClient(transfer_authorizer)

        self.endpoints = endpoints
        self.sync_level = sync_level
        logger.setLevel(log_level)

        # Track pending transfers
        self._next = 0
        self.active_transfers = {}
        self.completed_transfers = {}
        self.transfer_ids = {}

        # Initialize thread to wait on transfers
        self._polling_interval = 1
        self._tracker = Thread(target=self._track_transfers)
        self._tracker.daemon = True
        self._tracker.start()

    def transfer(self, files_by_src, dst, task_id='', unique_name=False):
        n = len(files_by_src)

        empty_transfer = True

        transfer_ids = []
        for i, (src, pairs) in enumerate(files_by_src.items(), 1):
            src_name = endpoint_name(src)
            dst_name = endpoint_name(dst)

            if src == dst:
                logger.debug(f'Skipped transfer from {src_name} to {dst_name}')
                continue
            else:
                empty_transfer = False

            files, _ = zip(*pairs)
            logger.info(f'Transferring {src_name} to {dst_name}: {files}')

            src_globus = self.endpoints[src]['globus']
            dst_globus = self.endpoints[dst]['globus']

            tdata = globus_sdk.TransferData(self.transfer_client,
                                            src_globus, dst_globus,
                                            label='FuncX Transfer {} - {} of {}'
                                            .format(self._next + 1, i, n),
                                            sync_level=self.sync_level)

            for f in files:
                if unique_name:
                    dst_file = '~/.globus_funcx/test_{}.txt'.format(
                        str(uuid.uuid4()))
                    logger.debug('Unique destination file name: {}'
                                 .format(dst_file))
                    tdata.add_item(f, dst_file)
                else:
                    tdata.add_item(f, f)

            res = self.transfer_client.submit_transfer(tdata)

            if res['code'] != 'Accepted':
                raise ValueError('Transfer not accepted')

            self.active_transfers[res['task_id']] = {
                'src': src_globus,
                'dst': dst_globus,
                'files': files,
                'name': f'{task_id} ({i}/{n})',
                'submission_time': time.time()
            }
            transfer_ids.append(res['task_id'])

            if len(self.active_transfers) > MAX_CONCURRENT_TRANSFERS:
                logger.warn('More than {} concurrent transfers! Expect delays.'
                            .format(MAX_CONCURRENT_TRANSFERS))

        if empty_transfer:
            return None
        else:
            self._next += 1
            self.transfer_ids[self._next] = transfer_ids
            return self._next

    def is_complete(self, num):
        assert(num <= self._next)

        return all(t in self.completed_transfers
                   for t in self.transfer_ids[num])

    def get_transfer_time(self, num):
        if not self.is_complete(num):
            raise ValueError('Cannot get transfer time of incomplete transfer')

        return max(self.completed_transfers[t]['time_taken']
                   for t in self.transfer_ids[num])

    def wait(self, num):
        while not self.is_complete(num):
            pass

    def _track_transfers(self):
        logger.info('Started transfer tracking thread')

        while True:
            time.sleep(self._polling_interval)

            for transfer_id, info in list(self.active_transfers.items()):
                name = info['name']
                status = self.transfer_client.get_task(transfer_id)

                if status['status'] == 'FAILED':
                    logger.error('Task {} failed. Canceling task!'
                                 .format(transfer_id))
                    res = self.transfer_client.cancel_task(transfer_id)
                    if res['code'] != 'Canceled':
                        logger.error('Could not cancel task {}. Reason: {}'
                                     .format(transfer_id, res['message']))
                    del self.active_transfers[transfer_id]

                elif status['status'] == 'ACTIVE':
                    continue

                elif status['status'] == 'SUCCEEDED':
                    info['time_taken'] = time.time() - info['submission_time']
                    logger.info('Globus transfer {} finished in time {}'
                                .format(name, info['time_taken']))
                    self.completed_transfers[transfer_id] = info
                    del self.active_transfers[transfer_id]
