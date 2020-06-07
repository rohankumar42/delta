import os
import logging
from threading import Thread

import globus_sdk
from fair_research_login import NativeClient, JSONTokenStorage

from utils import colored, endpoint_name


logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter(
    colored("[TRANSFER]  %(message)s", 'red')))
logger.addHandler(ch)


TOKEN_LOC = os.path.expanduser('~/.funcx/credentials/scheduler_tokens.json')
CLIENT_ID = 'f06739da-ad7d-40bd-887f-abb1d23bbd6f'


class TransferManager(object):

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
        self._active_transfers = {}
        self._completed_transfers = set()
        self._transfer_ids = {}

        # Initialize thread to wait on transfers
        self._timeout = 1
        self._polling_interval = 1
        self._tracker = Thread(target=self._track_transfers)
        self._tracker.daemon = True
        self._tracker.start()

    def transfer(self, files_by_src, dst, task_id=''):
        self._next += 1

        n = len(files_by_src)

        transfer_ids = []
        for i, (src, files) in enumerate(files_by_src.items(), 1):
            src_name = endpoint_name(src)
            dst_name = endpoint_name(dst)
            logger.debug(f'Transferring {src_name} to {dst_name}: {files}')

            src_globus = self.endpoints[src]['globus']
            dst_globus = self.endpoints[dst]['globus']

            tdata = globus_sdk.TransferData(self.transfer_client,
                                            src_globus, dst_globus,
                                            label='Transfer {} - {} of {}'
                                            .format(self._next, i, n),
                                            sync_level=self.sync_level)

            for f in files:
                tdata.add_item(f, f)

            res = self.transfer_client.submit_transfer(tdata)

            if res['code'] != 'Accepted':
                raise ValueError('Transfer not accepted')

            self._active_transfers[res['task_id']] = f'{task_id} ({i}/{n})'
            transfer_ids.append(res['task_id'])

        self._transfer_ids[self._next] = transfer_ids

        return self._next

    def is_complete(self, num):
        assert(num <= self._next)

        return all(t in self._completed_transfers
                   for t in self._transfer_ids[num])

    def _track_transfers(self):
        logger.info('Started transfer tracking thread')

        while True:

            for transfer_id, label in list(self._active_transfers.items()):
                completed = self.transfer_client.task_wait(
                    transfer_id, timeout=self._timeout,
                    polling_interval=self._polling_interval)

                if completed:
                    logger.info(f'Globus transfer finished: {label}')
                    self._completed_transfers.add(transfer_id)
                    del self._active_transfers[transfer_id]
