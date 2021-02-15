import json
import gzip
import base64
import logging
import threading
from typing import List, Dict, Tuple, Union
from xialib import backlog, Depositor, Publisher, Storer
from pyinsight.insight import Insight

__all__ = ['Receiver']


class Receiver(Insight):
    """Receive pushed data, save to depositor and publish to different destinations

    Attributes:
        storers (:obj:`list` of :obj:`Storer`): Read the data which is not in a message body
        storer_dict (:obj:`list`): data_store Type and its related Storer
        depoistor (:obj:`Depositor`): Depositor attach to this receiver
        publishers (:obj:`dict` of :obj:`Publisher`): publisher id, publisher object
        subscription_list (:obj:`dict`): Subscription Lists (
            key: source topic id, source table id
            value: list of dictionary (
            key: publisher id
            value: (target destination, target topic id, target table id, field list, filters list)))

    Notes:
        filter list must in the NDF form of list(list(list)))
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger("Insight.Receiver")
        self.logger.level = self.log_level
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)


    @backlog
    def receive_data(self, header: dict, data: Union[List[dict], str, bytes], **kwargs) -> bool:
        """ Public function

        This function will get the pushed data and save it to depositor and publish them to related subscribers

        Args:
            header (:obj:`str`): Document Header
            data (:obj:`list` of :obj:`dict`): Data in Python dictioany list format or file_store location str

        Returns:
            :obj:`bool`: If the data should be pushed again

        Notes:
            This function is decorated by @backlog, which means all Exceptions will be sent to internal message topic:
                backlog
        """
        self.log_context['context'] = '-'.join([header['topic_id'], header['table_id']])
        # Step 1: Data Preparation
        if isinstance(data, list):
            tar_full_data = data
        elif header['data_encode'] == 'blob':
            tar_full_data = json.loads(data.decode())
        elif header['data_encode'] == 'b64g':
            tar_full_data = json.loads(gzip.decompress(base64.b64decode(data)).decode())
        elif header['data_encode'] == 'gzip':
            tar_full_data = json.loads(gzip.decompress(data).decode())
        else:
            tar_full_data = json.loads(data)

        saved_headers = self.depositor.add_document(header, tar_full_data)
        # Step 4.2: Check if the first level merge process should be triggered
        for saved_header in saved_headers:
            if saved_header.get('merge_level', 0) > 0 and saved_header.get('merge_status', '') != 'header':
                self.logger.info("Trigger Merging", extra=self.log_context)
                self.trigger_merge(saved_header['topic_id'], saved_header['table_id'],
                                   saved_header['merge_key'], 1, saved_header['merge_level'])
            # Step 4.3: Header related operations
            if saved_header['merge_status'] == 'header':
                self.logger.info("Sending table creation event", extra=self.log_context)
                saved_header['event_type'] = 'target_table_update'
                self.trigger_cockpit(saved_header, tar_full_data)
                self.logger.info("Trigger Cleaning", extra=self.log_context)
                self.trigger_clean(saved_header['topic_id'], saved_header['table_id'], saved_header['start_seq'])
        return True
