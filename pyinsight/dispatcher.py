import json
import gzip
import base64
import logging
import threading
from typing import List, Dict, Tuple, Union
from xialib.depositor import Depositor
from xialib.publisher import Publisher
from xialib.storer import Storer
from pyinsight.insight import Insight, backlog

__all__ = ['Dispatcher']


class Dispatcher(Insight):
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
        self.logger = logging.getLogger("Insight.Dispatcher")
        self.logger.level = self.log_level
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

        if 'subscription_list' in kwargs:
            subscription_list = kwargs['subscription_list']
            if 'publishers' not in kwargs:
                self.logger.error("Subsciption list contains no publisher", extra=self.log_context)
                raise TypeError("INS-000008")

            if not all([all([next(iter(client)) in self.publishers for client in clients])
                        for k, clients in subscription_list.items()]):
                self.logger.error("subscription list contains unknown publisher", extra=self.log_context)
                raise TypeError("INS-000006")
            else:
                self.subscription_list = subscription_list
        else:
            self.subscription_list = {}

        if 'depositor' not in kwargs:
            self.depositor = None

    def _dispatch_data(self, header: dict, full_data: List[dict], publisher: Publisher,
                      dest_list: List[Tuple[str, str, str, list, list]]):
        for destination in dest_list:
            tar_header = header.copy()
            tar_header['topic_id'] = destination[1]
            tar_header['table_id'] = destination[2]
            if int(tar_header.get('age', 0)) == 1:
                tar_data = full_data
            else:
                tar_data = self.filter_table(full_data, destination[3], destination[4])
            tar_header['data_encode'] = 'gzip'
            tar_header['data_store'] = 'body'
            self.logger.info("Dispatch to {}-{}-{}".format(destination[0],
                                                           destination[1],
                                                           destination[2]), extra=self.log_context)
            publisher.publish(destination[0], destination[1], tar_header,
                              gzip.compress(json.dumps(tar_data, ensure_ascii=False).encode()))

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
        src_topic_id = header['topic_id']
        src_table_id = header['table_id']
        self.log_context['context'] = '-'.join([src_topic_id, src_table_id])
        # Step 1: Data Preparation
        if header['data_store'] != 'body':
            active_storer = self.storer_dict.get(header['data_store'], None)
            if active_storer is None:
                self.logger.error("No storer for store type {}".format(header['data_store']), extra=self.log_context)
                raise ValueError("INS-000005")
            header['data_store'] = 'body'
            tar_full_data = json.loads(gzip.decompress(active_storer.read(data)).decode())
        elif isinstance(data, list):
            tar_full_data = data
        elif header['data_encode'] == 'blob':
            tar_full_data = json.loads(data.decode())
        elif header['data_encode'] == 'b64g':
            tar_full_data = json.loads(gzip.decompress(base64.b64decode(data)).decode())
        elif header['data_encode'] == 'gzip':
            tar_full_data = json.loads(gzip.decompress(data).decode())
        else:
            tar_full_data = json.loads(data)
        # Step 2: Multi-thread publish
        handlers = list()
        for client_config in self.subscription_list.get((src_topic_id, src_table_id), list()):
            for publisher_id, dest_list in client_config.items():
                publisher = self.publishers.get(publisher_id)
                cur_handler = threading.Thread(target=self._dispatch_data,
                                               args=(header, tar_full_data, publisher, dest_list))
                cur_handler.start()
                handlers.append(cur_handler)
        # Step 3.1: Add to depositor if depositor is initialized
        if self.depositor:
            saved_headers = self.depositor.add_document(header, tar_full_data)
            # Step 3.2: Check if the first level merge process should be triggered
            for saved_header in saved_headers:
                if saved_header.get('merge_level', 0) > 0 and saved_header.get('merge_status', '') != 'header':
                    self.logger.info("Trigger Merging", extra=self.log_context)
                    self.trigger_merge(saved_header['topic_id'], saved_header['table_id'],
                                       saved_header['merge_key'], 1, saved_header['merge_level'])
                # Step 3.3: Trigger clean
                if saved_header['merge_status'] == 'header':
                    self.logger.info("Trigger Cleaning", extra=self.log_context)
                    self.trigger_clean(saved_header['topic_id'], saved_header['table_id'], saved_header['start_seq'])
        # Step 4: Wait until all the dispatch thread are finished
        for handler in handlers:
            handler.join()
        return True
