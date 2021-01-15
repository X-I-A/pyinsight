import json
import gzip
import base64
import logging
import threading
from typing import List, Dict, Tuple, Union
from xialib import backlog, Depositor, Publisher, Storer, BasicFlower, SegmentFlower
from pyinsight.insight import Insight

__all__ = ['Dispatcher']


class Dispatcher(Insight):
    """Receive pushed data, save to depositor and publish to different destinations

    Attributes:
        storers (:obj:`list` of :obj:`Storer`): Read the data which is not in a message body
        storer_dict (:obj:`list`): data_store Type and its related Storer
        depoistor (:obj:`Depositor`): Depositor attach to this receiver
        publishers (:obj:`dict` of :obj:`Publisher`): publisher id, publisher object
        subscription_list (:obj:`list` of :obj:`list`): Subscription Lists (
            source topic id,
            source table id,
            publisher id,
            target destination,
            target topic id,
            configuration id,
            field list,
            filters list,
            segment_config

    Notes:
        filter list must in the NDF form of list(list(list)))
    """
    def __init__(self, subscription_list: list, publisher: dict, **kwargs):
        super().__init__(publisher=publisher, **kwargs)
        self.logger = logging.getLogger("Insight.Dispatcher")
        self.logger.level = self.log_level
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)


        if not all([item[2] in self.publisher for item in subscription_list]):
            self.logger.error("subscription list contains unknown publisher", extra=self.log_context)
            raise TypeError("INS-000006")
        else:
            self.subscription_list = subscription_list

    def _iter_config_by_src_per_publisher(self, src_topic_id: str, src_table_id: str):
        config_list = [cfg for cfg in self.subscription_list if cfg[0] == src_topic_id and cfg[1] == src_table_id]
        publisher_list = set([cfg[2] for cfg in config_list])
        for publisher_id in publisher_list:
            dest_list = [cfg[3:9] for cfg in config_list if cfg[2] == publisher_id]
            yield {publisher_id: dest_list}


    def _dispatch_data(self, header: dict, full_data: List[dict], publisher: Publisher,
                      dest_list: List[Tuple[str, str, str, list, list]]):
        for destination in dest_list:
            tar_header = header.copy()
            tar_header['src_topic_id'] = tar_header.get('src_topic_id', tar_header['topic_id'])
            tar_header['src_table_id'] = tar_header.get('src_table_id', tar_header['table_id'])
            tar_header['insight_id'] = self.insight_id
            tar_header['topic_id'] = destination[1]
            tar_header['config_id'] = destination[2]
            tar_header.pop('table_id', None)
            basic_flower = BasicFlower(destination[3], destination[4])
            segment_flower = SegmentFlower(destination[5])
            tar_data = basic_flower.proceed(tar_header, full_data)[1]
            tar_header, tar_data = segment_flower.proceed(tar_header, tar_data)
            tar_header['data_encode'] = 'gzip'
            tar_header['data_store'] = 'body'
            self.logger.info("Dispatch to {}-{}-{}".format(destination[0],
                                                           destination[1],
                                                           destination[2]), extra=self.log_context)
            if int(tar_header.get('age', 0)) == 1:
                self.logger.info("Sending table creation event", extra=self.log_context)
                tar_header['event_type'] = 'target_table_update'
                self.trigger_cockpit(tar_header, tar_data)
            publisher.publish(destination[0], destination[1], tar_header,
                              gzip.compress(json.dumps(tar_data, ensure_ascii=False).encode()))


    @backlog
    def dispatch_data(self, header: dict, data: Union[List[dict], str, bytes], **kwargs) -> bool:
        """ Public function

        This function will get the pushed data and publish them to related subscribers

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
        # Step 2: Multi-thread publish
        handlers = list()
        for config in self._iter_config_by_src_per_publisher(src_topic_id, src_table_id):
            for publisher_id, dest_list in config.items():
                publisher = self.publisher.get(publisher_id)
                cur_handler = threading.Thread(target=self._dispatch_data,
                                               args=(header, tar_full_data, publisher, dest_list))
                cur_handler.start()
                handlers.append(cur_handler)
        # Step 3: Wait until all the dispatch thread are finished
        for handler in handlers:
            handler.join()
        return True
