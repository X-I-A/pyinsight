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
        depoistor (:obj:`Depositor`): Depositor attach to this receiver

    Notes:
        filter list must in the NDF form of list(list(list)))
    """
    def __init__(self, depositor, **kwargs):
        super().__init__(depositor=depositor, **kwargs)
        self.logger = logging.getLogger("Insight.Receiver")
        self.logger.level = self.log_level
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

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
        return True if saved_headers else False
