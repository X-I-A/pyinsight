import socket
import logging
from typing import List, Tuple
from pyinsight.insight import Insight

__all__ = ['Caller']

def prepare_source_table_init(data_header: dict, data_body: List[dict]):
    post_path = '/events/source-table-init'
    post_data = dict()
    post_data['event_type'] = 'source_table_init'
    post_data['event_token'] = data_header['event_token']
    post_data['source_id'] = data_header.get('source_id', data_header['table_id'])
    post_data['start_seq'] = data_header['start_seq']
    post_data['data'] = []
    return post_path, post_data

def prepare_target_table_update(data_header: dict, data_body: List[dict]):
    post_path = '/events/target-table-update'
    post_data = dict()
    post_data['event_type'] = data_header['event_type']
    post_data['source_id'] = data_header.get('source_id', data_header['table_id'])
    post_data['start_seq'] = data_header['start_seq']
    post_data['topic_id'] = data_header['topic_id']
    post_data['table_id'] = data_header.get('table_id', post_data['source_id'])
    post_data['data'] = [{key: value for key, value in line.items() if not key.startswith('_')} for line in data_body]
    return post_path, post_data

class Caller(Insight):
    """Call X-I-A Public API

    Triggered by cockpit and prepare X-I-A cockpit API call

    Attributes:
        insight_id (:obj:`str`): Insight ID in the form of url (domain name without path)

    """
    method_dict = {
        'source_table_init': prepare_source_table_init,
        'target_table_update': prepare_target_table_update,
    }

    def __init__(self, insight_id: str, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger("Insight.Caller")
        self.logger.level = self.log_level
        self.insight_id = None
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

        try:
            socket.getaddrinfo(insight_id, None)
            self.insight_id = insight_id
        except socket.gaierror:
            self.logger.error("Insight ID must be an existed domain", extra=self.log_context)
            raise TypeError("INS-000009")

        if self.insight_id is None:
            self.logger.error("Insight ID must be an existed domain", extra=self.log_context)  # pragma: no cover
            raise TypeError("INS-000009")  # pragma: no cover

    def prepare_call(self, data_header: dict, data_body: List[dict]) -> Tuple[str, str, dict]:
        """ Public function

        This function will prepared the call data with the correct API path

        Args:
            data_header (:obj:`str`): Document Header
            data_body (:obj:`list` of :obj:`dict`): Data in Python dictioany list format

        Returns:
            :obj:`str`: url to post (insight_id)
            :obj:`str`: path to post
            :obj:`dict`: json compatible dict to be sent as data body

        Notes:
            This function is decorated by @backlog, which means all Exceptions will be sent to internal message topic:
                backlog
        """
        event_type = data_header.get('event_type', None)
        prepare_method = self.method_dict.get(event_type, None)
        if prepare_method is None or not callable(prepare_method):
            self.logger.error("Unable to preparer call with {}".format(event_type), extra=self.log_context)
            raise ValueError("INS-000010")

        path, json_data = prepare_method(data_header, data_body)
        return self.insight_id, path, json_data