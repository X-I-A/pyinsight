import os
import gzip
import json
import logging
from typing import List, Dict, Any
from xialib import Service
from xialib import BasicPublisher
from xialib.storer import Storer
from xialib.publisher import Publisher

__all__ = ['Insight']


class Insight(Service):
    """Insight Application

    Attributes:
        messager (:obj:`Publisher`): A special publisher to handle internal control messages

    Notes:
        Considering implement your own messager, channel and its related topic_ids
    """
    INSIGHT_FIELDS = ['_AGE', '_SEQ', '_NO', '_OP']
    api_url = 'api.x-i-a.com'
    insight_id = ''
    messager = BasicPublisher()
    if not os.path.exists(os.path.join('.', 'insight')):
        os.mkdir(os.path.join('.', 'insight'))  # pragma: no cover
    if not os.path.exists(os.path.join('.', 'insight', 'messager')):
        os.mkdir(os.path.join('.', 'insight', 'messager'))  # pragma: no cover
    channel = os.path.join(os.path.join('.', 'insight', 'messager'))
    topic_cockpit = 'insight-cockpit'
    topic_cleaner = 'insight-cleaner'
    topic_merger = 'insight-merger'
    topic_packager = 'insight-packager'
    topic_loader = 'insight-loader'
    topic_backlog = 'insight-backlog'


    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def set_internal_channel(cls, **kwargs):
        """ Public function

        This function will set the correct internal message channel information

        Warnings:
            Please do not override this function.
        """
        if 'messager' in kwargs:
            messager = kwargs['messager']
            if not isinstance(messager, Publisher):
                logger = logging.getLogger('Insight')
                logger.error("messager should have type of Publisher", extra={'context': ''})
                raise TypeError("INS-000003")
            else:
                Insight.messager = messager
        if 'id' in kwargs:
            Insight.insight_id = kwargs['id']
        if 'channel' in kwargs:
            Insight.channel = kwargs['channel']
        if 'topic_cockpit' in kwargs:
            Insight.topic_cockpit = kwargs['topic_cockpit']
        if 'topic_cleaner' in kwargs:
            Insight.topic_cleaner = kwargs['topic_cleaner']
        if 'topic_merger' in kwargs:
            Insight.topic_merger = kwargs['topic_merger']
        if 'topic_packager' in kwargs:
            Insight.topic_packager = kwargs['topic_packager']
        if 'topic_loader' in kwargs:
            Insight.topic_loader = kwargs['topic_loader']
        if 'topic_backlog' in kwargs:
            Insight.topic_backlog = kwargs['topic_backlog']

    @classmethod
    def trigger_merge(cls, topic_id: str, table_id: str, merge_key: str, merge_level: int, target_merge_level: int):
        header = {'topic_id': topic_id, 'table_id': table_id, 'data_spec': 'internal', 'merge_key': merge_key,
                  'merge_level': str(merge_level), 'target_merge_level': str(target_merge_level)}
        return cls.messager.publish(cls.channel, cls.topic_merger, header, b'[]')

    @classmethod
    def trigger_clean(cls, topic_id: str, table_id: str, start_seq: str):
        header = {'topic_id': topic_id, 'table_id': table_id, 'data_spec': 'internal', 'start_seq': start_seq}
        return cls.messager.publish(cls.channel, cls.topic_cleaner, header, b'[]')

    @classmethod
    def trigger_package(cls, topic_id: str, table_id: str):
        header = {'topic_id': topic_id, 'table_id': table_id, 'data_spec': 'internal'}
        return cls.messager.publish(cls.channel, cls.topic_packager, header, b'[]')

    @classmethod
    def trigger_load(cls, load_config: Dict[str, Any]):
        header = {'load_config': json.dumps(load_config, ensure_ascii=False),
                  'topic_id': load_config['src_topic_id'],
                  'table_id': load_config['src_table_id'],
                  'data_spec': 'internal'}
        return cls.messager.publish(cls.channel, cls.topic_loader, header, b'[]')

    @classmethod
    def trigger_backlog(cls, header: dict, error_body: List[dict]):
        return cls.messager.publish(cls.channel, cls.topic_backlog, header,
                                    gzip.compress(json.dumps(error_body, ensure_ascii=False).encode()))

    @classmethod
    def trigger_cockpit(cls, data_header: dict, data_body: List[dict]):
        assert 'event_type' in data_header
        header_dict = data_header.copy()
        header_dict['data_encode'] = 'gzip'
        resp = cls.messager.publish(cls.channel, cls.topic_cockpit, header_dict,
                                    gzip.compress(json.dumps(data_body, ensure_ascii=False).encode()))
        data_header.pop('event_type', None)
        data_header.pop('evnet_token', None)
        return resp
