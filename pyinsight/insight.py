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


# Filter Section :
# DNF Filter Related
def xia_eq(a, b):
    return a is not None and a == b

def xia_ge(a, b):
    return a is not None and a >= b

def xia_gt(a, b):
    return a is not None and a > b

def xia_le(a, b):
    return a is not None and a <= b

def xia_lt(a, b):
    return a is not None and a < b

def xia_ne(a, b):
    return a is not None and a != b

# Operation Dictionary:
oper = {'=': xia_eq, '>=': xia_ge, '>': xia_gt, '<=': xia_le, '<': xia_lt, '!=': xia_ne, '<>': xia_ne}

# Get dnf filter field set
def get_fields_from_filter(ndf_filters: List[List[list]]):
    return set([x[0] for l1 in ndf_filters for x in l1 if len(x)>0])

# disjunctive normal form filters (DNF)
def _filter_dnf(line: dict, ndf_filters):
    return any([all([oper.get(l2[1])(line.get(l2[0],None),l2[2]) for l2 in l1 if len(l2)>0]) for l1 in ndf_filters])

# retrieve list of keys from
def _filter_column(line: dict, field_list):
    return {key: value for key, value in line.items() if key in field_list}


class Insight(Service):
    """Insight Application

    Attributes:
        messager (:obj:`Publisher`): A special publisher to handle internal control messages

    Notes:
        Considering implement your own messager, channel and its related topic_ids
    """
    INSIGHT_FIELDS = ['_AGE', '_SEQ', '_NO', '_OP']
    api_url = 'api.x-i-a.com'
    messager = BasicPublisher()
    if not os.path.exists(os.path.join('.', 'insight')):
        os.mkdir(os.path.join('.', 'insight'))  # pragma: no cover
    if not os.path.exists(os.path.join('.', 'insight', 'messager')):
        os.mkdir(os.path.join('.', 'insight', 'messager'))  # pragma: no cover
    channel = os.path.join(os.path.join('.', 'insight', 'messager'))
    topic_cockpit = 'cockpit'
    topic_cleaner = 'cleaner'
    topic_merger = 'merger'
    topic_packager = 'packager'
    topic_loader = 'loader'
    topic_backlog = 'backlog'


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
    def get_minimum_fields(cls, field_list, ndf_filters):
        filter_fields = get_fields_from_filter(ndf_filters)
        return list(set(filter_fields) | set(field_list) | set(Insight.INSIGHT_FIELDS))

    @classmethod
    def filter_table_dnf(cls, dict_list, ndf_filters):
        return [line for line in dict_list if _filter_dnf(line, ndf_filters)]

    @classmethod
    def filter_table_column(cls, dict_list: list, field_list):
        if field_list:
            field_list.extend(cls.INSIGHT_FIELDS)
        return [_filter_column(line, field_list) for line in dict_list]

    @classmethod
    def filter_table(cls, dict_list: list, field_list=list(), filter_list=list(list(list()))):
        if (not filter_list or filter_list == list(list(list()))) and not field_list:
            return dict_list
        elif not filter_list or filter_list == list(list(list())):
            return cls.filter_table_column(dict_list, field_list)
        elif not field_list:
            return cls.filter_table_dnf(dict_list, filter_list)
        else:
            return cls.filter_table_column(cls.filter_table_dnf(dict_list, filter_list), field_list)

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
        data_header['data_encode'] = 'gzip'
        resp = cls.messager.publish(cls.channel, cls.topic_cockpit, data_header,
                                    gzip.compress(json.dumps(data_body, ensure_ascii=False).encode()))
        data_header.pop('event_type', None)
        data_header.pop('evnet_token', None)
        return resp
