import json
import logging
from xialib import backlog
from pyinsight.insight import Insight

__all__ = ['Merger']


class Merger(Insight):
    """Merging data

    Attributes:
        depoistor (:obj:`Depositor`): Depositor attach to the merger
    """

    def __init__(self, depositor, **kwargs):
        super().__init__(depositor=depositor, **kwargs)
        self.logger = logging.getLogger("Insight.Merger")
        self.logger.level = self.log_level
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def merge_data(self, topic_id: str, table_id: str, merge_key: str, merge_level: int):
        self.log_context['context'] = '-'.join([topic_id, table_id])
        self.depositor.set_current_topic_table(topic_id, table_id)
        self.logger.info('Merging {}({})'.format(merge_key, merge_level), extra=self.log_context)
        merge_ok = self.depositor.merge_documents(merge_key, merge_level)
        return True if merge_ok else False

    def merge_all_data(self, topic_id: str, table_id: str):
        self.log_context['context'] = '-'.join([topic_id, table_id])
        self.depositor.set_current_topic_table(topic_id, table_id)
        for doc in self.depositor.get_stream_by_sort_key(min_merge_level=1, status_list=["initial"]):
            doc_header = self.depositor.get_header_from_ref(doc)
            self.merge_data(topic_id, table_id, doc_header["merge_key"], 1)
        # Need to fill the isolated merged fields
        for lvl in range(2, 8):
            for doc in self.depositor.get_stream_by_sort_key(min_merge_level=lvl, status_list=["initial", "merged"]):
                doc_header = self.depositor.get_header_from_ref(doc)
                self.merge_data(topic_id, table_id, doc_header["merge_key"], lvl)
