import json
import logging
from xialib.depositor import Depositor
from pyinsight.insight import Insight, backlog

__all__ = ['Merger']


class Merger(Insight):
    """Merging data

    Attributes:
        depoistor (:obj:`Depositor`): Depositor attach to the merger
    """

    def __init__(self, depositor, **kwargs):
        super().__init__(depositor=depositor)
        self.logger = logging.getLogger("Insight.Merger")
        self.logger.level = self.log_level
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    @backlog
    def merge_data(self, topic_id: str, table_id: str, merge_key: str, merge_level: int, target_merge_level: int,
                   **kwargs):
        self.log_context['context'] = '-'.join([topic_id, table_id])
        self.depositor.set_current_topic_table(topic_id, table_id)
        self.logger.info('Merging {}({})'.format(merge_key, merge_level), extra=self.log_context)
        merge_ok = self.depositor.merge_documents(merge_key, merge_level)
        if merge_ok:
            if target_merge_level > merge_level:
                self.trigger_merge(topic_id, table_id, merge_key, merge_level + 1, target_merge_level)
            return True
        else:
            return False
