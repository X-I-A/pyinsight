import logging
from xialib.archiver import Archiver
from xialib.depositor import Depositor
from pyinsight.insight import Insight, backlog

__all__ = ['Cleaner']


class Cleaner(Insight):
    """Clean old messager

    Move the data from depositor to archiver. Design to store huge amount of data on column usage

    Attributes:
        depoistor (:obj:`Depositor`): Data is retrieve from depositor
        archiver (:obj:`Archiver`): Data is saved to archiver
    """
    def __init__(self, depositor: Depositor, archiver: Archiver, **kwargs):
        super().__init__(depositor=depositor, archiver=archiver)
        self.logger = logging.getLogger("Insight.Cleaner")
        self.logger.level = self.log_level
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    @backlog
    def clean_data(self, topic_id: str, table_id: str, start_seq: str, **kwargs):
        """ Public function

        This function will cleaning the old data

        Args:
            topic_id (:obj:`str`): Topic id
            table_id (:obj:`str`): Table id
            start_seq (:obj:`str`): Data before this start_seq will be deleted

        Returns:
            :obj:`bool`: If the data should be pushed again

        Notes:
            This function is decorated by @backlog, which means all Exceptions will be sent to internal message topic:
            backlog
        """
        del_list, del_key_list, counter = list(), list(), 0
        # For header lines, start_seq = sort key, that is why we can use start_seq here
        self.depositor.set_current_topic_table(topic_id, table_id)
        self.archiver.set_current_topic_table(topic_id, table_id)
        self.log_context['context'] = '-'.join([topic_id, table_id])
        for doc in self.depositor.get_stream_by_sort_key(le_ge_key=start_seq, reverse=True, equal=False):
            if counter >= 16:
                self.logger.info("{} documents deleted".format(len(del_list)), extra=self.log_context)
                self.depositor.delete_documents(del_list)
                self.logger.info("{} archives deleted".format(len(del_key_list)), extra=self.log_context)
                self.archiver.remove_archives(del_key_list)
                del_list, del_key_list, counter = list(), list(), 0
            doc_dict = self.depositor.get_header_from_ref(doc)
            if doc_dict['start_seq'] < start_seq:
                del_list.append(doc)
                if doc_dict['data_store'] != 'body':
                    del_key_list.append(doc_dict['merge_key'])
                counter += 1
        if del_list:
            self.logger.info("{} documents deleted".format(len(del_list)), extra=self.log_context)
            self.depositor.delete_documents(del_list)
            self.logger.info("{} archives deleted".format(len(del_key_list)), extra=self.log_context)
            self.archiver.remove_archives(del_key_list)
        return True
