from pyinsight.action import Action, backlog
from pyinsight.utils.core import get_sort_key_from_dict

__all__ = ['Cleaner']


class Cleaner(Action):
    """
    Clean Old Messages (Depositor and Archive)
    """
    def set_current_topic_table(self, topic_id, table_id):
        self.archiver.set_current_topic_table(topic_id, table_id)
        self.depositor.set_current_topic_table(topic_id, table_id)
        self.log_context['context'] = topic_id + '-' + table_id

    @backlog
    def clean_data(self, topic_id, table_id):
        self.set_current_topic_table(topic_id, table_id)
        base_doc = self.depositor.get_table_header()
        if not base_doc:
            self.logger.warning("No Table Header Found", extra=self.log_context)
            return
        del_list, del_key_list, counter = list(), list(), 0
        base_doc_dict = self.depositor.get_dict_from_ref(base_doc)
        base_doc_key = get_sort_key_from_dict(base_doc_dict)
        for doc in self.depositor.get_stream_by_sort_key(le_ge_key=base_doc_key, reverse=True, ):
            if counter >= 16:
                self.logger.info("{} documents deleted".format(len(del_list)), extra=self.log_context)
                self.depositor.delete_documents(del_list)
                self.logger.info("{} archives deleted".format(len(del_key_list)), extra=self.log_context)
                self.archiver.remove_archives(del_key_list)
                del_list, del_key_list, counter = list(), list(), 0
            doc_dict = self.depositor.get_dict_from_ref(doc)
            if doc_dict['start_seq'] < base_doc_dict['start_seq']:
                del_list.append(doc)
                if doc_dict['data_store'] == 'file':
                    del_key_list.append(doc_dict['merge_key'])
                counter += 1
        if del_list:
            self.logger.info("{} documents deleted".format(len(del_list)), extra=self.log_context)
            self.depositor.delete_documents(del_list)
            self.logger.info("{} archives deleted".format(len(del_key_list)), extra=self.log_context)
            self.archiver.remove_archives(del_key_list)
        return True

    @backlog
    def remove_all_data(self, topic_id, table_id):
        self.set_current_topic_table(topic_id, table_id)
        del_list, del_key_list, counter = list(), list(), 0
        for doc in self.depositor.get_stream_by_sort_key():
            if counter >= 16:
                self.logger.info("{} documents deleted".format(len(del_list)), extra=self.log_context)
                self.depositor.delete_documents(del_list)
                self.logger.info("{} archives deleted".format(len(del_key_list)), extra=self.log_context)
                self.archiver.remove_archives(del_key_list)
                del_list, del_key_list, counter = list(), list(), 0
            doc_dict = self.depositor.get_dict_from_ref(doc)
            del_list.append(doc)
            if doc_dict['data_store'] == 'file':
                del_key_list.append(doc_dict['merge_key'])
        if del_list:
            self.logger.info("{} documents deleted".format(len(del_list)), extra=self.log_context)
            self.depositor.delete_documents(del_list)
            self.logger.info("{} archives deleted".format(len(del_key_list)), extra=self.log_context)
            self.archiver.remove_archives(del_key_list)
        return True