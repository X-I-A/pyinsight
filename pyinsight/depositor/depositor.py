from pyinsight.worker import Worker

class Depositor(Worker):
    def __init__(self):
        self.topic_id = None
        self.table_id = None
        self.data_encode = ''

    def set_current_topic_table(self, topic_id, table_id): pass

    def add_document(self, header, data) -> bool: pass

    def update_document(self, ref, doc_dict) -> bool: pass

    def delete_documents(self, ref_list) -> bool: pass

    def get_table_header(self): pass

    def get_ref_by_merge_key(self, merge_key) : pass

    def get_dict_from_ref(self, ref) -> dict: pass

    def get_stream_by_sort_key(self, status_list=None, le_ge_key=None, reverse=False,
                               min_merge_level=0, equal=True): pass

    def merge_documents(self, base_doc, merge_flag, start_key, end_key,
                        data_list, min_start=None, merged_level=0) -> dict: pass