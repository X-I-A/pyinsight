from pyinsight.worker import Worker

class Archiver(Worker):
    topic_id = None
    table_id = None
    topic_path = None
    table_path = None
    merge_key = ''
    workspace = list()
    workspace_size = 0
    data_encode = ''
    data_format = ''
    data_store = ''
    supported_encodes = list()
    supported_formats = list()

    def __init__(self): pass

    def set_merge_key(self, merge_key):
        self.merge_key = merge_key

    def load_archive(self, merge_key, fields=None, filters=None):
        self.remove_data()
        self.merge_key = merge_key
        self.append_archive(merge_key, fields, filters)

    def set_current_topic_table(self, topic_id, table_id): pass

    def add_data(self, data): pass

    def remove_data(self): pass

    def get_data(self) -> dict: pass

    def archive_data(self) -> str: pass

    def read_data_from_file(self, data_encode, data_format, file_path) -> list: pass

    def append_archive(self, append_merge_key, fields=None, filters=None): pass

    def remove_archives(self, merge_key_list): pass
