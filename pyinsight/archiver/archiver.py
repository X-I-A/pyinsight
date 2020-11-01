from pyinsight.worker import Worker

class Archiver(Worker):
    def __init__(self):
        self.topic_id = None
        self.table_id = None
        self.topic_path = None
        self.table_path = None
        self.merge_key = ''
        self.workspace = list()
        self.workspace_size = 0
        self.data_encode = ''
        self.data_format = ''
        self.data_store = ''
        self.supported_encodes = list()
        self.supported_formats = list()

    def set_merge_key(self, merge_key):
        self.merge_key = merge_key

    def load_archive(self, merge_key, fields=None):
        self.remove_data()
        self.merge_key = merge_key
        self.append_archive(merge_key, fields)

    def set_current_topic_table(self, topic_id, table_id): pass

    def add_data(self, data: list): pass

    def remove_data(self): pass

    def get_data(self) -> dict: pass

    def archive_data(self) -> str: pass

    def read_data_from_file(self, data_encode, data_format, file_path) -> list: pass

    def append_archive(self, append_merge_key, fields=None): pass

    def remove_archives(self, merge_key_list): pass
