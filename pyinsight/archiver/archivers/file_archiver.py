import os
import json
import shutil
import gzip
import logging
from pyinsight import archiver
from pyinsight.utils.core import filter_table_column

class FileArchiver(archiver.Archiver):

    def __init__(self):
        super().__init__()
        self.home_path = os.path.join(os.path.expanduser('~'), 'insight-archiver')
        self.data_encode = 'gzip'
        self.data_format = 'record'
        self.data_store = 'file'
        self.supported_encodes = ['gzip']
        self.supported_formats = ['record']

    def init_topic(self, topic_id):
        if not os.path.exists(self.home_path):
            os.makedirs(self.home_path)
        if not os.path.exists(os.path.join(self.home_path, topic_id)):
            os.makedirs(os.path.join(self.home_path, topic_id))

    def _merge_workspace(self):
        self.workspace[:] = [[unit for item in self.workspace for unit in item]]

    def set_current_topic_table(self, topic_id, table_id):
        self.topic_id = topic_id
        self.table_id = table_id
        self.topic_path = os.path.join(self.home_path, self.topic_id)
        self.table_path = os.path.join(self.topic_path, self.table_id)
        if not os.path.exists(self.topic_path): os.makedirs(self.topic_path)
        if not os.path.exists(self.table_path): os.makedirs(self.table_path)

    def add_data(self, data):
        self.workspace_size += len(json.dumps(data))
        self.workspace.append(data)

    def remove_data(self):
        self.merge_key, self.workspace, self.workspace_size = '', [[]], 0

    def get_data(self):
        if len(self.workspace) > 1:
            self._merge_workspace()
        return self.workspace[0]

    def archive_data(self):
        if len(self.workspace) > 1:
            self._merge_workspace()
        archive_file_name = os.path.join(self.table_path, self.merge_key + '.gz')
        with gzip.open(archive_file_name, 'wb') as f:
            f.write(json.dumps(self.workspace[0]).encode())
        return archive_file_name

    def read_data_from_file(self, data_encode, data_format, file_path):
        if data_encode not in self.supported_encodes:
            logging.error("{}-{}: Encode {} not supported".format(self.topic_id, self.table_id, data_encode))
        if data_format not in self.supported_formats:
            logging.error("{}-{}: Format {} not supported".format(self.topic_id, self.table_id, data_encode))
        if not os.path.exists(file_path):
            logging.error("{}-{}: Path {} not found / compatible".format(self.topic_id, self.table_id, file_path))
        # So the data must be a zipped record local system file:
        with gzip.open(file_path) as f:
            return json.load(f)

    def append_archive(self, append_merge_key, fields=None):
        field_list = fields
        with gzip.open(os.path.join(self.table_path, append_merge_key + '.gz')) as f:
            raw_data = f.read()
            table_data = json.loads(raw_data)
            if field_list:
                table_data = filter_table_column(table_data, field_list)
            table_size = len(json.dumps(table_data))
            self.workspace.append(table_data)
            self.workspace_size += table_size

    def remove_archives(self, merge_key_list):
        for merge_key in merge_key_list:
            os.remove(os.path.join(self.table_path, merge_key + '.gz'))
