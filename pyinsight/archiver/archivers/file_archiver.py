import os
import json
import shutil
from pyinsight import archiver
from pyinsight.utils.core import *

class FileArchiver(archiver.Archiver):
    home_path = os.path.expanduser('~')
    data_encode = 'gzip'
    data_format = 'record'
    data_store = 'file'

    def __init__(self):
        super().__init__()
        self.workspace = [[]] # List of record data type in FileArchiver
        self.home_path = os.path.join(self.home_path, 'insight-archive')
        for name, path in self.__dict__.items():
            if name.endswith('_path') and not os.path.exists(path):
                os.makedirs(path)

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

    def read_data_from_file(self, path):
        with gzip.open(path) as f:
            return json.load(f)

    def append_archive(self, append_merge_key, fields=None, filters=None):
        field_list, filter_list = fields, filters
        if not filters:
            filter_list = [[[]]]
        with gzip.open(os.path.join(self.table_path, append_merge_key + '.gz')) as f:
            raw_data = f.read()
            table_data = json.loads(raw_data)

            if filter_list == [[[]]] and not field_list:
                table_size = len(raw_data)
            elif filter_list == [[[]]]:
                table_data = [filter_column(line, field_list) for line in table_data]
                table_size = len(json.dumps(table_data))
            else:
                table_data = [line for line in table_data if filter_dnf(line, filter_list)]
                if field_list:
                    table_data = [filter_column(line, field_list) for line in table_data]
                table_size = len(json.dumps(table_data))
            self.workspace.append(table_data)
            self.workspace_size += table_size

    def remove_archives(self, merge_key_list):
        for merge_key in merge_key_list:
            os.remove(os.path.join(self.table_path, merge_key + '.gz'))

    def receive_archive(self, from_path, merge_key):
        archive_file_name = os.path.join(self.table_path, merge_key + '.gz')
        if archive_file_name != from_path:
            shutil.copy2(from_path, archive_file_name)
        return archive_file_name
