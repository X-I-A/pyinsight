import os
import json
import base64
import zipfile
import shutil
import gzip
import logging
from functools import reduce
from ..archiver import Archiver
from pyinsight.utils.core import filter_table_column, record_to_list, list_to_record

class ListArchiver(Archiver):
    def __init__(self):
        super().__init__()
        self.home_path = os.path.join(os.path.expanduser('~'), 'insight-archiver')
        self.data_encode = 'blob'
        self.data_format = 'zst'
        self.data_store = 'file'
        self.supported_encodes = ['blob']
        self.supported_formats = ['zst']

    def init_topic(self, topic_id):
        if not os.path.exists(self.home_path):
            os.makedirs(self.home_path)
        if not os.path.exists(os.path.join(self.home_path, topic_id)):
            os.makedirs(os.path.join(self.home_path, topic_id))

    def _merge_workspace(self):
        field_list = reduce(lambda a, b: set(a) | set(b), self.workspace)
        self.workspace[:] = [{key: [u for i in self.workspace for u in i.get(key, [])] for key in field_list}]

    def set_current_topic_table(self, topic_id, table_id):
        self.topic_id = topic_id
        self.table_id = table_id
        self.topic_path = os.path.join(self.home_path, self.topic_id)
        self.table_path = os.path.join(self.topic_path, self.table_id)
        if not os.path.exists(self.topic_path): os.makedirs(self.topic_path)
        if not os.path.exists(self.table_path): os.makedirs(self.table_path)

    def add_data(self, data):
        list_data = record_to_list(data)
        self.workspace_size += len(json.dumps(list_data))
        self.workspace.append(list_data)

    def remove_data(self):
        self.merge_key, self.workspace, self.workspace_size = '', [{}], 0

    def get_data(self):
        if len(self.workspace) > 1:
            self._merge_workspace()
        return list_to_record(self.workspace[0])

    def archive_data(self):
        if len(self.workspace) > 1:
            self._merge_workspace()
        archive_file_name = os.path.join(self.table_path, self.merge_key + '.zst')
        with zipfile.ZipFile(archive_file_name, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as f:
            for key, value in self.workspace[0].items():
                item_name = base64.b32encode(key.encode()).decode()
                f.writestr(item_name, json.dumps(value))
        return archive_file_name

    def read_data_from_file(self, data_encode, data_format, file_path):
        if data_encode not in self.supported_encodes:
            logging.error("{}-{}: Encode {} not supported".format(self.topic_id, self.table_id, data_encode))
        if data_format not in self.supported_formats:
            logging.error("{}-{}: Format {} not supported".format(self.topic_id, self.table_id, data_encode))
        if not os.path.exists(file_path):
            logging.error("{}-{}: Path {} not found / compatible".format(self.topic_id, self.table_id, file_path))
        # So the data must be a zipped list format local system file:
        with zipfile.ZipFile(file_path) as f:
            return list_to_record({base64.b32decode(item_name).decode(): json.loads(f.read(item_name).decode())
                                   for item_name in f.namelist()})

    def append_archive(self, append_merge_key, fields=None):
        field_list = fields
        archive_file_name = os.path.join(self.table_path, append_merge_key + '.zst')
        with zipfile.ZipFile(archive_file_name) as f:
            list_data = ({base64.b32decode(item_name).decode(): json.loads(f.read(item_name).decode())
                            for item_name in f.namelist() if base64.b32decode(item_name).decode() in field_list})
            list_size = len(json.dumps(list_data))
            self.workspace.append(list_data)
            self.workspace_size += list_size

    def remove_archives(self, merge_key_list):
        for merge_key in merge_key_list:
            os.remove(os.path.join(self.table_path, merge_key + '.zst'))
