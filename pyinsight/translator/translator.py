import json
import base64
import gzip
import logging
import pyinsight.utils.core

class Translator():
    # Simplest Case : No data spec change
    spec_list = ['x-i-a']

    def __init__(self):
        pass

    @classmethod
    def encoder(cls, data, src_encode, tar_encode):
        return pyinsight.utils.core.encoder(data, src_encode, tar_encode)

    # Depositor Scope - Data format = record
    def get_depositor_data(self, data, src_encode, tar_encode, header):
        record_data = json.loads(self.encoder(data, src_encode, 'flat'))
        translated_data = self.get_archive_data(record_data, header)
        return self.encoder(json.dumps(translated_data), 'flat', tar_encode)

    # Archive Scope - data is a python dictionary list
    def get_archive_data(self, data, header): pass