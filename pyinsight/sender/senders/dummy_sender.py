import os
import base64
import json
from ..sender import Sender

class DummySender(Sender):
    def _get_filename_from_dict(self, header: dict):
        if int(header.get('age', 0)) > 1:
            return str(int(header['start_seq']) + int(header['age']))
        else:
            return header['start_seq']

    def _send_by_file(self, header: dict, gzip_data: bytes):
        file_name = os.path.join(self.file_client, self._get_filename_from_dict(header) + '.gz')
        with open(file_name, 'wb') as f:
            f.write(gzip_data)
        return file_name

    def _send_by_message(self, header: dict, gzip_data):
        file_name = os.path.join(self.message_client, self._get_filename_from_dict(header))
        if header['data_store'] == 'body':
            header['data_encode'] = 'b64g'
            header['data'] = base64.b64encode(gzip_data).decode()
        else:
            header['data'] = gzip_data
        with open(file_name, 'w') as f:
            f.write(json.dumps(header))
        return file_name