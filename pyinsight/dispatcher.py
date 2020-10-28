import json
import gzip
import base64
from pyinsight.action import Action
from pyinsight.utils.core import filter_column, filter_dnf

__all__ = ['Dispatcher']

"""
# Send document to subscriptors
## Data Structure - subscriptions
* Key : Tuple of (topic_id, table_id) -> Source
* Value: List of destinations
* Destinations: topic_id, table_id, fields, filters (NDF)
"""
class Dispatcher(Action):
    X_I_HEADER = ['start_seq', 'aged', 'age', 'end_age', 'segment_start_age',
                  'data_encode', 'data_format', 'data_store', 'data_spec']
    messager_only = False
    subscription = dict()

    # Send different message based on messager blob support mode
    def _send_message(self, topic_id, header, data):
        if header['data_store'] == 'body':
            compress_data = gzip.compress(json.dumps(data).encode())
            if self.messager.blob_support:
                header['data_encode'] = 'gzip'
                return self.messager.publish(topic_id, header, compress_data)
            else:
                header['data_encode'] = 'b64g'
                return self.messager.publish(topic_id, header, base64.b64encode(compress_data).decode())
        else:
            return self.messager.publish(topic_id, header, data)

    # Get destination list:
    def get_destinations(self, src_topic_id, src_table_id) -> list:
        return self.subscription.get((src_topic_id, src_table_id), [])

    # Data dispatch
    def dispatch(self, src_header, src_body_data, src_file_data, tar_topic_id=None, tar_table_id=None):
        destinations = self.get_destinations(src_header['topic_id'], src_header['table_id'])
        for destination in destinations:
            if tar_topic_id and tar_topic_id != destination['topic_id'] or \
                tar_table_id and tar_table_id != destination['table_id']:
                continue
            # Build Header
            tar_header = {'table_id': destination['table_id']}
            tar_body_data = src_body_data
            tar_file_data = src_file_data
            for key in [k for k in self.X_I_HEADER if k in src_header]:
                tar_header[key] = src_header[key]
            # Build Data
            if destination.get('fields', None):
                field_list = destination['fields']
                field_list.extend(['_AGE', '_SEQ', '_NO', '_OP'])
            else:
                field_list = None
            filter_list = destination.get('filters', [[[]]])
            # Case 1: Header => No modification
            if int(src_header.get('age', 0)) == 1:
                return self._send_message(destination['topic_id'], tar_header, tar_body_data)
            # Case 2: Body Message Send Only
            elif src_header['data_store'] == 'body':
                if filter_list == [[[]]] and not field_list:
                    pass
                elif filter_list == [[[]]]:
                    tar_body_data = [filter_column(line, field_list) for line in src_body_data]
                else:
                    tar_body_data = [line for line in src_body_data if filter_dnf(line, filter_list)]
                    if field_list:
                        tar_body_data = [filter_column(line, field_list) for line in tar_body_data]
                return self._send_message(destination['topic_id'], tar_header, tar_body_data)
            # Case 3: File Message
            elif src_header['data_store'] == 'file':
                if filter_list == [[[]]] and not field_list:
                    pass
                elif filter_list == [[[]]]:
                    tar_file_data = [filter_column(line, field_list) for line in src_file_data]
                else:
                    tar_file_data = [line for line in src_file_data if filter_dnf(line, filter_list)]
                    if field_list:
                        tar_file_data = [filter_column(line, field_list) for line in tar_file_data]
                # Case 3.1 Send content by message
                if self.messager_only:
                    tar_header['data_store'] = 'body'
                    # Case 3.1.1 : Aged Document => Must be cut age by age
                    if 'age' in src_header:
                        tar_file_data = sorted(tar_file_data, key=lambda line: line['_AGE'])
                        tar_msg_data, start_age, cur_age = list(), 0, 0
                        for line in tar_file_data:
                            if start_age == 0:
                                start_age = int(src_header['age'])
                            if line['_AGE'] != cur_age and tar_msg_data:
                                tar_header['age'] = start_age
                                tar_header['end_age'] = line['_AGE'] - 1
                                self._send_message(destination['topic_id'], tar_header, tar_msg_data)
                                tar_msg_data = list()
                                start_age = line['_AGE']
                            tar_msg_data.append(line)
                            cur_age = line['_AGE']
                        tar_header['age'] = start_age
                        tar_header['end_age'] = int(src_header.get('end_age', src_header['age']))
                        self._send_message(destination['topic_id'], tar_header, tar_msg_data)
                    # Case 3.2.2 : Normal Document
                    else:
                        tar_file_data = sorted(tar_file_data, key=lambda line: line['_SEQ'])
                        tar_msg_data, cur_seq = list(), ''
                        for line in tar_file_data:
                            if not cur_seq:
                                cur_seq = line['_SEQ']
                            if line['_SEQ'] != cur_seq and tar_msg_data:
                                tar_header['start_seq'] = cur_seq
                                self._send_message(destination['topic_id'], tar_header, tar_msg_data)
                                tar_msg_data = list()
                            tar_msg_data.append(line)
                            cur_seq = line['_SEQ']
                        tar_header['start_seq'] = cur_seq
                        self._send_message(destination['topic_id'], tar_header, tar_msg_data)
                # Case 3.2 Send content by file
                else:
                    if 'age' in src_header:
                        merge_key = str(int(src_header['start_seq']) + int(src_header['age']))
                    else:
                        merge_key = src_header['start_seq']
                    self.archiver.set_current_topic_table(destination['topic_id'], destination['table_id'])
                    self.archiver.set_merge_key(merge_key)
                    self.archiver.add_data(tar_file_data)
                    tar_body_data = self.archiver.archive_data()
                    self.archiver.remove_data()
                    return self._send_message(destination['topic_id'], tar_header, tar_body_data)
