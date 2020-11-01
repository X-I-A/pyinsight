import os
import json
from pyinsight import depositor
from pyinsight.utils.core import get_current_timestamp, encoder, get_merge_level

class FileDepositor(depositor.Depositor):
    def __init__(self):
        super().__init__()
        self.data_encode = 'b64g'
        self.file_type = {'initial': '.initial', 'merged': '.merged', 'packaged': '.packaged'}
        self.home_path = os.path.join(os.path.expanduser('~'), 'insight-depositor')

    def init_topic(self, topic_id):
        if not os.path.exists(self.home_path):
            os.makedirs(self.home_path)
        if not os.path.exists(os.path.join(self.home_path, topic_id)):
            os.makedirs(os.path.join(self.home_path, topic_id))

    def _get_ref_from_filename(self, filename):
        file = filename.split('.')[0]
        if os.path.exists(os.path.join(self.table_path, file+'.initial')):
            return file+'.initial'
        elif os.path.exists(os.path.join(self.table_path, file+'.merged')):
            return file+'.merged'
        elif os.path.exists(os.path.join(self.table_path, file+'.header')):
            return file+'.header'
        else:
            return file+'.packaged'

    def add_document(self, header, data) -> bool:
        self.set_current_topic_table(header['topic_id'], header['table_id'])
        content = header.copy()
        # Encoder
        if content['data_store'] == 'body':
            if isinstance(data, list):
                content['data'] = encoder(json.dumps(data), content['data_encode'], self.data_encode)
            else:
                content['data'] = encoder(data, content['data_encode'], self.data_encode)
            content['data_encode'] = self.data_encode
        else:
            content['data'] = data
        # Case 1 : Header
        if content.get('age', '') == '1':
            content['age'] = 1
            filename = content['start_seq'] + '.header'
            content['aged'] = content.get('aged', '') == 'true'
        # Case 2 : Aged Document
        elif 'age' in content:
            for key in [k for k in ['age', 'end_age', 'segment_start_age'] if k in content]:
                content[key] = int(content[key])
            filename = content['merge_key'] + self.file_type.get(header['merge_status'])
        # Case 3 : Normal Document
        else:
            content['deposit_at'] = get_current_timestamp()
            filename = content['deposit_at'] + '-' + content['merge_key'] + self.file_type.get(header['merge_status'])
        # Save at last
        with open(os.path.join(self.table_path, filename), 'w') as f:
            f.write(json.dumps(content))
        return True

    def update_document(self, ref, doc_dict) -> bool:
        ori_filename = os.path.join(self.table_path, ref)
        tar_filename = os.path.join(self.table_path, '.'.join([ref.split('.')[0], doc_dict['merge_status']]))
        with open(tar_filename, 'w') as f:
            f.write(json.dumps(doc_dict))
        if ori_filename != tar_filename:
            os.remove(ori_filename)
        return True

    # All of the following method need
    def set_current_topic_table(self, topic_id, table_id):
        self.topic_id = topic_id
        self.table_id = table_id
        self.topic_path = os.path.join(self.home_path, self.topic_id)
        self.table_path = os.path.join(self.topic_path, self.table_id)
        if not os.path.exists(self.topic_path): os.makedirs(self.topic_path)
        if not os.path.exists(self.table_path): os.makedirs(self.table_path)

    def get_table_header(self):
        header_list = [f for f in os.listdir(self.table_path) if f.endswith(('.header'))]
        if not header_list: # No File Found
            return None
        else:
            return max(header_list)

    def get_ref_by_merge_key(self, merge_key):
        # Normal Doc
        normal_doc_suffix = '-' + merge_key
        based_doc_query = [f for f in os.listdir(self.table_path) if normal_doc_suffix in f]
        if based_doc_query:
            return self._get_ref_from_filename(max(based_doc_query))
        # Aged Doc
        aged_doc_filename = self._get_ref_from_filename(merge_key+'.')
        if os.path.exists(os.path.join(self.table_path,aged_doc_filename)):
            return aged_doc_filename

    # In the file depositor, sorted_key = first 20 character of filename
    def get_stream_by_sort_key(self, status_list=None, le_ge_key=None, reverse=False, min_merge_level=0, equal=True):
        if not status_list:
            status_list = ['header', 'initial', 'merged', 'packaged']
        if reverse:
            if not le_ge_key:
                doc_list = sorted([f for f in os.listdir(self.table_path)
                                   if f.endswith(tuple(status_list))], reverse=True)
            elif equal:
                doc_list = sorted([f for f in os.listdir(self.table_path)
                                   if f.endswith(tuple(status_list)) and f[:20] <= le_ge_key], reverse=True)
            else:
                doc_list = sorted([f for f in os.listdir(self.table_path)
                                   if f.endswith(tuple(status_list)) and f[:20] < le_ge_key], reverse=True)
        else:
            if not le_ge_key:
                doc_list = sorted([f for f in os.listdir(self.table_path)
                                   if f.endswith(tuple(status_list))])
            elif equal:
                doc_list = sorted([f for f in os.listdir(self.table_path)
                                   if f.endswith(tuple(status_list)) and f[:20] >= le_ge_key])
            else:
                doc_list = sorted([f for f in os.listdir(self.table_path)
                                   if f.endswith(tuple(status_list)) and f[:20] > le_ge_key])
        # Merge Level Check
        for doc in doc_list:
            if not doc.endswith('.header') and get_merge_level((doc.split('.')[0])[-20:]) < min_merge_level:
                continue
            yield self._get_ref_from_filename(doc)

    def get_dict_from_ref(self, doc_ref):
        with open(os.path.join(self.table_path, self._get_ref_from_filename(doc_ref))) as f:
            return json.load(f)

    def delete_documents(self, ref_list) -> bool:
        for file_to_delete in ref_list:
            os.remove(os.path.join(self.table_path, self._get_ref_from_filename(file_to_delete)))
        return True

    def merge_documents(self, base_doc, merge_flag, start_key, end_key, data_list,
                        min_start=None, merged_level=0) -> bool:
        base_doc_dict = self.get_dict_from_ref(base_doc)
        data_operation_flag = False
        if 'age' in base_doc_dict:
            aged_flag = True
            segment_key_start = base_doc_dict.get('segment_start_age', 0)
            doc_key_start = base_doc_dict['age']
            doc_key_end = base_doc_dict.get('end_age', base_doc_dict['age'])
        else:
            aged_flag = False
            segment_key_start = base_doc_dict.get('segment_start_time', 0)
            doc_key_start = base_doc_dict.get('start_time', base_doc_dict['deposit_at'])
            doc_key_end = base_doc_dict['deposit_at']
        # Case 1: Merged Member Nodes / Leader with same min_start = Do nothing
        if base_doc_dict['merge_status'] == 'merged' and base_doc_dict['merged_level'] == merged_level:
            if not min_start or min_start == segment_key_start:
                return data_operation_flag
        # Case 2: Leader nodes, need to update min_start
        if base_doc_dict.get('merged_level', -1) != merged_level:
            base_doc_dict['merged_level'] = merged_level
        if min_start:
            if aged_flag:
                base_doc_dict['segment_start_age'] = min_start
            else:
                base_doc_dict['segment_start_time'] = min_start
        # Case 3: Check if data should be manimulated
        if doc_key_end == end_key and doc_key_start == start_key:
            pass
        elif base_doc_dict['merge_status'] == 'merged':
            pass
        else:
            base_doc_dict['data'] = encoder(json.dumps(data_list), 'flat', self.data_encode)
            data_operation_flag = True
            if aged_flag:
                base_doc_dict.update({'age': start_key, 'end_age': end_key})
            else:
                base_doc_dict.update({'start_time': start_key})
        # Case 4.1: Final Merge cases
        if merge_flag:
            base_doc_dict['merge_status'] = 'merged'
            with open(os.path.join(self.table_path, base_doc.split('.')[0] + '.merged'), 'w') as f:
                f.write(json.dumps(base_doc_dict))
            if self._get_ref_from_filename(base_doc).endswith('.initial'):
                os.remove(os.path.join(self.table_path, base_doc))
        # Case 4.2: Size not enough : Not a final merge case
        else:
            with open(os.path.join(self.table_path, base_doc), 'w') as f:
                f.write(json.dumps(base_doc_dict))
        return data_operation_flag