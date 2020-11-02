import json
import logging
import pyinsight
from pyinsight.transfer import Transfer
from pyinsight.dispatcher import Dispatcher
from pyinsight.utils.core import get_sort_key_from_dict, encoder

__all__ = ['Loader']

"""
Load and Dispatch Message
Dispatcher with only one configuration
Load Configuration Structure:
src_topic_id, src_table_id
client_id, tar_topic_id, tar_table_id
load_type: 'initial', 'header', 'normal', 'packaged'
Range of load: 'start_key': str, 'end_key': str
initial load_sequence:
* Start by header
* header thread = find the first merged package
* create package load chain (do one and create two logic)
* load the merged and initial segment
* create package load chain if met
Best Pratice: Deploy un loader per client_id
"""

class Loader(Transfer):
    def __init__(self, messager=None, depositor=None, archiver=None, translators=list()):
        super().__init__(messager=None, depositor=None, archiver=None, translators=list())
        self.dispatcher = None

    # Head Load: Simple Sent
    def _header_load(self, header_dict, tar_topic_id, tar_table_id):
        tar_header = header_dict
        tar_body_data = tar_header.pop('data')
        self.dispatch_data(self.dispatcher, tar_header, tar_body_data, tar_topic_id, tar_table_id)

    # Normal Load : Send One by One
    def _normal_load(self, header_dict, tar_topic_id, tar_table_id, start_key, end_key):
        for doc_ref in self.depositor.get_stream_by_sort_key(['merged', 'initial'], start_key):
            doc_dict = self.depositor.get_dict_from_ref(doc_ref)
            # Case 1 : End of the Scope
            if get_sort_key_from_dict(doc_dict) > end_key:
                return
            # Case 2 : Obsolete Document
            if doc_dict['merge_key'] < header_dict['start_seq']:
                continue
            # Case 3: Normal -> Dispatch Document
            tar_header = doc_dict
            tar_body_data = json.loads(encoder(tar_header.pop('data'), self.depositor.data_encode, 'flat'))
            self.dispatch_data(self.dispatcher ,tar_header, tar_body_data, tar_topic_id, tar_table_id)

    # Package Load : Asynochronous Parallel Processing
    def _package_load(self, header_dict, load_config, tar_topic_id, tar_table_id):
        start_doc_ref, end_doc_ref = None, None
        start_key, end_key = load_config['start_key'], load_config['end_key']
        # Preparation: Ajust the start_key / end_key
        for start_doc_ref in self.depositor.get_stream_by_sort_key(['packaged'], start_key):
            break
        if not start_doc_ref:
            return
        start_key = get_sort_key_from_dict(self.depositor.get_dict_from_ref(start_doc_ref))
        if start_key > end_key:
            return
        elif start_key != end_key:
            for end_doc_ref in self.depositor.get_stream_by_sort_key(['packaged'], end_key, True):
                break
            if not end_doc_ref:
                return
            end_key = get_sort_key_from_dict(self.depositor.get_dict_from_ref(end_doc_ref))
        if start_key > end_key:
            return
        mid_key = str((int(start_key) + int(end_key)) // 2)
        # Step 1: If start == end, do the job
        if start_key == end_key:
            for doc_ref in self.depositor.get_stream_by_sort_key(['packaged'], mid_key):
                tar_header = self.depositor.get_dict_from_ref(doc_ref)
                # Obsolete Document
                if tar_header['merge_key'] < header_dict['start_seq']:
                    return
                # Dispatch Data
                self.dispatch_data(self.dispatcher, tar_header, None, tar_topic_id, tar_table_id)
                return
        # Step 2: Get the right start key
        for right_doc_ref in self.depositor.get_stream_by_sort_key(['packaged'], mid_key, False, 0, False):
            right_start_key = get_sort_key_from_dict(self.depositor.get_dict_from_ref(right_doc_ref))
            if right_start_key <= end_key:
                right_load_config = load_config.copy()
                right_load_config['start_key'] = right_start_key
                self.messager.trigger_load(right_load_config)
            break
        # Step 3: Get the left start key
        for left_doc_ref in self.depositor.get_stream_by_sort_key(['packaged'], mid_key, True, 0, True):
            left_end_key = get_sort_key_from_dict(self.depositor.get_dict_from_ref(left_doc_ref))
            if left_end_key >= start_key:
                left_load_config = load_config.copy()
                left_load_config['end_key'] = left_end_key
                self.messager.trigger_load(left_load_config)
            break

    def load(self, load_config: dict):
        src_topic_id, src_table_id = load_config['src_topic_id'], load_config['src_table_id']
        tar_topic_id, tar_table_id = load_config['tar_topic_id'], load_config['tar_table_id']
        # Dispatch Setting
        client_set = self.subscription_dict.get((src_topic_id, src_table_id), [])
        if load_config['client_id'] in client_set:
            self.dispatcher = self.client_dict.get(load_config['client_id'], None)
            self.dispatcher.set_merge_size(self.merge_size)
        if not self.dispatcher:
            logging.warning("{}-{}: Source Table / Topic is not subscribed".format(src_topic_id, src_table_id))
            return
        # Check if target exists
        target_list = [(i['topic_id'], i['table_id'])
                       for i in self.dispatcher.get_destinations(src_topic_id, src_table_id)]
        if (tar_topic_id, tar_table_id) not in target_list:
            logging.warning("{}-{}: Target Table / Topic is not subscribed".format(tar_topic_id, tar_table_id))
            return
        # Loader Setting
        self.depositor.set_current_topic_table(src_topic_id, src_table_id)
        self.archiver.set_current_topic_table(src_topic_id, src_table_id)
        header_ref = self.depositor.get_table_header()
        if not header_ref:
            logging.warning("{}-{}: No Table Header Found, Quit".format(src_topic_id, src_table_id))
            return
        header_dict = self.depositor.get_dict_from_ref(header_ref)
        load_type = load_config.get('load_type', 'unknown')
        # Initial Loading
        if load_type == 'initial':
            # Header Load
            self._header_load(header_dict, tar_topic_id, tar_table_id)
            # Get Start key or End key
            start_doc_ref, end_doc_ref, start_merge_ref = None, None, None
            for start_doc_ref in self.depositor.get_stream_by_sort_key(['packaged', 'merged', 'initial']):
                break
            for start_merge_ref in self.depositor.get_stream_by_sort_key(['merged', 'initial']):
                break
            for end_doc_ref in self.depositor.get_stream_by_sort_key(['packaged', 'merged', 'initial'], reverse=True):
                break
            if not start_doc_ref or not end_doc_ref:
                logging.warning("{}-{}: No Data to load".format(tar_topic_id, tar_table_id))
                return
            start_key = get_sort_key_from_dict(self.depositor.get_dict_from_ref(start_doc_ref))
            end_key = get_sort_key_from_dict(self.depositor.get_dict_from_ref(end_doc_ref))
            # Case 1: Only Package Load Process:
            if not start_merge_ref:
                package_load_config = load_config.copy()
                package_load_config.update({'load_type': 'package', 'start_key': start_key, 'end_key': end_key})
                self._package_load(header_dict, package_load_config, tar_topic_id, tar_table_id)
            # Case 2: Pacakge Load + Normal Load + Package Load
            else:
                start_merge_key = get_sort_key_from_dict(self.depositor.get_dict_from_ref(start_merge_ref))
                package_load_config = load_config.copy()
                package_load_config.update({'load_type': 'package', 'start_key': start_key, 'end_key': start_merge_key})
                self._package_load(header_dict, package_load_config, tar_topic_id, tar_table_id)
                self._normal_load(header_dict, tar_topic_id, tar_table_id, start_merge_key, end_key)
                package_load_config.update({'load_type': 'package', 'start_key': start_merge_key, 'end_key': end_key})
                self._package_load(header_dict, package_load_config, tar_topic_id, tar_table_id)
        elif load_type == 'header':
            self._header_load(header_dict, tar_topic_id, tar_table_id)
        elif load_type == 'normal':
            start_key, end_key = load_config['start_key'], load_config['end_key']
            self._normal_load(header_dict, tar_topic_id, tar_table_id, start_key, end_key)
        elif load_type == 'package':
            self._package_load(header_dict, load_config, tar_topic_id, tar_table_id)
        else:
            logging.warning("{}-{}: load type {} not supported".format(src_topic_id, src_table_id, load_type))
            return
