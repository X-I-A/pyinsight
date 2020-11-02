import os
import json
import pyinsight
from pyinsight.utils.exceptions import *
from pyinsight.action import Action
from pyinsight.utils.validation import *
from pyinsight.utils.core import *

__all__ = ['Packager']

"""Packaging Merged Documents (Messager, Depositor, Archiver and Dispatcher)"""
class Packager(Action):
    def _get_record_from_doc_dict(self, doc_dict):
        return json.loads(encoder(doc_dict['data'], doc_dict['data_encode'], 'flat'))

    def package_data(self, topic_id, table_id):
        package_size = self.package_size
        self.archiver.set_current_topic_table(topic_id, table_id)
        self.depositor.set_current_topic_table(topic_id, table_id)
        min_age, min_start_time, del_list, merge_list = 0, '', list(), list()
        start_age = 2
        for doc_ref in self.depositor.get_stream_by_sort_key(['merged', 'initial']):
            doc_dict = self.depositor.get_dict_from_ref(doc_ref)
            if 'age' in doc_dict:
                if int(doc_dict['age']) != start_age:
                    break
                else:
                    start_age = int(doc_dict.get('end_age', doc_dict['age'])) + 1
            if doc_dict['merge_status'] == 'initial':
                break
            if not min_age and 'age' in doc_dict:
                min_age = doc_dict['age']
            elif not min_start_time and 'start_time' in doc_dict:
                min_start_time = doc_dict['start_time']
            self.archiver.add_data(self._get_record_from_doc_dict(doc_dict))
            if self.archiver.workspace_size >= package_size:
                self.archiver.set_merge_key(doc_dict['merge_key'])
                archive_path = self.archiver.archive_data()
                if min_age:
                    doc_dict['min_age'] = min_age
                elif min_start_time:
                    doc_dict['min_start_time'] = min_start_time
                else:
                    logging.warning("{}-{}: Min age or Min Start time not found".format(topic_id, table_id))
                doc_dict['data_encode'] = self.archiver.data_encode
                doc_dict['data_format'] = self.archiver.data_format
                doc_dict['data_store'] = 'file'
                doc_dict['merge_status'] = 'packaged'
                doc_dict['merged_list'] = del_list
                doc_dict['data'] = archive_path
                self.depositor.update_document(doc_ref, doc_dict)
                self.archiver.remove_data()
                self.depositor.delete_documents(del_list)
                del_list = list()
            else:
                del_list.append(doc_ref)
