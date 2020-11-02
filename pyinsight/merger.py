import os, json, logging
import pyinsight
from pyinsight.utils.exceptions import *
from pyinsight.action import *
from pyinsight.utils.validation import *
from pyinsight.utils.core import *

__all__ = ['Merger', 'AgeMerger', 'NormalMerger']

"""
# General Specification: Merge Single Messages
## Data Structure:
* Each document will have a unique merge key and thus a defined merge level
* Scope of a Level N node = All of the entries of Level N-1 before the last Level N document
* Once passed Level N : merged_level = N
* Different Document Merge Status:
  * initial = original status
  * merged = MERGE_SIZE reached. No merge operation allowed
## Core design principals
* No lock requirement => distribute system oriented
## Aged data

"""

class Merger(Action):
    def _merge_data(self, start_seq, merge_key, merge_level, merge_size):
        pass

    def _get_stream_by_merged_key(self, merge_key, min_merge_level):
        base_doc = self.depositor.get_ref_by_merge_key(merge_key)
        if not base_doc:
            logging.error("{}-{}: Can not get base doc by Merge Key".format(self.depositor.topic_id,
                                                                            self.depositor.table_id))
            return
        # Give the current one
        yield base_doc
        # Search the other ones
        base_doc_dict = self.depositor.get_dict_from_ref(base_doc)
        base_doc_key = get_sort_key_from_dict(base_doc_dict)
        for doc in self.depositor.get_stream_by_sort_key(le_ge_key=base_doc_key, reverse=True,
                                                         min_merge_level=min_merge_level, equal=False):
            yield doc

    def _get_record_from_doc_dict(self, doc_dict):
        return json.loads(encoder(doc_dict['data'], doc_dict['data_encode'], 'flat'))

    def _merge_and_delete_document(self, merge_level, merge_list, del_list):
        if merge_list:
            min_sort_key = min([i[2] for i in merge_list])
            max_sort_key = max([i[3] for i in merge_list])
            # Merge Body Items
            for item in merge_list:
                if item[3] != max_sort_key:
                    self.depositor.merge_documents(item[0], item[1], item[2], item[3], item[4])
            # Merge Leader Item
            for item in merge_list:
                if item[3] == max_sort_key: # Leader item
                    self.depositor.merge_documents(item[0], item[1], item[2], item[3], item[4],
                                                       min_sort_key, merge_level)
            # Merge List
            merge_ref_list = [i[0] for i in merge_list]
            to_del_list = [x for x in del_list if x not in merge_ref_list]
        else: # No merging => Delete Only
            to_del_list = del_list
        self.depositor.delete_documents(to_del_list)  # Only delete the no-merged items

    def merge_data(self, topic_id, table_id, merge_key, merge_level):
        self.depositor.set_current_topic_table(topic_id, table_id)
        header_ref = self.depositor.get_table_header()
        if not header_ref:
            return # No header found, do nothing
        header_dict = self.depositor.get_dict_from_ref(header_ref)
        if header_dict['aged']:
            self.__class__ = AgeMerger
        else:
            self.__class__ = NormalMerger
        self._merge_data(header_dict['start_seq'], merge_key, merge_level, self.merge_size)


class AgeMerger(Merger):
    def _merge_data(self, start_seq, merge_key, merge_level, merge_size=MERGE_SIZE):
        target_merge_level, del_list, merge_list = merge_level, list(), list()
        base_doc, start_age, end_age, data_list, total_size = None, 0, 0, list(), 0
        doc_dict = None
        for doc in self._get_stream_by_merged_key(merge_key, merge_level - 1):
            doc_dict = self.depositor.get_dict_from_ref(doc)
            # Start Point Condition
            if doc_dict.get('merge_key','') == merge_key:
                target_merge_level = doc_dict['merge_level']
                # Level - 1 Not merged
                if doc_dict.get('merged_level', 0) < merge_level - 1:
                    logging.warning('{}-{}: Lower level not ready yet'.format(self.depositor.topic_id,
                                                                              self.depositor.table_id))
                    return
                if doc_dict['merge_status'] == 'merged': # Header Merged
                    start_age, end_age = doc_dict['segment_start_age'], doc_dict.get('end_age', doc_dict['age'])
                    # Segment Related information should be updated
                    if doc_dict.get('merged_level', 0) == merge_level - 1:
                        base_doc = doc
                    else:
                        merge_list.append((doc, True, start_age, end_age, data_list))
                    continue
                else:
                    base_doc = doc
                    start_age, end_age = doc_dict['age'], doc_dict.get('end_age', doc_dict['age'])
                    doc_dict = self.depositor.get_dict_from_ref(doc)
                    data_list.extend(self._get_record_from_doc_dict(doc_dict))
                    total_size += len(doc_dict['data'])
                    continue
            # End of Scope Detection
            if doc_dict['start_seq'] < start_seq or doc_dict['age'] == 1 \
                or get_merge_level(doc_dict['merge_key']) >= merge_level \
                or doc_dict['merge_status'] == 'packaged':
                break # End of scope
            # Operation Start
            if doc_dict['merge_status'] != 'merged':
                del_list.append(doc)  # Delete Every Mot merged memeber Node by default
            # Case 1: Lower no-zero Level not merged yet
            if doc_dict['merge_level'] != 0 and doc_dict.get('merged_level', 0) < merge_level - 1:
                if doc_dict.get('end_age', doc_dict['age']) == start_age - 1: # No GAP Merge and continue
                    self._merge_data(start_seq, doc_dict['merge_key'], merge_level - 1, merge_size)
                    doc_dict = self.depositor.get_dict_from_ref(doc) # Get the data again
                elif doc_dict.get('end_age', doc_dict['age']) < start_age - 1: # GAP Detected, Quit
                    break
                else:
                    logging.warning('Overlapping / Obsolated at no-zero level nodes')
                    continue
            # Case 2: Lower level is ready
            if doc_dict['merge_level'] == 0 or doc_dict.get('merged_level', 0) >= merge_level - 1:
                # Age related operations
                if doc_dict.get('segment_start_age', doc_dict['age']) >= start_age:  # Obsolete Documents
                    continue
                elif doc_dict.get('end_age', doc_dict['age']) == start_age - 1:  # Standard
                    if not base_doc: # Start a new merging process
                        base_doc = doc
                        start_age = doc_dict.get('segment_start_age', doc_dict['age'])
                        end_age = doc_dict.get('end_age', doc_dict['age'])
                    if doc_dict['merge_status'] == 'merged':
                        pass # age, data will be initialized in merge operation
                    else:
                        start_age = doc_dict.get('segment_start_age', doc_dict['age'])
                        doc_dict = self.depositor.get_dict_from_ref(doc)
                        data_list.extend(self._get_record_from_doc_dict(doc_dict))
                        total_size += len(doc_dict['data'])
                elif doc_dict.get('end_age', doc_dict['age']) > start_age - 1:  # Overlap - Shouldn't happen
                    logging.warning('{}-{}:Overlapping at zero level nodes'.format(self.depositor.topic_id,
                                                                                   self.depositor.table_id))
                    continue
                else:  # GAP detected, Abandon
                    logging.warning('{}-{}: GAP Detected'.format(self.depositor.topic_id, self.depositor.table_id))
                    return
                # Merge opertaions :
                if doc_dict['merge_status'] == 'merged':  # Reached a merged document, merge current one
                    merge_list.append((base_doc, True, start_age, end_age, data_list))
                    if doc != base_doc: # Add current doc to merge list
                        start_age = doc_dict.get('segment_start_age', doc_dict['age'])
                        merge_list.append((doc, True, start_age,
                                           doc_dict.get('end_age', doc_dict['age']), []))
                    base_doc, end_age, data_list, total_size = None, start_age - 1, list(), 0
                elif total_size >= merge_size:  # Oversized, should merge and prepare a new merging process
                    merge_list.append((base_doc, True, start_age, end_age, data_list))
                    base_doc, end_age, data_list, total_size = None, start_age - 1, list(), 0
        # Final steps of the current merge
        if doc_dict and doc_dict.get('end_age', doc_dict['age']) == start_age - 1:  # Merge and safe delete
            if base_doc and (total_size >= merge_size or merge_list): # Too big or there is sth already merged
                merge_list.append((base_doc, True, start_age, end_age, data_list))
            elif base_doc and total_size < merge_size: # Merge the remain data
                merge_list.append((base_doc, False, start_age, end_age, data_list))
            # An isolated and clean all Merge and all Delete
            self._merge_and_delete_document(merge_level, merge_list, del_list)
        # Trigger the merge of next level
        if target_merge_level > merge_level:
            self.messager.trigger_merge(self.depositor.topic_id, self.depositor.table_id, merge_key, merge_level+1)


class NormalMerger(Merger):
    def _merge_data(self, start_seq, merge_key, merge_level, merge_size=MERGE_SIZE):
        target_merge_level, del_list, merge_list = merge_level, list(), list()
        base_doc, start_time, end_time, data_list, total_size = None, '', '', list(), 0
        doc_dict = None
        for doc in self._get_stream_by_merged_key(merge_key, merge_level - 1):
            doc_dict = self.depositor.get_dict_from_ref(doc)
            if doc_dict.get('merge_key','') == merge_key:
                target_merge_level = doc_dict['merge_level']
                # Level - 1 Not merged
                if doc_dict.get('merged_level', 0) < merge_level - 1:
                    logging.warning('{}-{}: Lower level not ready yet'.format(self.depositor.topic_id,
                                                                              self.depositor.table_id))
                    return
                if doc_dict['merge_status'] == 'merged': # Header Merged
                    start_time, end_time = doc_dict['segment_start_time'], doc_dict['deposit_at']
                    if doc_dict.get('merged_level', 0) == merge_level - 1:
                        base_doc = doc
                    else:
                        merge_list.append((doc, True, start_time, end_time, data_list))
                    continue
                else:
                    base_doc = doc
                    start_time, end_time = doc_dict.get('start_time', doc_dict['deposit_at']), doc_dict['deposit_at']
                    doc_dict = self.depositor.get_dict_from_ref(doc)
                    data_list.extend(self._get_record_from_doc_dict(doc_dict))
                    total_size += len(doc_dict['data'])
                    continue
            # End of Scope Detection
            if doc_dict['start_seq'] < start_seq or doc_dict.get('age', 0) == 1 \
                or get_merge_level(doc_dict['merge_key']) >= merge_level \
                or doc_dict['merge_status'] == 'packaged':
                break # End of scope
            # Operation Start
            if doc_dict['merge_status'] != 'merged':
                del_list.append(doc)  # Delete Every Mot merged memeber Node by default
            # Case 1: Lower no-zero Level not merged yet
            if doc_dict['merge_level'] != 0 and doc_dict.get('merged_level', 0) < merge_level - 1:
                self._merge_data(start_seq, doc_dict['merge_key'], merge_level - 1, merge_size)
                doc_dict = self.depositor.get_dict_from_ref(doc) # Get the data again
            # Case 2: Lower level is ready
            if doc_dict['merge_level'] == 0 or doc_dict.get('merged_level', 0) >= merge_level - 1:
                if not base_doc:  # Start a new merging process
                    base_doc = doc
                    start_time = doc_dict.get('segment_start_time', doc_dict['deposit_at'])
                    end_time = doc_dict['deposit_at']
                if doc_dict['merge_status'] == 'merged':
                    pass  # age, data will be initialized in merge operation
                else:
                    start_time = doc_dict.get('segment_start_time', doc_dict['deposit_at'])
                    doc_dict = self.depositor.get_dict_from_ref(doc)
                    data_list.extend(self._get_record_from_doc_dict(doc_dict))
                    total_size += len(doc_dict['data'])
                # Merge opertaions :
                if doc_dict['merge_status'] == 'merged':  # Reached a merged document, merge current one
                    merge_list.append((base_doc, True, start_time, end_time, data_list))
                    if doc != base_doc: # Add current doc to merge list
                        merge_list.append((doc, True, doc_dict.get('segment_start_time', doc_dict['deposit_at']),
                                           doc_dict['deposit_at'], []))
                    base_doc, start_time, end_time, data_list, total_size = None, '', '', list(), 0
                elif total_size >= merge_size:  # Oversized, should merge and prepare a new merging process
                    merge_list.append((base_doc, True, start_time, end_time, data_list))
                    base_doc, start_time, end_time, data_list, total_size = None, '', '', list(), 0
        # Final steps of the current merge
        if base_doc and (total_size >= merge_size or merge_list): # Too big or there is sth already merged
            merge_list.append((base_doc, True, start_time, end_time, data_list))
        elif base_doc and total_size < merge_size: # Merge the remain data
            merge_list.append((base_doc, False, start_time, end_time, data_list))
        # An isolated and clean all Merge and all Delete
        self._merge_and_delete_document(merge_level, merge_list, del_list)
        # Trigger the merge of next level
        if target_merge_level > merge_level:
            self.messager.trigger_merge(self.depositor.topic_id, self.depositor.table_id, merge_key, merge_level+1)
