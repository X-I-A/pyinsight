import json
from pyinsight.action import Action, backlog
from pyinsight.utils.core import encoder, get_merge_level, get_sort_key_from_dict, MERGE_SIZE

__all__ = ['Merger', 'AgeMerger', 'NormalMerger']


class Merger(Action):
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
    def _merge_data(self, start_seq, merge_key, merge_level, merge_size):
        pass

    def _get_stream_by_merged_key(self, merge_key, min_merge_level):
        base_doc = self.depositor.get_ref_by_merge_key(merge_key)
        if not base_doc:
            self.logger.error("Can not get base doc by Merge Key", extra=self.log_context)
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
        merged_size = 0
        if merge_list:
            min_sort_key = min([i[2] for i in merge_list])
            max_sort_key = max([i[3] for i in merge_list])
            # Merge Body Items
            for item in merge_list:
                if item[3] != max_sort_key:
                    merged_size += self.depositor.merge_documents(item[0], item[1], item[2], item[3], item[4])
            # Merge Leader Item
            for item in merge_list:
                if item[3] == max_sort_key: # Leader item
                    merged_size += self.depositor.merge_documents(item[0], item[1], item[2], item[3], item[4],
                                                                  min_sort_key, merge_level)
            # Merge List
            merge_ref_list = [i[0] for i in merge_list]
            to_del_list = [x for x in del_list if x not in merge_ref_list]
        else: # No merging => Delete Only
            to_del_list = del_list
        self.depositor.delete_documents(to_del_list)
        # Update header counter
        if merged_size > 0:
            new_header_dict = self.depositor.inc_table_header(merged_size=merged_size)
        # Trigger Packager
            if new_header_dict['merged_size'] - new_header_dict.get('packaged_size', 0) > self.package_size * 1.2:
                self.messager.trigger_package(self.depositor.topic_id, self.depositor.table_id)

    @backlog
    def merge_data(self, topic_id, table_id, merge_key, merge_level):
        self.depositor.set_current_topic_table(topic_id, table_id)
        self.log_context['context'] = topic_id + '-' + table_id
        header_ref = self.depositor.get_table_header()
        if not header_ref:
            return False
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
            self.log_context['context'] = self.depositor.topic_id + '-' + self.depositor.table_id + '-' + \
                doc_dict.get('merge_key', doc_dict['start_seq']) + '(' + str(merge_level) + ')'
            # Start Point Condition
            if doc_dict.get('merge_key','') == merge_key:
                target_merge_level = doc_dict['merge_level']
                # Current Level Already Merged:
                if doc_dict.get('merged_level', 0) >= merge_level:
                    self.logger.info("Merge Level {} already Merged".format(merge_level), extra=self.log_context)
                    return True
                # Level - 1 Not merged
                if doc_dict.get('merged_level', 0) < merge_level - 1:
                    self.logger.warning('Lower level {} not ready yet'.format(merge_level - 1), extra=self.log_context)
                    return False
                if doc_dict['merge_status'] == 'merged': # Header Merged
                    start_age, end_age = doc_dict['segment_start_age'], doc_dict.get('end_age', doc_dict['age'])
                    self.logger.info("Leader already merged {}-{}".format(start_age, end_age), extra=self.log_context)
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
                    self.logger.info("Lead node {}-{}".format(start_age, end_age), extra=self.log_context)
                    if total_size >= merge_size:
                        merge_list.append((base_doc, True, start_age, end_age, data_list))
                        base_doc, end_age, data_list, total_size = None, start_age - 1, list(), 0
                    continue
            # End of Scope Detection
            if doc_dict['start_seq'] < start_seq or doc_dict['age'] == 1 \
                or get_merge_level(doc_dict['merge_key']) >= merge_level \
                or doc_dict['merge_status'] == 'packaged':
                self.logger.info("End of scope", extra=self.log_context)
                break
            # Operation Start
            if doc_dict['merge_status'] != 'merged':
                del_list.append(doc)  # Delete Every Mot merged memeber Node by default
            # Case 1: Lower no-zero Level not merged yet
            if doc_dict['merge_level'] != 0 and doc_dict.get('merged_level', 0) < merge_level - 1:
                if doc_dict.get('end_age', doc_dict['age']) == start_age - 1: # No GAP Merge and continue
                    self.logger.info("Merging lower-level node", extra=self.log_context)
                    self._merge_data(start_seq, doc_dict['merge_key'], merge_level - 1, merge_size)
                    doc_dict = self.depositor.get_dict_from_ref(doc) # Get the data again
                elif doc_dict.get('end_age', doc_dict['age']) < start_age - 1: # GAP Detected, Quit
                    break
                else:
                    self.logger.warning("Overlapped/Obsolete lower-level node", extra=self.log_context)
                    continue
            # Case 2: Lower level is ready
            if doc_dict['merge_level'] == 0 or doc_dict.get('merged_level', 0) >= merge_level - 1:
                # Age related operations
                if doc_dict.get('segment_start_age', doc_dict['age']) >= start_age:  # Obsolete Documents
                    self.logger.warning("Obsolete document", extra=self.log_context)
                    continue
                elif doc_dict.get('end_age', doc_dict['age']) == start_age - 1:  # Standard
                    if not base_doc: # Start a new merging process
                        base_doc = doc
                        start_age = doc_dict.get('segment_start_age', doc_dict['age'])
                        end_age = doc_dict.get('end_age', doc_dict['age'])
                        self.logger.info("First node {}-{}".format(start_age, end_age), extra=self.log_context)
                    if doc_dict['merge_status'] == 'merged':
                        self.logger.info("Already merged {}-{}".format(doc_dict.get('segment_start_age',
                                                                                    doc_dict['age']),
                                                                       doc_dict.get('end_age', doc_dict['age'])),
                                         extra=self.log_context)
                        pass # age, data will be initialized in merge operation
                    else:
                        doc_dict = self.depositor.get_dict_from_ref(doc)
                        start_age = doc_dict.get('segment_start_age', doc_dict['age'])
                        data_list.extend(self._get_record_from_doc_dict(doc_dict))
                        total_size += len(doc_dict['data'])
                        self.logger.info("Adding node {}-{}".format(start_age, doc_dict.get('end_age', start_age)),
                                         extra=self.log_context)
                elif doc_dict.get('end_age', doc_dict['age']) > start_age - 1:  # Overlap - Shouldn't happen
                    self.logger.warning("Overlapped", extra=self.log_context)
                    continue
                else:  # GAP detected, Abandon
                    self.logger.warning("GAP Detected", extra=self.log_context)
                    return
                # Merge opertaions :
                if doc_dict['merge_status'] == 'merged':  # Reached a merged document, merge current one
                    merge_list.append((base_doc, True, start_age, end_age, data_list))
                    if doc != base_doc: # Add current doc to merge list
                        start_age = doc_dict.get('segment_start_age', doc_dict['age'])
                        merge_list.append((doc, True, start_age, doc_dict.get('end_age', doc_dict['age']), []))
                    base_doc, end_age, data_list, total_size = None, start_age - 1, list(), 0
                elif total_size >= merge_size:  # Oversized, should merge and prepare a new merging process
                    merge_list.append((base_doc, True, start_age, end_age, data_list))
                    base_doc, end_age, data_list, total_size = None, start_age - 1, list(), 0
        # Final steps of the current merge
        if doc_dict and doc_dict.get('end_age', doc_dict['age']) == start_age - 1:  # Merge and safe delete
            # Too big or max_level reached or there is sth already merged
            if base_doc and (total_size >= merge_size or merge_level==8 or merge_list):
                merge_list.append((base_doc, True, start_age, end_age, data_list))
            elif base_doc and total_size < merge_size: # Merge the remain data
                merge_list.append((base_doc, False, start_age, end_age, data_list))
            # An isolated and clean all Merge and all Delete
            for merge_item in merge_list:
                self.logger.info("Merging {}-{}: {} records".format(merge_item[2], merge_item[3], len(merge_item[4])),
                                 extra=self.log_context)
            self._merge_and_delete_document(merge_level, merge_list, del_list)
        # Trigger the merge of next level
        if target_merge_level > merge_level:
            self.logger.info("Trigger merger of higher level", extra=self.log_context)
            self.messager.trigger_merge(self.depositor.topic_id, self.depositor.table_id, merge_key, merge_level + 1)
        return True

class NormalMerger(Merger):
    def _merge_data(self, start_seq, merge_key, merge_level, merge_size=MERGE_SIZE):
        target_merge_level, del_list, merge_list = merge_level, list(), list()
        base_doc, start_time, end_time, data_list, total_size = None, '', '', list(), 0
        doc_dict = None
        for doc in self._get_stream_by_merged_key(merge_key, merge_level - 1):
            doc_dict = self.depositor.get_dict_from_ref(doc)
            self.log_context['context'] = self.depositor.topic_id + '-' + self.depositor.table_id + '-' + \
                doc_dict.get('merge_key', doc_dict['start_seq']) + '(' + str(merge_level) + ')'
            if doc_dict.get('merge_key','') == merge_key:
                target_merge_level = doc_dict['merge_level']
                # Current Level Already Merged:
                if doc_dict.get('merged_level', 0) >= merge_level:
                    self.logger.info("Merge Level {} already Merged".format(merge_level), extra=self.log_context)
                    return False
                # Level - 1 Not merged
                if doc_dict.get('merged_level', 0) < merge_level - 1:
                    self.logger.warning('Lower level {} not ready yet'.format(merge_level - 1), extra=self.log_context)
                    return False
                if doc_dict['merge_status'] == 'merged': # Header Merged
                    start_time, end_time = doc_dict['segment_start_time'], doc_dict['deposit_at']
                    self.logger.info("Leader already merged {}-{}".format(start_time, end_time), extra=self.log_context)
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
                    self.logger.info("Lead node {}-{}".format(start_time, end_time), extra=self.log_context)
                    if total_size >= merge_size:
                        merge_list.append((base_doc, True, start_time, end_time, data_list))
                        base_doc, start_time, end_time, data_list, total_size = None, '', '', list(), 0
                    continue
            # End of Scope Detection
            if doc_dict['start_seq'] < start_seq or doc_dict.get('age', 0) == 1 \
                or get_merge_level(doc_dict['merge_key']) >= merge_level \
                or doc_dict['merge_status'] == 'packaged':
                self.logger.info("End of scope", extra=self.log_context)
                break # End of scope
            # Operation Start
            if doc_dict['merge_status'] != 'merged':
                del_list.append(doc)  # Delete Every Mot merged memeber Node by default
            # Case 1: Lower no-zero Level not merged yet
            if doc_dict['merge_level'] != 0 and doc_dict.get('merged_level', 0) < merge_level - 1:
                self.logger.info("Merging lower-level node", extra=self.log_context)
                self._merge_data(start_seq, doc_dict['merge_key'], merge_level - 1, merge_size)
                doc_dict = self.depositor.get_dict_from_ref(doc) # Get the data again
            # Case 2: Lower level is ready
            if doc_dict['merge_level'] == 0 or doc_dict.get('merged_level', 0) >= merge_level - 1:
                # Time related operations
                if not base_doc:  # Start a new merging process
                    base_doc = doc
                    start_time = doc_dict.get('segment_start_time', doc_dict['deposit_at'])
                    end_time = doc_dict['deposit_at']
                    self.logger.info("First node {}-{}".format(start_time, end_time), extra=self.log_context)
                if doc_dict['merge_status'] == 'merged':
                    self.logger.info("Already Merged {}-{}".format(doc_dict.get('segment_start_time',
                                                                                doc_dict['deposit_at']),
                                                                   doc_dict['deposit_at']),
                                     extra=self.log_context)
                    pass  # deposit time, data will be initialized in merge operation
                else:
                    start_time = doc_dict.get('segment_start_time', doc_dict['deposit_at'])
                    doc_dict = self.depositor.get_dict_from_ref(doc)
                    data_list.extend(self._get_record_from_doc_dict(doc_dict))
                    total_size += len(doc_dict['data'])
                    self.logger.info("Adding node {}-{}".format(start_time, doc_dict['deposit_at']),
                                     extra=self.log_context)
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
        # Too big or max_level reached or there is sth already merged
        if base_doc and (total_size >= merge_size or merge_level==8 or merge_list):
            merge_list.append((base_doc, True, start_time, end_time, data_list))
        elif base_doc and total_size < merge_size: # Merge the remain data
            merge_list.append((base_doc, False, start_time, end_time, data_list))
        # An isolated and clean all Merge and all Delete
        for merge_item in merge_list:
            self.logger.info("Merging {}-{}: {} records".format(merge_item[2], merge_item[3], len(merge_item[4])),
                             extra=self.log_context)
        self._merge_and_delete_document(merge_level, merge_list, del_list)
        # Trigger the merge of next level
        if target_merge_level > merge_level:
            self.messager.trigger_merge(self.depositor.topic_id, self.depositor.table_id, merge_key, merge_level+1)
        return True