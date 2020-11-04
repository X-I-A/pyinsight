import json
from pyinsight.action import Action, backlog
from pyinsight.utils.core import encoder

__all__ = ['Packager']


class Packager(Action):
    """
    Packaging Merged Documents (Messager, Depositor, Archiver and Dispatcher)
    """
    def _get_record_from_doc_dict(self, doc_dict):
        flat_data = encoder(doc_dict['data'], doc_dict['data_encode'], 'flat')
        return json.loads(flat_data), len(flat_data)

    @backlog
    def package_data(self, topic_id, table_id):
        package_size = self.package_size
        self.archiver.set_current_topic_table(topic_id, table_id)
        self.depositor.set_current_topic_table(topic_id, table_id)
        packaged_size, min_age, min_start_time, del_list = 0, '', '', list()
        start_age = 2
        for last_pkg_ref in self.depositor.get_stream_by_sort_key(['packaged'], reverse=True):
            last_pkg_dict = self.depositor.get_dict_from_ref(last_pkg_ref)
            if 'age' in last_pkg_dict:
                start_age = int(last_pkg_dict.get('end_age', last_pkg_dict['age'])) + 1
            break

        for doc_ref in self.depositor.get_stream_by_sort_key(['merged', 'initial']):
            doc_dict = self.depositor.get_dict_from_ref(doc_ref)
            self.log_context['context'] = '-'.join([self.depositor.topic_id, self.depositor.table_id,
                                                    doc_dict['merge_key']])
            if 'age' in doc_dict:
                if int(doc_dict['age']) != start_age:
                    self.logger.warning("Aged dataflow start by {} instead of {}".format(doc_dict['age'], start_age),
                                        extra=self.log_context)
                    break
                else:
                    start_age = int(doc_dict.get('end_age', doc_dict['age'])) + 1
            if doc_dict['merge_status'] == 'initial':
                self.logger.info("Reach a not merged yet document", extra=self.log_context)
                break
            if not min_age and 'age' in doc_dict:
                min_age = doc_dict['age']
            elif not min_start_time and 'deposit_at' in doc_dict:
                min_start_time = doc_dict.get('start_time', doc_dict['deposit_at'])
            record_data, record_data_size = self._get_record_from_doc_dict(doc_dict)
            packaged_size += record_data_size
            self.archiver.add_data(record_data)
            self.logger.info("Adding to archive with min:{}{}".format(min_age, min_start_time), extra=self.log_context)
            if self.archiver.workspace_size >= package_size:
                self.archiver.set_merge_key(doc_dict['merge_key'])
                archive_path = self.archiver.archive_data()
                self.logger.info("Archiving {} bytes to {}".format(self.archiver.workspace_size, archive_path),
                                 extra=self.log_context)
                if min_age:
                    doc_dict['min_age'] = min_age
                elif min_start_time:
                    doc_dict['min_start_time'] = min_start_time
                else:
                    self.logger.warning("Archiving without age / time", extra=self.log_context)
                doc_dict['data_encode'] = self.archiver.data_encode
                doc_dict['data_format'] = self.archiver.data_format
                doc_dict['data_store'] = 'file'
                doc_dict['merge_status'] = 'packaged'
                doc_dict['merged_list'] = del_list
                doc_dict['data'] = archive_path
                self.logger.info("Updating packaged document {}".format(doc_dict['merge_key']), extra=self.log_context)
                self.depositor.update_document(doc_ref, doc_dict)
                self.archiver.remove_data()
                self.logger.info("Deleting {} merged documents".format(len(del_list)), extra=self.log_context)
                self.depositor.delete_documents(del_list)
                self.depositor.inc_table_header(packaged_size=packaged_size)
                packaged_size, min_age, min_start_time, del_list = 0, '', '', list()
            else:
                del_list.append(doc_ref)
        self.archiver.remove_data()
        return True