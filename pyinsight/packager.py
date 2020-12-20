import logging
import json
import gzip
import base64
from xialib.archiver import Archiver
from xialib.depositor import Depositor
from pyinsight.insight import Insight, backlog

__all__ = ['Packager']


class Packager(Insight):
    """Packaging data

    Move the data from depositor to archiver. Design to store huge amount of data on column usage

    Attributes:
        depoistor (:obj:`Depositor`): Data is retrieve from depositor
        archiver (:obj:`Archiver`): Data is saved to archiver
        package_size (:obj:`int`): Package size (raw size without compression), depends on memory limit.
            It is not a hard limit
    """
    package_size = 2 ** 26

    def __init__(self, depositor: Depositor, archiver: Archiver, **kwargs):
        super().__init__(depositor=depositor, archiver=archiver)
        self.logger = logging.getLogger("Insight.Packager")
        self.logger.level = self.log_level
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    @backlog
    def package_data(self, topic_id: str, table_id: str, **kwargs) -> bool:
        """ Public function

        This function will read the merged data from depositor and save then into archiver

        Args:
            topic_id (:obj:`str`): Topic id
            table_id (:obj:`str`): Table id

        Returns:
            :obj:`bool`: If the data should be pushed again

        Notes:
            This function is decorated by @backlog, which means all Exceptions will be sent to internal message topic:
            backlog
        """
        self.archiver.set_current_topic_table(topic_id, table_id)
        self.depositor.set_current_topic_table(topic_id, table_id)
        packaged_size, packaged_lines, min_age, min_start_time, del_list = 0, 0, '', '', list()
        start_age = 2
        for last_pkg_ref in self.depositor.get_stream_by_sort_key(['packaged'], reverse=True):
            last_pkg_dict = self.depositor.get_header_from_ref(last_pkg_ref)
            if 'age' in last_pkg_dict:
                start_age = int(last_pkg_dict.get('end_age', last_pkg_dict['age'])) + 1
            break

        for doc_ref in self.depositor.get_stream_by_sort_key(['merged', 'initial']):
            doc_dict = self.depositor.get_header_from_ref(doc_ref)
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
            record_data = self.depositor.get_data_from_header(doc_dict)
            packaged_size += doc_dict['data_size']
            packaged_lines += doc_dict['line_nb']
            self.archiver.add_data(record_data)
            self.logger.info("Adding to archive with min:{}{}".format(min_age, min_start_time), extra=self.log_context)
            if self.archiver.workspace_size >= self.package_size:
                self.archiver.set_merge_key(doc_dict['merge_key'])
                archive_path = self.archiver.archive_data()
                self.logger.info("Archiving {} bytes to {}".format(self.archiver.workspace_size, archive_path),
                                 extra=self.log_context)
                if min_age:
                    doc_dict['age'] = min_age
                    doc_dict['segment_start_age'] = self.depositor.DELETE
                elif min_start_time:
                    doc_dict['start_time'] = min_start_time
                    doc_dict['segment_start_time'] = self.depositor.DELETE
                else:
                    self.logger.warning("Archiving without age / time", extra=self.log_context)  # pragma: no cover
                catalog = {fn: self.archiver.describe_single_field(fn) for fn in self.archiver.get_field_list()}
                doc_dict['catalog'] = \
                    base64.b64encode(gzip.compress(json.dumps(catalog, ensure_ascii=False).encode())).decode()
                doc_dict['data_encode'] = self.archiver.data_encode
                doc_dict['data_format'] = self.archiver.data_format
                doc_dict['data_store'] = self.archiver.data_store
                doc_dict['merge_status'] = 'packaged'
                doc_dict['merged_level'] = self.depositor.DELETE
                doc_dict['merge_level'] = 8
                doc_dict['data'] = archive_path
                doc_dict['data_size'] = packaged_size
                doc_dict['line_nb'] = packaged_lines
                self.logger.info("Updating packaged document {}".format(doc_dict['merge_key']), extra=self.log_context)
                self.depositor.update_document(doc_ref, doc_dict)
                self.archiver.remove_data()
                self.logger.info("Deleting {} merged documents".format(len(del_list)), extra=self.log_context)
                self.depositor.delete_documents(del_list)
                self.depositor.inc_table_header(packaged_size=packaged_size,
                                                packaged_lines=packaged_lines,
                                                merged_size=packaged_size*-1,
                                                merged_lines=packaged_lines*-1)
                packaged_size, packaged_lines, min_age, min_start_time, del_list = 0, 0, '', '', list()
            else:
                del_list.append(doc_ref)
        self.archiver.remove_data()
        return True
