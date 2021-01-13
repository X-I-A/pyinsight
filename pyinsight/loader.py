import json
import logging
import base64
import gzip
import hashlib
from typing import Union, List, Dict, Any
from xialib.archiver import Archiver
from xialib.depositor import Depositor
from xialib.publisher import Publisher
from xialib.storer import Storer
from pyinsight.insight import Insight, backlog, get_fields_from_filter


__all__ = ['Loader']

class Loader(Insight):
    """Load Full table data to a destination

    Design to take only useful message to Agent Module

    Attributes:
        storers (:obj:`list` of :obj:`Storer`): Read the data which is not in a message body
        storer_dict (:obj:`list`): data_store Type and its related Storer
        depoistor (:obj:`Depositor`): Data is retrieve from depositor
        archiver (:obj:`Archiver`): Data is saved to archiver
        publishers (:obj:`dict` of :obj:`Publisher`): publisher id and its related publisher object
    """
    def __init__(self, depositor: Depositor, publisher: Dict[str, Publisher], **kwargs):
        super().__init__(depositor=depositor, publisher=publisher, **kwargs)
        self.logger = logging.getLogger("Insight.Loader")
        self.logger.level = self.log_level
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        self.active_storer = None
        self.active_publisher = None

    # Head Load: Simple Sent
    def _header_load(self, header_dict, destination, tar_topic_id, tar_table_id, fields) -> bool:
        tar_header = header_dict.copy()
        tar_body_data = self.depositor.get_data_from_header(tar_header)
        tar_body_data = [field for field in tar_body_data if field['field_name'] in fields or field['key_flag']]
        tar_header['source_id'] = tar_header.get('source_id', tar_header['table_id'])
        tar_header['topic_id'] = tar_topic_id
        tar_header['table_id'] = tar_table_id
        tar_header['data_encode'] = 'gzip'
        tar_header['data_store'] = 'body'
        tar_header.pop('data', None)
        self.logger.info("Header to be loaded", extra=self.log_context)
        self.active_publisher.publish(destination, tar_topic_id, tar_header,
                                      gzip.compress(json.dumps(tar_body_data, ensure_ascii=False).encode()))
        self.logger.info("Sending table creation event", extra=self.log_context)
        tar_header['event_type'] = 'target_table_update'
        self.trigger_cockpit(tar_header, tar_body_data)
        return True

    # Normal Load : Send One by One without duplications
    def _normal_load(self, header_dict, destination, tar_topic_id, tar_table_id, start_key, end_key, fields, filters):
        load_history = dict()
        for doc_ref in self.depositor.get_stream_by_sort_key(['merged', 'initial'], start_key):
            doc_dict = self.depositor.get_header_from_ref(doc_ref)
            # Case 1 : End of the Scope
            if doc_dict['sort_key'] > end_key:
                return True  # pragma: no cover
            # Case 2 : Obsolete Document
            if doc_dict['merge_key'] < header_dict['start_seq']:
                continue  # pragma: no cover
            # Case 3: Normal -> Dispatch Document
            tar_header = doc_dict.copy()
            tar_body_data = self.depositor.get_data_from_header(tar_header)
            tar_body_data = [line for line in tar_body_data
                if (line.get('_AGE', None), line.get('_SEQ', None), line.get('_NO', None)) not in load_history]
            load_history.update({(line.get('_AGE', None), line.get('_SEQ', None), line.get('_NO', None)): None
                                 for line in tar_body_data})
            tar_body_data = self.filter_table(tar_body_data, fields, filters)
            tar_header['source_id'] = tar_header.get('source_id', tar_header['table_id'])
            tar_header['topic_id'] = tar_topic_id
            tar_header['table_id'] = tar_table_id
            tar_header['data_encode'] = 'gzip'
            tar_header['data_store'] = 'body'
            tar_header.pop('data', None)
            self.logger.info("Doc {} load {} lines".format(doc_dict['merge_key'], len(tar_body_data)),
                             extra=self.log_context)
            self.active_publisher.publish(destination, tar_topic_id, tar_header,
                                          gzip.compress(json.dumps(tar_body_data, ensure_ascii=False).encode()))
        return True

    # Package Load : Asynochronous Parallel Processing
    def _package_load(self, header_dict, load_config: Dict[str, Any]) -> bool:
        start_doc_ref, end_doc_ref = None, None
        destination = load_config['destination']
        tar_topic_id, tar_table_id = load_config['tar_topic_id'], load_config['tar_table_id']
        start_key, end_key = load_config['start_key'], load_config['end_key']
        fields, filters = load_config['fields'], load_config['filters']
        # Preparation: Ajust the start_key / end_key
        for start_doc_ref in self.depositor.get_stream_by_sort_key(['packaged'], start_key):
            break
        if not start_doc_ref:
            return True
        start_key = self.depositor.get_header_from_ref(start_doc_ref).get('sort_key')
        if start_key > end_key:
            return True  # pragma: no cover
        elif start_key != end_key:
            for end_doc_ref in self.depositor.get_stream_by_sort_key(['packaged'], end_key, True):
                break
            if not end_doc_ref:
                return True  # pragma: no cover
            end_key = self.depositor.get_header_from_ref(end_doc_ref).get('sort_key')
        if start_key > end_key:
            return True  # pragma: no cover
        mid_key = str((int(start_key) + int(end_key)) // 2)
        # Step 1: If start == end, do the job
        if start_key == end_key:
            for doc_ref in self.depositor.get_stream_by_sort_key(['packaged'], mid_key):
                tar_header = self.depositor.get_header_from_ref(doc_ref)
                # Obsolete Document
                if tar_header['merge_key'] < header_dict['start_seq']:
                    return True  # pragma: no cover
                # Check if the package has the requested data
                skip_flag = False
                if 'catalog' in tar_header:
                    catalog = json.loads(gzip.decompress(base64.b64decode(tar_header['catalog'])))
                    if not self._check_has_needed_data(filters, catalog):
                        skip_flag = True

                if skip_flag:
                    tar_body_data = []
                else:
                    needed_fields = list( set(fields)
                                          | set(self.INSIGHT_FIELDS)
                                          | set([x[0] for l1 in filters for x in l1 if len(x)>0]))
                    self.archiver.load_archive(tar_header['merge_key'], needed_fields)
                    tar_body_data = self.archiver.get_data()
                    tar_body_data = self.filter_table(tar_body_data, fields, filters)
                if load_config.get('data_store', 'body') == 'body':
                    tar_header['source_id'] = tar_header.get('source_id', tar_header['table_id'])
                    tar_header['topic_id'] = tar_topic_id
                    tar_header['table_id'] = tar_table_id
                    tar_header['data_encode'] = 'gzip'
                    tar_header['data_store'] = 'body'
                    tar_header['data_format'] = 'record'
                    tar_header.pop('data', None)
                    self.logger.info("Doc {} load {} lines".format(tar_header['merge_key'], len(tar_body_data)),
                                     extra=self.log_context)
                    self.active_publisher.publish(destination, tar_topic_id, tar_header,
                                                  gzip.compress(json.dumps(tar_body_data, ensure_ascii=False).encode()))
                    return True
                else:
                    location = load_config['store_path'] + \
                        hashlib.md5(tar_header['merge_key'].encode()).hexdigest()[:4] + '-' + \
                        str(int(tar_header['start_seq']) + int(tar_header.get('age', 0))) + '.gz'
                    self.active_storer.write(gzip.compress(json.dumps(tar_body_data, ensure_ascii=False).encode()),
                                             location)
                    tar_header['source_id'] = tar_header.get('source_id', tar_header['table_id'])
                    tar_header['topic_id'] = tar_topic_id
                    tar_header['table_id'] = tar_table_id
                    tar_header['data_encode'] = 'gzip'
                    tar_header['data_store'] = load_config['data_store']
                    tar_header['data_format'] = 'record'
                    tar_header.pop('data', None)
                    self.active_publisher.publish(destination, tar_topic_id, tar_header, location)
                    return True
        # Step 2: Get the right start key
        for right_doc_ref in self.depositor.get_stream_by_sort_key(['packaged'], mid_key, False, 0, False):
            right_start_key = self.depositor.get_header_from_ref(right_doc_ref).get('sort_key')
            if right_start_key <= end_key:
                right_load_config = load_config.copy()
                right_load_config['start_key'] = right_start_key
                self.trigger_load(right_load_config)
            break
        # Step 3: Get the left start key
        for left_doc_ref in self.depositor.get_stream_by_sort_key(['packaged'], mid_key, True, 0, True):
            left_end_key = self.depositor.get_header_from_ref(left_doc_ref).get('sort_key')
            if left_end_key >= start_key:
                left_load_config = load_config.copy()
                left_load_config['end_key'] = left_end_key
                self.trigger_load(left_load_config)
            break
        return True

    def _get_single_dnf_field(self, ndf_filter: List[List[list]], field_name: str, info_type: str):
        new_dnf_filter = list()
        for or_filter in ndf_filter:
            new_or_filter = list()
            for and_filter in [item for item in or_filter if item[0] == field_name]:
                if info_type == 'full':
                    new_or_filter.append(and_filter)
                elif info_type == 'number' and and_filter[1] in ['=', '<', '<=', '>', '>=']:
                    and_filter[2] = self.archiver.func_map[info_type](and_filter[2])
                    if and_filter[1] in ['<', '<=']:
                        and_filter[2] += 1
                    elif and_filter[1] in ['>', '>=']:
                        and_filter[2] -= 1
                    new_or_filter.append(and_filter)
                elif info_type.startswith('c_') and and_filter[1] == '=':
                    and_filter[2] = self.archiver.func_map[info_type](and_filter[2])
                    new_or_filter.append(and_filter)
            if not new_or_filter:
                return [[]]
            else:
                new_dnf_filter.append(new_or_filter)
        return new_dnf_filter

    def _check_has_needed_data(self, ndf_filters: List[List[list]], catalog: dict) -> bool:
        field_list = [fn for fn in get_fields_from_filter(ndf_filters) if catalog.get(fn, {}).get('value', []) != []]
        for field in field_list:
            data_list = [{field: value} for value in catalog[field]['value']]
            new_filter = self._get_single_dnf_field(ndf_filters, field, catalog[field]['type'])
            data_list = self.filter_table_dnf(data_list, new_filter)
            if not data_list:
                return False
        return True

    @backlog
    def load(self, load_config: Union[dict, str], **kwargs):
        """ Load data of a source to a destination

        This function will load full / partial data of a source to a destination. Data format will be gzipped records.
        Args lists all elements of load_config

        Args:
            publisher_id (:obj:`str`): which publisher to publish message
            src_topic_id (:obj:`str`): Source topic id
            src_table_id (:obj:`str`): Source table id
            destination (:obj:`str`): Destination used by publisher
            tar_topic_id (:obj:`str`): Target topic id
            tar_table_id (:obj:`str`): Target table id
            fields (:obj:`list`): fields to be loaded
            filters (:obj:`list`): data filter
            data_store (:obj:`str`): data store type (use Storer instead of message body)
            store_path (:obj:`str`): save path prefix. Full filename will be
                store_path + 4 first digits of hash(merge_key) + '-' + merge_key + '.' + merge_status
            load_type (:obj:`str`): 'initial', 'header', 'normal', 'packaged'
            start_key (:obj:`str`): load start merge key
            end_key (:obj:`str`): load end merge key

        Notes:
            For the store_path, please including the os path seperator at the end
        """
        src_topic_id, src_table_id = load_config['src_topic_id'], load_config['src_table_id']
        destination = load_config['destination']
        tar_topic_id, tar_table_id = load_config['tar_topic_id'], load_config['tar_table_id']
        fields, filers = load_config['fields'], load_config['filters']
        self.log_context['context'] = src_topic_id + '-' + src_table_id + '|' + \
                                      tar_topic_id + '-' + tar_table_id
        # Step 1: Get the correct task taker
        self.active_publisher = self.publisher.get(load_config['publisher_id'])
        if load_config.get('data_store', 'body') != 'body':
            self.active_storer = self.storer_dict.get(load_config['data_store'], None)
            if not self.active_storer:
                self.logger.error("No storer for store type {}".format(load_config['data_store']),
                                  extra=self.log_context)
                raise ValueError("INS-000005")
        self.depositor.set_current_topic_table(src_topic_id, src_table_id)
        self.archiver.set_current_topic_table(src_topic_id, src_table_id)
        load_type = load_config.get('load_type', 'unknown')
        header_ref = self.depositor.get_table_header()
        if not header_ref:
            self.logger.warning("No Table Header Found", extra=self.log_context)
            return False
        header_dict = self.depositor.get_header_from_ref(header_ref)

        if load_type == 'initial':
            # self.logger.info("Header to be loaded", extra=self.log_context)
            # self._header_load(header_dict, destination, tar_topic_id, tar_table_id, fields)
            # Get Start key or End key
            start_doc_ref, end_doc_ref, start_merge_ref = None, None, None
            for start_doc_ref in self.depositor.get_stream_by_sort_key(['packaged', 'merged', 'initial']):
                break
            for start_merge_ref in self.depositor.get_stream_by_sort_key(['merged', 'initial']):
                break
            for end_doc_ref in self.depositor.get_stream_by_sort_key(['packaged', 'merged', 'initial'], reverse=True):
                break
            if not start_doc_ref or not end_doc_ref:
                self.logger.info("No data to load", extra=self.log_context)
                return False
            start_key = self.depositor.get_header_from_ref(start_doc_ref).get('sort_key')
            end_key = self.depositor.get_header_from_ref(end_doc_ref).get('sort_key')
            # Case 1: Only Package Load Process:
            if not start_merge_ref:
                package_load_config = load_config.copy()
                package_load_config.update({'load_type': 'package', 'start_key': start_key, 'end_key': end_key})
                self.logger.info("Package load range {}-{}".format(start_key, end_key), extra=self.log_context)
                self._package_load(header_dict, package_load_config)
            # Case 2: Pacakge Load + Normal Load + Package Load
            else:
                start_merge_key = self.depositor.get_header_from_ref(start_merge_ref).get('sort_key')
                package_load_config = load_config.copy()
                package_load_config.update({'load_type': 'package', 'start_key': start_key, 'end_key': start_merge_key})
                self.logger.info("Package load range {}-{}".format(start_key, start_merge_key), extra=self.log_context)
                self._package_load(header_dict, package_load_config)
                self.logger.info("Document load range {}-{}".format(start_merge_key, end_key), extra=self.log_context)
                self._normal_load(header_dict, destination, tar_topic_id, tar_table_id,
                                  start_merge_key, end_key, fields, filers)
                package_load_config.update({'load_type': 'package', 'start_key': start_merge_key, 'end_key': end_key})
                self.logger.info("Package load range {}-{}".format(start_merge_key, end_key), extra=self.log_context)
                self._package_load(header_dict, package_load_config)
            return True
        elif load_type == 'header':
            return self._header_load(header_dict, destination, tar_topic_id, tar_table_id, fields)
        elif load_type == 'normal':
            start_key, end_key = load_config['start_key'], load_config['end_key']
            return self._normal_load(header_dict, destination, tar_topic_id, tar_table_id,
                                     start_key, end_key, fields, filers)
        elif load_type == 'package':
            start_key, end_key = load_config['start_key'], load_config['end_key']
            self.logger.info("Package load range {}-{}".format(start_key, end_key), extra=self.log_context)
            return self._package_load(header_dict, load_config)
        else:
            self.logger.error("load type {} not supported".format(load_type), extra=self.log_context)
            return True
