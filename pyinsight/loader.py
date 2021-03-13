import json
import logging
import base64
import gzip
import zipfile
import datetime
from functools import lru_cache, wraps
from typing import Union, List, Dict, Any
from xialib import backlog, BasicFlower, SegmentFlower, BasicStorer, IOStorer
from pyinsight.insight import Insight

__all__ = ['Loader']


def timed_lru_cache(maxsize: int = 1024):
    def wrapper_cache(func):
        func = lru_cache(maxsize=maxsize)(func)
        func.lifetime = datetime.timedelta(seconds=600)
        func.expiration = datetime.datetime.utcnow() + func.lifetime
        @wraps(func)
        def wrapped_func(*args, **kwargs):
            if datetime.datetime.utcnow() >= func.expiration:
                # Test Covered by PubsubGcrPublisher (xialib-pubsub)
                func.cache_clear()  # pragma: no cover
                func.expiration = datetime.datetime.utcnow() + func.lifetime  # pragma: no cover
            return func(*args, **kwargs)
        return wrapped_func
    return wrapper_cache


@timed_lru_cache()
def get_routes(route_file: str, storer: IOStorer, src_topic_id: str, src_table_id) -> list:
    """Get Route Table for specific topic and table

    """
    l2_path = src_topic_id + "/" + src_table_id
    for route_io in storer.get_io_stream(route_file):
        with zipfile.ZipFile(route_io) as route_zip:
            try:
                with route_zip.open(l2_path) as route_fp:
                    route_list = []
                    for key, routes in json.load(route_fp).items():
                        route_list.extend(routes)
                        logging.info("Route {}-{} Loaded".format(src_topic_id, src_table_id))
                        return route_list
            except Exception as e:  # pragma: no cover
                logging.warning("Route {}-{} Load Error or Not found".format(src_topic_id, src_table_id))
                return []  # pragma: no cover
    logging.warning("Route {}-{} Not found".format(src_topic_id, src_table_id))  # pragma: no cover
    return []  # pragma: no cover

class Loader(Insight):
    """Load Full table data to a destination

    Design to take only useful message to Agent Module

    Attributes:
        storer (:obj:`Storer`): Read the data of route file
        route_file (:obj:`str`): Location of route file
        depoistor (:obj:`Depositor`): Data is retrieve from depositor
        archiver (:obj:`Archiver`): Data is saved to archiver
        publishers (:obj:`dict` of :obj:`Publisher`): publisher id and its related publisher object
    """
    def __init__(self, route_file: str, depositor, archiver, publisher: dict, **kwargs):
        super().__init__(archiver=archiver, depositor=depositor, publisher=publisher, **kwargs)
        self.logger = logging.getLogger("Insight.Loader")
        self.logger.level = self.log_level
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        self.active_publisher = None

        # Unique storer support
        if "storer" not in kwargs:
            self.storer = BasicStorer()
        elif isinstance(self.storer, dict):
            self.storer = [v for k, v in self.publisher.items()][0]
        self.route_file = route_file

        # Publisher Must be Dict
        if not isinstance(self.publisher, dict):
            self.logger.error("Publisher of Dispatch should be arranged as dictionary", extra=self.log_context)
            raise ValueError("Publisher of Dispatch should be arranged as dictionary")

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
        field_list = [fn for fn in BasicFlower.get_fields_from_filter(ndf_filters)
                      if catalog.get(fn, {}).get('value', []) != []]
        for field in field_list:
            data_list = [{field: value} for value in catalog[field]['value']]
            new_filter = self._get_single_dnf_field(ndf_filters, field, catalog[field]['type'])
            data_list = BasicFlower.filter_table_dnf(data_list, new_filter)
            if not data_list:
                return False
        return True

    # Head Load:
    def _header_load(self, header_dict, destination, tar_topic_id, tar_table_id) -> bool:
        tar_header = header_dict.copy()
        tar_body_data = self.basic_flower.proceed(tar_header, self.depositor.get_data_from_header(tar_header))[1]
        tar_header, tar_body_data = self.segment_flower.proceed(tar_header, tar_body_data)
        tar_header['topic_id'] = tar_topic_id
        tar_header['table_id'] = tar_table_id
        tar_header['data_encode'] = 'gzip'
        tar_header['data_store'] = 'body'
        tar_header.pop('data', None)
        self.logger.info("Header to be loaded", extra=self.log_context)
        pub = self.active_publisher.publish(destination, tar_topic_id, tar_header,
                                            gzip.compress(json.dumps(tar_body_data, ensure_ascii=False).encode()))
        return True if pub else False

    # Document Load (merge status = "merged" or "initial"):
    def _document_load(self, header_dict, destination, tar_topic_id, tar_table_id) -> bool:
        tar_header = header_dict.copy()
        tar_body_data = self.basic_flower.proceed(tar_header, self.depositor.get_data_from_header(tar_header))[1]
        tar_header, tar_body_data = self.segment_flower.proceed(tar_header, tar_body_data)
        tar_header['topic_id'] = tar_topic_id
        tar_header['table_id'] = tar_table_id
        tar_header['data_encode'] = 'gzip'
        tar_header['data_store'] = 'body'
        tar_header.pop('data', None)
        self.logger.info("Depositor: {} load {} lines".format(tar_header['merge_key'], len(tar_body_data)),
                         extra=self.log_context)
        pub = self.active_publisher.publish(destination, tar_topic_id, tar_header,
                                            gzip.compress(json.dumps(tar_body_data, ensure_ascii=False).encode()))
        return True if pub else False

    # Archive Load (merge status = "packaged")
    def _archive_load(self, header_dict, destination, tar_topic_id, tar_table_id) -> bool:
        tar_header = header_dict.copy()
        # Check if the package has the requested data
        skip_flag = False
        if 'catalog' in tar_header:
            catalog = json.loads(gzip.decompress(base64.b64decode(tar_header['catalog'])))
            if not self._check_has_needed_data(self.filters, catalog):
                skip_flag = True
        if skip_flag:
            tar_body_data = []
        else:
            needed_fields = list(set(self.fields)
                                 | set(self.INSIGHT_FIELDS)
                                 | set([x[0] for l1 in self.filters for x in l1 if len(x) > 0]))
            self.archiver.load_archive(tar_header['merge_key'], needed_fields)
            tar_body_data = self.basic_flower.proceed(tar_header, self.archiver.get_data())[1]
            tar_header, tar_body_data = self.segment_flower.proceed(tar_header, tar_body_data)
        tar_header['topic_id'] = tar_topic_id
        tar_header['table_id'] = tar_table_id
        tar_header['data_encode'] = 'gzip'
        tar_header['data_store'] = 'body'
        tar_header['data_format'] = 'record'
        tar_header.pop('data', None)
        self.logger.info("Archiver: {} load {} lines".format(tar_header['merge_key'], len(tar_body_data)),
                         extra=self.log_context)
        pub = self.active_publisher.publish(destination, tar_topic_id, tar_header,
                                            gzip.compress(json.dumps(tar_body_data, ensure_ascii=False).encode()))
        return True if pub else False

    def load(self, src_topic_id: str, src_table_id: str, tar_topic_id: str, tar_table_id: str,
             publisher_id: str, destination: str, start_key: str, end_key: str) -> list:
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
            start_key (:obj:`str`): load start sort key
            end_key (:obj:`str`): load end sort key

        Notes:
            For the store_path, please including the os path seperator at the end
        """
        error_list = []
        self.log_context['context'] = src_topic_id + '-' + src_table_id + '|' + \
                                      tar_topic_id + '-' + tar_table_id
        l2_rts = get_routes(self.route_file, self.storer, src_topic_id, src_table_id)
        routes = [rt for rt in l2_rts if rt["tar_topic_id"] == tar_topic_id and rt["tar_table_id"] == tar_table_id]
        if not routes:
            self.logger.warning("No Routes Found", extra=self.log_context)
            return error_list
        active_route = routes[0]
        self.fields, self.filters = active_route.get('fields', None), active_route.get('filters', None)
        self.basic_flower = BasicFlower(self.fields, self.filters)
        self.segment_config = active_route.get('segment', None)
        self.segment_flower = SegmentFlower(self.segment_config)
        # Step 1: Get the correct task taker
        self.active_publisher = self.publisher.get(publisher_id)
        self.depositor.set_current_topic_table(src_topic_id, src_table_id)
        self.archiver.set_current_topic_table(src_topic_id, src_table_id)
        # Step 2: Iterator
        for doc_ref in self.depositor.get_stream_by_sort_key(le_ge_key=start_key):
            doc_dict = self.depositor.get_header_from_ref(doc_ref)
            # End of the Scope
            if doc_dict['sort_key'] > end_key:
                break  # pragma: no cover
            tar_header = doc_dict.copy()
            if tar_header["merge_level"] == 9:
                load_result = self._header_load(tar_header, destination, tar_topic_id, tar_table_id)
            elif tar_header["merge_level"] == 8:
                load_result = self._archive_load(tar_header, destination, tar_topic_id, tar_table_id)
            else:
                load_result = self._document_load(tar_header, destination, tar_topic_id, tar_table_id)
            if not load_result:
                error_list.append({"merge_key": doc_dict["merge_key"]})  # pragma: no cover
        return error_list
