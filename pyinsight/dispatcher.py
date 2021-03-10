import json
import gzip
import base64
import logging
import threading
import datetime
import zipfile
from functools import lru_cache, wraps
from typing import List, Dict, Tuple, Union
from xialib import backlog, Publisher, BasicFlower, SegmentFlower, BasicStorer
from pyinsight.insight import Insight

__all__ = ['Dispatcher']


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

class Dispatcher(Insight):
    """Receive pushed data, save to depositor and publish to different destinations

    Attributes:
        publisher (:obj:`dict` of :obj:`Publisher`): publisher lists
        subscription_list (:obj:`list` of :obj:`list`): Subscription Lists (
            source topic id,
                publisher key,
                target destination,
                target topic id,
                    source table id,
                        target table id,
                        field list,
                        filters list,
                        segment_config

    Notes:
        filter list must in the NDF form of list(list(list)))
    """
    def __init__(self, route_file: str, publisher: dict, **kwargs):
        super().__init__(publisher=publisher, **kwargs)
        self.logger = logging.getLogger("Insight.Dispatcher")
        self.logger.level = self.log_level
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

        # Unique storer support
        if "storer" not in kwargs:
            self.storer = BasicStorer()
        elif isinstance(self.storer, dict):
            self.storer = [v for k, v in self.publisher.items()][0]

        # Publisher Must be Dict
        if not isinstance(self.publisher, dict):
            self.logger.error("Publisher of Dispatch should be arranged as dictionary", extra=self.log_context)
            raise ValueError("Publisher of Dispatch should be arranged as dictionary")

        self.tar_config, self.default_routes = {}, {}

        self.route_file = route_file

    def set_tar_config(self, tar_config: List[dict]):
        """Target Configuration

        Attributes:
            tar_config (:obj:`list` of :obj:`dict`): Destination Configuration
                publisher_id: str
                destination: str
                tar_topic_id: str

        """
        self.tar_config = {}
        for config in tar_config:
            self.tar_config[config["topic"]] = [config["publisher_id"], config["destination"]]

    def set_topic_routes(self, routes: List[List[str]]):
        """Topic Level Routes

        Attributes:
            routes (:obj:`list` of :obj:`list`): topic_level routes
        """
        self.default_routes = {}
        for route in routes:
            if route["source"] in self.default_routes:
                self.default_routes[route["source"]].append(route["target"])
            else:
                self.default_routes[route["source"]] = [route["target"]]

    @timed_lru_cache()
    def get_routes(self, src_topic_id, src_table_id) -> list:
        """Get Route Table for specific topic and table

        """
        l2_path = src_topic_id + "/" + src_table_id
        for route_io in self.storer.get_io_stream(self.route_file):
            with zipfile.ZipFile(route_io) as route_zip:
                try:
                    with route_zip.open(l2_path) as route_fp:
                        route_list = []
                        for key, routes in json.load(route_fp).items():
                            route_list.extend(routes)
                            self.logger.info("Route {}-{} Loaded".format(src_topic_id, src_table_id),
                                             extra=self.log_context)
                            return route_list
                except Exception as e:  # pragma: no cover
                    self.logger.warning("Route {}-{} Load Error or Not found".format(src_topic_id, src_table_id),
                                        extra=self.log_context)
                    return []  # pragma: no cover
        self.logger.warning("Route {}-{} Not found".format(src_topic_id, src_table_id),
                            extra=self.log_context)  # pragma: no cover
        return []  # pragma: no cover

    def get_config_by_publisher(self, src_topic_id, src_table_id):
        table_routes = self.get_routes(src_topic_id, src_table_id)
        for default_tar_topic in self.default_routes.get(src_topic_id, []):
            if default_tar_topic not in [route["tar_topic_id"] for route in table_routes]:
                table_routes.append({"src_topic_id": src_topic_id, "src_table_id": src_table_id,
                                     "tar_topic_id": default_tar_topic, "tar_table_id": src_table_id})
        for route in table_routes:
            publisher_id, destination = self.tar_config.get(route["tar_topic_id"])
            route.update({"publisher_id": publisher_id, "destination": destination})
        publisher_list = set([route["publisher_id"] for route in table_routes])
        for publisher_id in publisher_list:
            dests = list()
            for route in table_routes:
                if route["publisher_id"] == publisher_id:
                    dests.append([route["destination"], route["tar_topic_id"], route["tar_table_id"],
                                  route["fields"] if route.get("fields", None) else None,
                                  route["filters"] if route.get("filters", None) else None,
                                  route["segment"] if route.get("segment", None) else None])
            yield {publisher_id: dests}

    def _dispatch_data(self, header: dict, full_data: List[dict], publisher: Publisher,
                      dest_list: List[Tuple[str, str, str, list, list]]):
        for destination in dest_list:
            tar_header = header.copy()
            tar_header['src_topic_id'] = tar_header.get('src_topic_id', tar_header['topic_id'])
            tar_header['src_table_id'] = tar_header.get('src_table_id', tar_header['table_id'])
            tar_header['topic_id'] = destination[1]
            tar_header['table_id'] = destination[2]
            basic_flower = BasicFlower(destination[3], destination[4])
            segment_flower = SegmentFlower(destination[5])
            tar_data = basic_flower.proceed(tar_header, full_data)[1]
            tar_header, tar_data = segment_flower.proceed(tar_header, tar_data)
            tar_header['data_encode'] = 'gzip'
            tar_header['data_store'] = 'body'
            self.logger.info("Dispatch to {}-{}-{}".format(destination[0],
                                                           destination[1],
                                                           destination[2]), extra=self.log_context)
            if int(tar_header.get('age', 0)) == 1:
                self.logger.info("Sending table creation event", extra=self.log_context)
                tar_header['event_type'] = 'target_table_update'
                self.trigger_cockpit(tar_header, tar_data)
            publisher.publish(destination[0], destination[1], tar_header,
                              gzip.compress(json.dumps(tar_data, ensure_ascii=False).encode()))


    @backlog
    def dispatch_data(self, header: dict, data: Union[List[dict], str, bytes], **kwargs) -> bool:
        """ Public function

        This function will get the pushed data and publish them to related subscribers

        Args:
            header (:obj:`str`): Document Header
            data (:obj:`list` of :obj:`dict`): Data in Python dictioany list format or file_store location str

        Returns:
            :obj:`bool`: If the data should be pushed again

        Notes:
            This function is decorated by @backlog, which means all Exceptions will be sent to internal message topic:
                backlog
        """
        src_topic_id = header['topic_id']
        src_table_id = header['table_id']
        self.log_context['context'] = '-'.join([src_topic_id, src_table_id])
        # Step 1: Data Preparation
        if isinstance(data, list):
            tar_full_data = data
        elif header['data_encode'] == 'blob':
            tar_full_data = json.loads(data.decode())
        elif header['data_encode'] == 'b64g':
            tar_full_data = json.loads(gzip.decompress(base64.b64decode(data)).decode())
        elif header['data_encode'] == 'gzip':
            tar_full_data = json.loads(gzip.decompress(data).decode())
        else:
            tar_full_data = json.loads(data)
        # Step 2: Multi-thread publish
        handlers = list()
        for config in self.get_config_by_publisher(src_topic_id, src_table_id):
            for publisher_id, dest_list in config.items():
                publisher = self.publisher.get(publisher_id)
                cur_handler = threading.Thread(target=self._dispatch_data,
                                               args=(header, tar_full_data, publisher, dest_list))
                cur_handler.start()
                handlers.append(cur_handler)
        # Step 3: Wait until all the dispatch thread are finished
        for handler in handlers:
            handler.join()
        return True
