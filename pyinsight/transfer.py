import logging
from pyinsight.action import Action
from pyinsight.dispatcher import Dispatcher
from pyinsight.messager import Messager
from pyinsight.archiver import Archiver
from pyinsight.utils.core import filter_table_column, get_fields_from_filter

__all__ = ['Transfer']

class Transfer(Action):
    def __init__(self, messager=None, depositor=None, archiver=None, translators=list()):
        super().__init__(messager=None, depositor=None, archiver=None, translators=list())
        self.client_dict = dict() # {client_id: dispatcher}
        self.subscription_dict = dict() # {tuple(topic_id, table_id) : set{client_id1, client_id2}}

    def upsert_client_config(self, client_id, subscription, messager=None, archiver=None):
        if messager and not isinstance(messager, Messager):
            logging.error("{} has a wrong Messager".format(client_id))
            return
        if archiver and not isinstance(archiver, Archiver):
            logging.error("{} has a wrong Archiver".format(client_id))
            return
        cur_dispatcher = self.client_dict.get(client_id, None)
        # Create a new dispatcher if needed
        if not cur_dispatcher or not messager:
            cur_dispatcher = Dispatcher(messager=messager, archiver=archiver)
            self.client_dict[client_id] = cur_dispatcher
        if messager and not archiver:
            cur_dispatcher.messager_only = True
        cur_dispatcher.subscription = subscription
        # subscription_dict update
        for key, value in self.subscription_dict.items():
            value.discard(client_id)
        for key, value in subscription.items():
            if key not in self.subscription_dict:
                self.subscription_dict[key] = set()
            self.subscription_dict[key].add(client_id)

    # Segment Level and Column Level Filter
    def dispatch_data(self, dispatcher: Dispatcher, header, body_data, tar_topic_id=None, tar_table_id=None):
        destinations = dispatcher.get_destinations(header['topic_id'], header['table_id'])
        if tar_topic_id and tar_table_id:
            destinations = [d for d in destinations if d['topic_id'] == tar_topic_id and d['table_id'] == tar_table_id]
        # Column Level Filter
        field_list, field_set = list(), set()
        for destination in destinations:
            if not destination.get('fields', None):
                field_set = set()
                break
            else:
                field_set |= set(destination['fields'])
                if destination.get('filters', None):
                    filter_fields = get_fields_from_filter(destination['filters'])
                    field_set |= set(filter_fields)
        field_list = list(field_set)
        # Header Data, No modification at all
        if int(header.get('age', 0)) == 1:
            return dispatcher.dispatch(header, body_data, None, tar_topic_id, tar_table_id)
        # Body Data Type => Only send needed column
        elif header['data_store'] == 'body':
            if field_list:
                tar_body_data = filter_table_column(body_data, field_list)
            else:
                tar_body_data = body_data
            return dispatcher.dispatch(header, tar_body_data, None, tar_topic_id, tar_table_id)
        # File Data Type
        elif header['data_store'] == 'file':
            # Check if the sennd is necessary
            if 'portait' in header:
                for f, description in header['portait'].items():
                    pass
            # Segement Check Pass, There is some data to be sent
            self.archiver.load_archive(header['merge_key'], field_list)
            tar_file_data = self.archiver.get_data()
            return dispatcher.dispatch(header, None, tar_file_data, tar_topic_id, tar_table_id)




