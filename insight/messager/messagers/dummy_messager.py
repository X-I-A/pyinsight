import os
import json
from insight import messager
from insight.utils.core import *

class DummyMessager(messager.Messager):
    home_path = os.path.expanduser('~')
    def __init__(self):
        super().__init__()
        self.home_path = os.path.join(self.home_path, 'insight')
        for name, path in self.__dict__.items():
            if name.endswith('_path') and not os.path.exists(path):
                os.makedirs(path)

    # Publish Message
    def publish(self, topic_id, header, body):
        publish_path = os.path.join(self.home_path, topic_id)
        if not os.path.exists(publish_path):
            os.makedirs(publish_path)
        header = {key: str(value) for key, value in header.items()}
        message = {'header': header, 'data': body}
        filename = get_current_timestamp()
        with open(os.path.join(publish_path, filename), 'w') as f:
            f.write(json.dumps(message))
        return filename

    # Get Message
    def pull(self, subscription_id):
        subscription_path = os.path.join(self.home_path, subscription_id)
        if not os.path.exists(subscription_path):
            return
        msg_list = sorted([p for p in os.listdir(subscription_path)])
        for msg_id in msg_list:
            with open(os.path.join(subscription_path, msg_id)) as f:
                message = json.load(f)
                message['id'] = msg_id
                message['topic_id'] = subscription_id
            yield message

    # Translate Message Content
    def extract_message_content(self, message):
        return message['header'], message['data'], message['id'], message['topic_id']

    # Acknowledge Reception
    def ack(self, subscription_id, msg_id):
        subscription_path = os.path.join(self.home_path, subscription_id)
        os.remove(os.path.join(subscription_path, msg_id))

    # Clean All Data before the precised start_seq
    def trigger_clean(self, topic_id, table_id, start_seq):
        header = {'topic_id':topic_id, 'table_id':table_id, 'start_seq':start_seq}
        return self.publish(self.topic_cleaner, header, '')

    # Trigger the merge process
    def trigger_merge(self, topic_id, table_id,  merge_key, merge_level):
        header = {'topic_id':topic_id,'table_id':table_id,'merge_key':merge_key, 'merge_level':merge_level}
        return self.publish(self.topic_merger, header, '')

    # Trigger the package process
    def trigger_package(self, topic_id, table_id):
        header = {'topic_id':topic_id, 'table_id':table_id}
        return self.publish(self.topic_packager, header, '')

    # Trigger the load process of header or packaged data
    def trigger_load(self, load_config: dict):
        return self.publish(self.topic_loader, load_config, '')


