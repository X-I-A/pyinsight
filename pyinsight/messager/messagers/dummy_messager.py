import os
import json
from pyinsight import messager
from pyinsight.utils.core import *

class DummyMessager(messager.Messager):
    def __init__(self):
        super().__init__()
        self.home_path = os.path.join(os.path.expanduser('~'), 'insight-messager')

    def init_insight(self):
        if not os.path.exists(self.home_path):
            os.makedirs(self.home_path)
        for topic_name in self.insight_topics:
            path = os.path.join(self.home_path, topic_name)
            if not os.path.exists(path):
                os.makedirs(path)

    def init_topic(self, topic_id):
        if not os.path.exists(self.home_path):
            os.makedirs(self.home_path)
        path = os.path.join(self.home_path, topic_id)
        if not os.path.exists(path):
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
                if isinstance(message['data'], list):
                    message['data'] = json.dumps(message.pop('data'))
                message['id'] = msg_id
            yield message

    # Translate Message Content
    def extract_message_content(self, message):
        return message['header'], message['data'], message['id']

    # Acknowledge Reception
    def ack(self, subscription_id, msg_id):
        subscription_path = os.path.join(self.home_path, subscription_id)
        os.remove(os.path.join(subscription_path, msg_id))




