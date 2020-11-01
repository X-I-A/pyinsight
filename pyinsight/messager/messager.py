from pyinsight.worker import Worker

class Messager(Worker):
    def __init__(self):
        self.blob_support = False
        self.topic_cockpit = 'insight-cockpit'
        self.topic_cleaner = 'insight-cleaner'
        self.topic_merger = 'insight-merger'
        self.topic_packager = 'insight-packager'
        self.topic_loader = 'insight-loader'
        self.topic_backlog = 'insight-backlog'
        self.insight_topics = [self.topic_cockpit, self.topic_cleaner, self.topic_merger,
                               self.topic_packager, self.topic_loader, self.topic_backlog]

    # Send Message
    def publish(self, topic_id, header, body): pass

    # Pull Message
    def pull(self, subscription_id): pass

    # Acknowledge Reception
    def ack(self, subscription_id, msg_id): pass

    # Translate Message Content
    def extract_message_content(self, message): pass

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
