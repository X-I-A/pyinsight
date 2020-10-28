from insight.worker import Worker

class Messager(Worker):
    blob_support = False
    topic_cockpit  = 'insight-cockpit'
    topic_cleaner  = 'insight-cleaner'
    topic_merger   = 'insight-merger'
    topic_packager = 'insight-packager'
    topic_loader   = 'insight-loader'
    topic_backlog  = 'insight-backlog'

    def __init__(self): pass

    # Send Message
    def publish(self, topic_id, header, body): pass

    # Pull Message
    def pull(self, subscription_id): pass

    # Acknowledge Reception
    def ack(self, subscription_id, msg_id): pass

    # Translate Message Content
    def extract_message_content(self, message): pass

    # Clean All Data before the precised start_seq
    def trigger_clean(self, topic_id, table_id, start_seq): pass

    # Trigger the merge process
    def trigger_merge(self, topic_id, table_id, merge_key, merge_level): pass

    # Trigger the package process
    def trigger_package(self, topic_id, table_id): pass

    # Trigger the load process of packaged data
    def trigger_load(self, load_config: dict): pass

