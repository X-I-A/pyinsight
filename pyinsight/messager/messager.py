from pyinsight.worker import Worker

class Messager(Worker):
    blob_support = False
    topic_cockpit  = 'pyinsight-cockpit'
    topic_cleaner  = 'pyinsight-cleaner'
    topic_merger   = 'pyinsight-merger'
    topic_packager = 'pyinsight-packager'
    topic_loader   = 'pyinsight-loader'
    topic_backlog  = 'pyinsight-backlog'

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

