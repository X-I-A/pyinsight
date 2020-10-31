import logging

class Worker():
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s')
    # Initialize a Topic
    @classmethod
    def init_topic(self, topic_id): pass