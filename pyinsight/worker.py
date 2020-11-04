import logging
from pyinsight.utils.core import LOGGING_LEVEL

class Worker():
    def __init__(self):
        self.log_context = {'context': 'init'}
        self.logger = logging.getLogger("Insight.Worker")
        self.logger.setLevel(LOGGING_LEVEL)

    def init_insight(self): pass

    def init_topic(self, topic_id): pass