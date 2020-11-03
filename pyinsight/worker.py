import logging
from pyinsight.utils.core import LOGGING_LEVEL

class Worker():
    def __init__(self):
        self.log_context = {'context': 'init'}
        self.logger = logging.getLogger("Insight.Worker")

        if self.logger.hasHandlers():
            self.logger.handlers.clear()
            formatter = logging.Formatter('%(asctime)s-%(funcName)s-%(levelname)s-%(context)s-%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        self.logger.setLevel(LOGGING_LEVEL)

    def init_insight(self): pass

    def init_topic(self, topic_id): pass