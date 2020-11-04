import logging
from pyinsight.insight import Insight

__all__ = ['Worker']


class Worker(Insight):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger("Insight.Worker")
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          ':%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def init_insight(self): pass

    def init_topic(self, topic_id): pass