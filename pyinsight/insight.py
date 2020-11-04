import logging
from pyinsight.utils.core import LOGGING_LEVEL

__all__ = ['Insight']


class Insight():
    def __init__(self):
        self.logger = logging.getLogger("Insight")
        self.logger.setLevel(LOGGING_LEVEL)