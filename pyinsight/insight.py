import os
import gzip
import json
import logging
from typing import List, Dict, Any
from xialib import Service
from xialib import BasicPublisher
from xialib.storer import Storer
from xialib.publisher import Publisher

__all__ = ['Insight']


class Insight(Service):
    """Insight Application

    Attributes:
        messager (:obj:`Publisher`): A special publisher to handle internal control messages

    Notes:
        Considering implement your own messager, channel and its related topic_ids
    """
    INSIGHT_FIELDS = ['_AGE', '_SEQ', '_NO', '_OP']
    api_url = 'api.x-i-a.com'


    def __init__(self, **kwargs):
        super().__init__(**kwargs)
