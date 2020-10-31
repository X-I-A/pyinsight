import os
import json
import pytest
from pyinsight.utils.core import get_current_timestamp
from pyinsight import Receiver, Merger, Packager


def test_simple_aged_flow():
    start_seq = get_current_timestamp()
    topic_id = 'test-001'
    r = Receiver()
    m = Merger()
    p = Packager()
    r.messager.init_topic(topic_id)
    r.depositor.init_topic(topic_id)
    r.archiver.init_topic(topic_id)

    # Step 1: Read Test data and send message
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), 'r') as f:
        table_header = json.load(f)

