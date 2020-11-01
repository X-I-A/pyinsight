import pytest
from pyinsight.utils.core import get_current_timestamp
from pyinsight.messager.messagers.dummy_messager import DummyMessager

@pytest.fixture(scope='module')
def messager():
    messager = DummyMessager()
    yield messager

def test_init_topic(messager):
    messager.init_insight()
    messager.init_topic('test-001')

def test_load_message(messager):
    load_config = {'src_topic_id': 'test-001', 'src_table_id': 'person_simple',
                   'load_type':'initial'}
    message_no = messager.trigger_load(load_config)
    assert message_no is not None
    for msg in messager.pull(messager.topic_loader):
        header, data, id = messager.extract_message_content(msg)
        assert header['src_topic_id'] == 'test-001'
        assert header['src_table_id'] == 'person_simple'
        assert header['load_type'] == 'initial'
        messager.ack(messager.topic_loader, id)
    for msg in messager.pull(messager.topic_loader):
        assert 1 != 1

def test_clean_message(messager):
    message_no = messager.trigger_clean('test-001', 'person_simple', get_current_timestamp())
    assert message_no is not None
    for msg in messager.pull(messager.topic_cleaner):
        header, data, id = messager.extract_message_content(msg)
        assert header['topic_id'] == 'test-001'
        assert header['table_id'] == 'person_simple'
        messager.ack(messager.topic_cleaner, id)
    for msg in messager.pull(messager.topic_cleaner):
        assert 1 != 1

def test_merge_message(messager):
    message_no = messager.trigger_merge('test-001', 'person_simple', get_current_timestamp(), 2)
    assert message_no is not None
    for msg in messager.pull(messager.topic_merger):
        header, data, id = messager.extract_message_content(msg)
        assert header['topic_id'] == 'test-001'
        assert header['table_id'] == 'person_simple'
        assert int(header['merge_level']) == 2
        messager.ack(messager.topic_merger, id)
    for msg in messager.pull(messager.topic_merger):
        assert 1 != 1

def test_package_message(messager):
    message_no = messager.trigger_package('test-001', 'person_simple')
    assert message_no is not None
    for msg in messager.pull(messager.topic_packager):
        header, data, id = messager.extract_message_content(msg)
        assert header['topic_id'] == 'test-001'
        assert header['table_id'] == 'person_simple'
        messager.ack(messager.topic_packager, id)
    for msg in messager.pull(messager.topic_packager):
        assert 1 != 1