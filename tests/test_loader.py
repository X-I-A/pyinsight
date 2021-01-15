import os
import json
import pytest
from xialib import IOListArchiver, FileDepositor, BasicPublisher, BasicStorer, BasicSubscriber
from pyinsight.packager import Packager
from pyinsight.dispatcher import Dispatcher
from pyinsight.loader import Loader

load_config = {
    'publisher_id': 'client-001',
    'src_topic_id': 'scenario_01',
    'src_table_id': 'aged_data',
    'destination': os.path.join('.', 'output', 'loader'),
    'tar_topic_id': 'test_unit',
    'tar_table_id': 'aged_unit',
    'fields': ['id', 'first_name', 'last_name', 'height', 'children', 'lucky_numbers'],
    'filters': [[['gender', '=', 'Male'], ['height', '>=', 175]],
                [['gender', '=', 'Female'], ['weight', '<=', 100]]],
    'load_type': 'initial'
}

@pytest.fixture(scope='module')
def loader():
    storer = BasicStorer()
    archiver = IOListArchiver(archive_path=os.path.join('.', 'output', 'archiver'), fs=BasicStorer())
    depositor = FileDepositor(deposit_path=os.path.join('.', 'output', 'depositor'))
    publisher = BasicPublisher()
    publishers = {'client-001': publisher,
                  'client-002': publisher}
    loader = Loader(depositor=depositor, archiver=archiver, publisher=publishers)
    yield loader

def test_exceptions(loader):
    subscriber = BasicSubscriber()
    error_config_1 = load_config.copy()
    error_config_1.pop('publisher_id')
    loader.load(error_config_1)
    for msg in subscriber.pull(loader.channel, loader.topic_backlog):
        header, data, msg_id = subscriber.unpack_message(msg)
        assert header['table_id'] == 'INS-000011'
        subscriber.ack(loader.channel, loader.topic_backlog, msg_id)

    error_config_2 = load_config.copy()
    error_config_2['src_topic_id'] = 'err'
    assert not loader.load(error_config_2)

    load_config_3 = load_config.copy()
    load_config_3['src_topic_id'] = 'scenario_02'
    load_config_3['src_table_id'] = 'header_only'
    assert not loader.load(load_config_3)

    load_config_4 = load_config.copy()
    load_config_4['src_topic_id'] = 'scenario_02'
    assert loader.load(load_config_4)

    error_config_5 = load_config_4.copy()
    error_config_5['load_type'] = 'err'
    assert loader.load(error_config_5)

    load_config_6 = load_config_4.copy()
    load_config_6['load_type'] = 'err'
    assert loader.load(load_config_6)

    load_config_7 = load_config_4.copy()
    load_config_7['load_type'] = 'header'
    assert loader.load(load_config_7)

    load_config_8 = load_config_4.copy()
    load_config_8['load_type'] = 'normal'
    load_config_8['start_key'] = '0'
    load_config_8['end_key'] = '9'
    assert loader.load(load_config_8)