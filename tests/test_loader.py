import os
import json
import pytest
from xialib import IoListArchiver, FileDepositor, BasicPublisher, BasicStorer, BasicSubscriber
from pyinsight.packager import Packager
from pyinsight.dispatcher import Dispatcher
from pyinsight.loader import Loader

load_config = {
    'publisher_id': 'client-001',
    'src_topic_id': 'scenario_01',
    'src_table_id': 'aged_data',
    'destination': os.path.join('.', 'output', 'loader'),
    'tar_topic_id': 'test_01',
    'tar_table_id': 'aged_01',
    'start_key': '20201013222500000016',
    'end_key': '20211113222500000016'
}

@pytest.fixture(scope='module')
def loader():
    archiver = IoListArchiver(archive_path=os.path.join('.', 'output', 'archiver'), fs=BasicStorer())
    depositor = FileDepositor(deposit_path=os.path.join('.', 'output', 'depositor'))
    publisher = BasicPublisher()
    publishers = {'client-001': publisher,
                  'client-002': publisher}
    loader = Loader(depositor=depositor, archiver=archiver, publisher=publishers,
                    route_file=os.path.join(".", "input", "loader.zip"))
    yield loader

def test_simple(loader: Loader):
    cfg1 = load_config.copy()
    cfg1["tar_table_id"] = "dummy"
    loader.load(**cfg1)

def test_send_init_with_storer():
    archiver = IoListArchiver(archive_path=os.path.join('.', 'output', 'archiver'), fs=BasicStorer())
    depositor = FileDepositor(deposit_path=os.path.join('.', 'output', 'depositor'))
    disp1 = Loader(depositor=depositor, archiver=archiver,
                   publisher={"dummy": BasicPublisher()}, route_file=".", storer=BasicStorer())
    disp2 = Loader(depositor=depositor, archiver=archiver,
                   publisher={"dummy": BasicPublisher()}, route_file=".", storer={"default": BasicStorer()})

def test_exceptions(loader):
    archiver = IoListArchiver(archive_path=os.path.join('.', 'output', 'archiver'), fs=BasicStorer())
    depositor = FileDepositor(deposit_path=os.path.join('.', 'output', 'depositor'))
    error_config_1 = load_config.copy()
    error_config_1.pop('publisher_id')
    with pytest.raises(TypeError):
        loader.load(**error_config_1)

    with pytest.raises(ValueError):
        disp1 = Loader(depositor=depositor, archiver=archiver,
                       publisher=BasicPublisher(), route_file=".", storer=BasicStorer())
