import os
import json
import pytest
from xialib import BasicPublisher, BasicTranslator, FileDepositor, BasicStorer, ListArchiver
from pyinsight.insight import Insight

@pytest.fixture(scope='module')
def insight():
    storer = BasicStorer()
    publishers = {'test-001': BasicPublisher()}
    archiver = ListArchiver(archive_path=os.path.join('.', 'output', 'archiver'))
    depositor = FileDepositor(deposit_path=os.path.join('.', 'output', 'depositor'))
    insight = Insight(storers=[storer], publishers=publishers, archiver=archiver, depositor=depositor)
    yield insight

def test_messager_setting(insight):
    messager = BasicPublisher()
    insight.set_internal_channel(messager=messager,
                                 topic_backlog='backlog',
                                 topic_cleaner='cleaner',
                                 topic_cockpit='cockpit',
                                 topic_loader='loader',
                                 topic_merger='merger',
                                 topic_packager='packager',
                                 channel=os.path.join('.', 'insight', 'messager'))

def test_messager_send(insight):
    insight.trigger_merge('test', 'aged_data', '1234567890', 1, 2)
    insight.trigger_clean('test', 'aged_data', '1234567890')
    insight.trigger_package('test', 'aged_data')
    load_config = {'src_topic_id': 'test', 'src_table_id': 'aged_data'}
    insight.trigger_load(load_config)

def test_filter_operations(insight):
    filters1 = [[['gender', '=', 'Male'], ['height', '>=', 175]],
              [['gender', '=', 'Female'], ['weight', '<=', 100]]]
    filters2 = [[['gender', '=', 'Male'], ['height', '>', 175]],
              [['gender', '!=', 'Male'], ['weight', '<', 100]]]
    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
    fields = ['id', 'first_name', 'last_name', 'height', 'children', 'lucky_numbers']
    result1 = insight.filter_table(data_body, fields, filters1)
    result2 = insight.filter_table(data_body, None, filters2)
    result3 = insight.filter_table(data_body, fields, None)
    result4 = insight.filter_table(data_body, None, None)
    all_need_fields = insight.get_minimum_fields(fields, filters1)

def test_exceptions(insight):
    with pytest.raises(TypeError):
        wrong_ins = Insight(archiver=object())
    with pytest.raises(TypeError):
        wrong_ins = Insight(depositor=object())
    with pytest.raises(TypeError):
        wrong_ins = Insight(publishers={'dummy': object()})
    with pytest.raises(TypeError):
        wrong_ins = Insight(storers=[object()])
    with pytest.raises(TypeError):
        insight.set_internal_channel(messager=object())
