import os
import json
import pytest
from xialib import BasicPublisher, BasicTranslator, FileDepositor, BasicStorer
from pyinsight.dispatcher import Dispatcher

"""Simple Dispatcher Receive Body Message and Resend Body Message

"""

@pytest.fixture(scope='module')
def dispatcher():
    dest01 = os.path.join('.', 'output', 'dispatcher', 'dest01')
    dest02 = os.path.join('.', 'output', 'dispatcher', 'dest02')
    dest03 = os.path.join('.', 'output', 'dispatcher', 'dest03')
    dest04 = os.path.join('.', 'output', 'dispatcher', 'dest04')
    fields = ['id', 'first_name', 'last_name', 'height', 'children', 'lucky_numbers']
    filters1 = [[['gender', '=', 'Male'], ['height', '>=', 175]],
              [['gender', '=', 'Female'], ['weight', '<=', 100]]]
    filters2 = [[['gender', '=', 'Male'], ['height', '>', 175]],
              [['gender', '!=', 'Male'], ['weight', '<', 100]]]
    publisher = BasicPublisher()
    storer = BasicStorer()
    publishers = {'client-001': publisher,
                  'client-002': publisher,
                  'client-003': publisher,
                  'client-004': publisher}
    subscription_list = {('test', 'aged_data'):
                             [{'client-001': [(dest01, 't1', 'aged', None, None)]},
                              {'client-002': [(dest02, 't2', 'aged', fields, None)]}],
                         ('test', 'normal_data'):
                             [{'client-003': [(dest03, 't3', 'aged', None, filters1)]},
                              {'client-004': [(dest04, 't4', 'aged', fields, filters2)]}]}
    depositor = FileDepositor(deposit_path=os.path.join('.', 'output', 'depositor'))
    dispatcher = Dispatcher(publishers=publishers,
                            storers=[storer],
                            depositor=depositor,
                            subscription_list=subscription_list)
    dispatcher.set_internal_channel(channel=os.path.join('.', 'output', 'messager'))
    yield dispatcher

def test_send_age_header(dispatcher):
    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        field_data = data_header.pop('columns')
        header = {'topic_id': 'test', 'table_id': 'aged_data', 'aged': 'True',
                  'data_encode': 'flat', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': data_header}
    dispatcher.receive_data(header, field_data)

def test_send_age_document(dispatcher):
    translator = BasicTranslator()

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        age_header = {'topic_id': 'test', 'table_id': 'aged_data',
                      'data_encode': 'gzip', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                      'age': '2', 'end_age': '100', 'start_seq': '20201113222500000000'}
    translator.compile(age_header, data_body)
    age_data_body = [translator.get_translated_line(item, age=2) for item in data_body]
    dispatcher.receive_data(age_header, age_data_body)

def test_send_normal_header(dispatcher):
    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        field_data = data_header.pop('columns')
        header = {'topic_id': 'test', 'table_id': 'normal_data',
                  'data_encode': 'flat', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': data_header}
    dispatcher.receive_data(header, field_data)

def test_send_normal_document(dispatcher):
    translator = BasicTranslator()

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        normal_header = {'topic_id': 'test', 'table_id': 'normal_data',
                         'data_encode': 'gzip', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                         'start_seq': '20201113222500001234'}
    translator.compile(normal_header, data_body)
    normal_data_body = [translator.get_translated_line(item, start_seq='20201113222500001234') for item in data_body]
    dispatcher.receive_data(normal_header, normal_data_body)

def test_send_stored_document(dispatcher):
    age_header = {'topic_id': 'test', 'table_id': 'aged_data',
                  'data_encode': 'gzip', 'data_format': 'record', 'data_spec': 'x-i-a', 'data_store': 'file',
                  'age': '2', 'end_age': '100', 'start_seq': '20201113222500000000'}
    data_body = '000002.gz'
    dispatcher.receive_data(age_header, data_body)

def test_send_with_single_component(dispatcher):
    dispatcher_1 = Dispatcher(publishers=dispatcher.publishers, subscription_list=dispatcher.subscription_list)
    dispatcher_2 = Dispatcher(depositor=dispatcher.depositor)

    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        field_data = data_header.pop('columns')
        header = {'topic_id': 'test', 'table_id': 'aged_data', 'aged': 'True',
                  'data_encode': 'flat', 'data_format': 'record', 'data_spec': 'x-i-a', 'data_store': 'body',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': data_header}
    dispatcher_1.receive_data(header, field_data)
    dispatcher_2.receive_data(header, field_data)

def test_exceptions(dispatcher):
    with pytest.raises(TypeError):
        ko_disp = Dispatcher(subscription_list=dispatcher.subscription_list)
    ko_publishers = dispatcher.publishers.copy()
    ko_publishers.pop('client-003')
    with pytest.raises(TypeError):
        ko_disp = Dispatcher(publishers=ko_publishers, subscription_list=dispatcher.subscription_list)

    with pytest.raises(ValueError):
        age_header = {'topic_id': 'test', 'table_id': 'aged_data',
                      'data_encode': 'gzip', 'data_format': 'record', 'data_spec': 'x-i-a', 'data_store': 'gcs',
                      'age': '2', 'end_age': '100', 'start_seq': '20201113222500000000'}
        data_body = '000002.gz'
        dispatcher.receive_data(age_header, data_body)