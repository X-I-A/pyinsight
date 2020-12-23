import os
import json
import gzip
import base64
import pytest
from xialib import BasicPublisher, BasicTranslator, FileDepositor, BasicStorer, BasicSubscriber
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
        header = {'topic_id': 'test', 'table_id': 'aged_data', 'aged': 'True',
                  'data_encode': 'flat', 'data_format': 'record', 'data_spec': 'x-i-a', 'data_store': 'body',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': {}}
    dispatcher.receive_data(header, data_header)

def test_send_age_document(dispatcher):
    translator = BasicTranslator()

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        age_header = {'topic_id': 'test', 'table_id': 'aged_data',
                      'data_encode': 'gzip', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                      'age': '2', 'end_age': '100', 'start_seq': '20201113222500000000'}
    translator.compile(age_header, data_body)
    age_data_body = [translator.get_translated_line(item, age=2) for item in data_body]
    age_header['data_spec'] = 'x-i-a'
    dispatcher.receive_data(age_header, age_data_body)

def test_send_age_flat_document(dispatcher):
    translator = BasicTranslator()

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        age_header = {'topic_id': 'test', 'table_id': 'aged_data',
                      'data_encode': 'flat', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                      'age': '2', 'end_age': '100', 'start_seq': '20201113222500000000'}
    translator.compile(age_header, data_body)
    age_data_body = [translator.get_translated_line(item, age=2) for item in data_body]
    age_header['data_spec'] = 'x-i-a'
    age_data_flat = json.dumps(age_data_body, ensure_ascii=False)
    dispatcher.receive_data(age_header, age_data_flat)

def test_send_age_blob_document(dispatcher):
    translator = BasicTranslator()

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        age_header = {'topic_id': 'test', 'table_id': 'aged_data',
                      'data_encode': 'blob', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                      'age': '2', 'end_age': '100', 'start_seq': '20201113222500000000'}
    translator.compile(age_header, data_body)
    age_data_body = [translator.get_translated_line(item, age=2) for item in data_body]
    age_header['data_spec'] = 'x-i-a'
    age_data_blob = json.dumps(age_data_body, ensure_ascii=False).encode()
    dispatcher.receive_data(age_header, age_data_blob)

def test_send_age_gzip_document(dispatcher):
    translator = BasicTranslator()

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        age_header = {'topic_id': 'test', 'table_id': 'aged_data',
                      'data_encode': 'gzip', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                      'age': '2', 'end_age': '100', 'start_seq': '20201113222500000000'}
    translator.compile(age_header, data_body)
    age_data_body = [translator.get_translated_line(item, age=2) for item in data_body]
    age_header['data_spec'] = 'x-i-a'
    age_data_zipped = gzip.compress(json.dumps(age_data_body, ensure_ascii=False).encode())
    dispatcher.receive_data(age_header, age_data_zipped)

def test_send_normal_header(dispatcher):
    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        header = {'topic_id': 'test', 'table_id': 'normal_data',
                  'data_encode': 'flat', 'data_format': 'record', 'data_spec': 'x-i-a', 'data_store': 'body',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': {}}
    dispatcher.receive_data(header, data_header)

def test_send_normal_document(dispatcher):
    translator = BasicTranslator()

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        normal_header = {'topic_id': 'test', 'table_id': 'normal_data',
                         'data_encode': 'gzip', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                         'start_seq': '20201113222500001234'}
    translator.compile(normal_header, data_body)
    normal_data_body = [translator.get_translated_line(item, start_seq='20201113222500001234') for item in data_body]
    normal_header['data_spec'] = 'x-i-a'
    dispatcher.receive_data(normal_header, normal_data_body)

def test_send_with_single_component(dispatcher):
    dispatcher_1 = Dispatcher(publishers=dispatcher.publishers, subscription_list=dispatcher.subscription_list)
    dispatcher_2 = Dispatcher(depositor=dispatcher.depositor)

    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        header = {'topic_id': 'test', 'table_id': 'aged_data', 'aged': 'True',
                  'data_encode': 'flat', 'data_format': 'record', 'data_spec': 'x-i-a', 'data_store': 'body',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': {}}
    dispatcher_1.receive_data(header, data_header)
    dispatcher_2.receive_data(header, data_header)

def test_exceptions(dispatcher):
    with pytest.raises(TypeError):
        ko_disp = Dispatcher(subscription_list=dispatcher.subscription_list)
    ko_publishers = dispatcher.publishers.copy()
    ko_publishers.pop('client-003')
    with pytest.raises(TypeError):
        ko_disp = Dispatcher(publishers=ko_publishers, subscription_list=dispatcher.subscription_list)

    age_header = {'topic_id': 'test', 'table_id': 'aged_data',
                  'data_encode': 'gzip', 'data_format': 'record', 'data_spec': 'x-i-a', 'data_store': 'gcs',
                  'age': '2', 'end_age': '100', 'start_seq': '20201113222500000000'}
    data_body = '000002.gz'
    dispatcher.receive_data(age_header, data_body)

    subscriber = BasicSubscriber()
    for msg in subscriber.pull(dispatcher.channel, dispatcher.topic_backlog):
        header, data, msg_id = subscriber.unpack_message(msg)
        assert header['table_id'] == 'INS-000005'
        subscriber.ack(dispatcher.channel, dispatcher.topic_backlog, msg_id)