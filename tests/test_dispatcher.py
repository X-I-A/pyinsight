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
    publisher = {'client-001': publisher,
                 'client-002': publisher,
                 'client-003': publisher,
                 'client-004': publisher}
    subscription_list = [['test', 'aged_data', 'client-001', dest01, 't1', 'aged', None, None, None],
                         ['test', 'aged_data', 'client-002', dest02, 't2', 'aged', fields, None, None],
                         ['test', 'normal_data', 'client-003', dest03, 't3', 'aged', None, filters1, None],
                         ['test', 'normal_data', 'client-004', dest04, 't4', 'aged', fields, filters1, None]]

    tar_config = [
        {"publisher_id": "client-001", "destination": dest01, "tar_topic_id": "t1"},
        {"publisher_id": "client-002", "destination": dest02, "tar_topic_id": "t2"},
        {"publisher_id": "client-003", "destination": dest03, "tar_topic_id": "t3"},
        {"publisher_id": "client-004", "destination": dest04, "tar_topic_id": "t4"},
    ]
    topic_routes = [
        {"source": "test", "target": "t1"},
        {"source": "test", "target": "t3"}
    ]
    dispatcher = Dispatcher(publisher=publisher, route_file=os.path.join(".", "input", "test.zip"))
    dispatcher.set_tar_config(tar_config)
    dispatcher.set_topic_routes(topic_routes)
    dispatcher.set_internal_channel(channel=os.path.join('.', 'output', 'messager'))
    yield dispatcher

def generate_routes():
    fields = ['id', 'first_name', 'last_name', 'height', 'children', 'lucky_numbers']
    filters1 = [[['gender', '=', 'Male'], ['height', '>=', 175]],
                [['gender', '=', 'Female'], ['weight', '<=', 100]]]
    filters2 = [[['gender', '=', 'Male'], ['height', '>', 175]],
                [['gender', '!=', 'Male'], ['weight', '<', 100]]]
    route1 = {"src_topic_id": "test", "src_table_id": "aged_data", "tar_topic_id": "t1", "tar_table_id": "aged"}
    route2 = {"src_topic_id": "test", "src_table_id": "aged_data", "tar_topic_id": "t2", "tar_table_id": "aged",
              "fields": fields}
    route3 = {"src_topic_id": "test", "src_table_id": "normal_data", "tar_topic_id": "t3", "tar_table_id": "aged",
              "filters": filters1}
    route4 = {"src_topic_id": "test", "src_table_id": "normal_data", "tar_topic_id": "t4", "tar_table_id": "aged",
              "fields": fields, "filters": filters1}
    with open("aged_data", "w") as fp:
        json.dump([route1, route2], fp)
    with open("normal_data", "w") as fp:
        json.dump([route3, route4], fp)

def test_send_age_header(dispatcher):
    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        header = {'topic_id': 'test', 'table_id': 'aged_data', 'aged': 'True',
                  'data_encode': 'flat', 'data_format': 'record', 'data_spec': 'x-i-a', 'data_store': 'body',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': {}}
    dispatcher.dispatch_data(header, data_header)

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
    dispatcher.dispatch_data(age_header, age_data_body)

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
    dispatcher.dispatch_data(age_header, age_data_flat)

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
    dispatcher.dispatch_data(age_header, age_data_blob)

def test_send_age_b64g_document(dispatcher):
    translator = BasicTranslator()

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        age_header = {'topic_id': 'test', 'table_id': 'aged_data',
                      'data_encode': 'b64g', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                      'age': '2', 'end_age': '100', 'start_seq': '20201113222500000000'}
    translator.compile(age_header, data_body)
    age_data_body = [translator.get_translated_line(item, age=2) for item in data_body]
    age_header['data_spec'] = 'x-i-a'
    age_data_b64g = base64.b64encode(gzip.compress(json.dumps(age_data_body, ensure_ascii=False).encode())).decode()
    dispatcher.dispatch_data(age_header, age_data_b64g)

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
    dispatcher.dispatch_data(age_header, age_data_zipped)

def test_send_normal_header(dispatcher):
    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        header = {'topic_id': 'test', 'table_id': 'normal_data',
                  'data_encode': 'flat', 'data_format': 'record', 'data_spec': 'x-i-a', 'data_store': 'body',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': {}}
    dispatcher.dispatch_data(header, data_header)

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
    dispatcher.dispatch_data(normal_header, normal_data_body)

def test_send_init_with_storer():
    disp1 = Dispatcher(publisher={"dummy": BasicPublisher()}, route_file=".", storer=BasicStorer())
    disp2 = Dispatcher(publisher={"dummy": BasicPublisher()}, route_file=".", storer={"default": BasicStorer()})

def test_exceptions(dispatcher):
    with pytest.raises(ValueError):
        ko_disp = Dispatcher(publisher=BasicPublisher(), route_file=".")
