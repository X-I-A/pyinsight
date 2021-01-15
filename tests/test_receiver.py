import os
import json
import gzip
import base64
import pytest
from xialib import BasicPublisher, BasicTranslator, FileDepositor, BasicStorer, BasicSubscriber
from pyinsight.receiver import Receiver

"""Simple Dispatcher Receive Body Message

"""

@pytest.fixture(scope='module')
def receiver():
    depositor = FileDepositor(deposit_path=os.path.join('.', 'output', 'depositor'))
    receiver = Receiver(depositor=depositor)
    receiver.set_internal_channel(channel=os.path.join('.', 'output', 'messager'))
    yield receiver

def test_send_age_flat_document(receiver):
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
    receiver.receive_data(age_header, age_data_flat)

def test_send_age_blob_document(receiver):
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
    receiver.receive_data(age_header, age_data_blob)

def test_send_age_b64g_document(receiver):
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
    receiver.receive_data(age_header, age_data_b64g)

def test_send_age_gzip_document(receiver):
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
    receiver.receive_data(age_header, age_data_zipped)


def test_exceptions(receiver):
    with pytest.raises(TypeError):
        ko_disp = Receiver(depositor=object())
