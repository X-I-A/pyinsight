import os
import json
import pytest
from xialib import ListArchiver, FileDepositor, BasicTranslator
from pyinsight.packager import Packager

@pytest.fixture(scope='module')
def packager():
    archiver = ListArchiver(archive_path=os.path.join('.', 'output', 'archiver'))
    depositor = FileDepositor(deposit_path=os.path.join('.', 'output', 'depositor'))
    packager = Packager(depositor=depositor, archiver=archiver)
    yield packager

def test_age_package(packager):
    packager.package_size = 2 ** 10
    translator = BasicTranslator()
    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        field_data = data_header.pop('columns')
        header = {'topic_id': 'test', 'table_id': 'aged_data', 'aged': 'True',
                  'data_encode': 'flat', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': data_header}
        packager.depositor.add_document(header, field_data)

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        age_header = {'topic_id': 'test', 'table_id': 'aged_data',
                      'data_encode': 'gzip', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                      'age': '2', 'end_age': '100', 'start_seq': '20201113222500000000'}
    translator.compile(age_header, data_body)
    age_data_body = [translator.get_translated_line(item, age=2) for item in data_body]
    age_header['data_spec'] = 'x-i-a'
    doc_dict = packager.depositor.add_document(age_header, age_data_body)

    packager.depositor.size_limit = 100000
    for mlvl in range(1, 8):
        assert packager.depositor.merge_documents('20201113222500000100', mlvl)
    assert packager.package_data('test', 'aged_data')

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        age_header = {'topic_id': 'test', 'table_id': 'aged_data',
                      'data_encode': 'gzip', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                      'age': '101', 'end_age': '156', 'start_seq': '20201113222500000000'}
    translator.compile(age_header, data_body)
    age_data_body = [translator.get_translated_line(item, age=101) for item in data_body]
    age_header['data_spec'] = 'x-i-a'
    doc_dict = packager.depositor.add_document(age_header, age_data_body)

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        age_header = {'topic_id': 'test', 'table_id': 'aged_data',
                      'data_encode': 'gzip', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                      'age': '157', 'end_age': '267', 'start_seq': '20201113222500000000'}
    translator.compile(age_header, data_body)
    age_data_body = [translator.get_translated_line(item, age=157) for item in data_body]
    age_header['data_spec'] = 'x-i-a'
    doc_dict = packager.depositor.add_document(age_header, age_data_body)
    assert packager.depositor.merge_documents('20201113222500000267', 1)
    assert packager.package_data('test', 'aged_data')

    assert packager.depositor.merge_documents('20201113222500000156', 1)
    packager.package_size = 2 ** 18
    assert packager.package_data('test', 'aged_data')

def test_normal_package(packager):
    packager.package_size = 2 ** 10
    packager.depositor.size_limit = 2 ** 10
    translator = BasicTranslator()

    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        field_data = data_header.pop('columns')
        header = {'topic_id': 'test', 'table_id': 'normal_data',
                  'data_encode': 'flat', 'data_format': 'record', 'data_spec': 'x-i-a', 'data_store': 'body',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': data_header}
        doc_dict = packager.depositor.add_document(header, field_data)

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        normal_header = {'topic_id': 'test', 'table_id': 'normal_data',
                         'data_encode': 'gzip', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                         'start_seq': '20201113222500001234'}
        translator.compile(normal_header, data_body)
    normal_data_body = [translator.get_translated_line(item, start_seq='20201113222500001234') for item in data_body]
    normal_header['data_spec'] = 'x-i-a'
    doc_dict = packager.depositor.add_document(normal_header, normal_data_body)
    assert packager.depositor.merge_documents('20201113222500001234', 1)
    assert packager.package_data('test', 'normal_data')