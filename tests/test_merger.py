import os
import json
import pytest
from xialib import FileDepositor, BasicTranslator
from pyinsight.merger import Merger

@pytest.fixture(scope='module')
def merger():
    depositor = FileDepositor(deposit_path=os.path.join('.', 'output', 'depositor'))
    merger = Merger(depositor=depositor)
    yield merger

def test_simpler_merger(merger):
    translator = BasicTranslator()
    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        field_data = data_header.pop('columns')
        header = {'topic_id': 'test', 'table_id': 'aged_data', 'aged': 'True',
                  'data_encode': 'flat', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': data_header}
    merger.depositor.add_document(header, field_data)

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        age_header = {'topic_id': 'test', 'table_id': 'aged_data',
                      'data_encode': 'gzip', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                      'age': '2', 'end_age': '100', 'start_seq': '20201113222500000000'}
    translator.compile(age_header, data_body)
    age_data_body = [translator.get_translated_line(item, age=2) for item in data_body]
    merger.depositor.add_document(age_header, age_data_body)

    assert not merger.merge_data('test', 'aged_data', '20201113222500000100', 2, 7)
    assert merger.merge_data('test', 'aged_data', '20201113222500000100', 1, 7)

