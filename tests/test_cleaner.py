import os
import json
import pytest
from xialib import ListArchiver, FileDepositor, BasicTranslator
from pyinsight.cleaner import Cleaner

@pytest.fixture(scope='module')
def cleaner():
    archiver = ListArchiver(archive_path=os.path.join('.', 'output', 'archiver'))
    depositor = FileDepositor(deposit_path=os.path.join('.', 'output', 'depositor'))
    cleaner = Cleaner(depositor=depositor, archiver=archiver)
    yield cleaner

def test_simple_cleaner(cleaner):
    translator = BasicTranslator()
    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        field_data = data_header.pop('columns')
        header = {'topic_id': 'test', 'table_id': 'normal_data',
                  'data_encode': 'flat', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': data_header}
    cleaner.depositor.add_document(header, field_data)

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        normal_header = {'topic_id': 'test', 'table_id': 'normal_data',
                         'data_encode': 'gzip', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                         'start_seq': '20201113222500001234'}
    translator.compile(normal_header, data_body)
    normal_data_body = [translator.get_translated_line(item, start_seq='20201113222500001234') for item in data_body]
    cleaner.archiver.set_current_topic_table('test', 'aged_data')
    cleaner.archiver.set_merge_key('20201113222500001234')
    cleaner.archiver.add_data(normal_data_body)
    archive_name = cleaner.archiver.archive_data()
    dummy_data = [{'_SEQ': '20201113222500001234'}]
    header_dict = cleaner.depositor.add_document(normal_header, dummy_data)

    for file in os.listdir(os.path.join('.', 'output', 'depositor', 'test', 'normal_data')):
        if file.startswith(header_dict['sort_key']):
            with open(os.path.join('.', 'output', 'depositor', 'test', 'normal_data', file)) as f1:
                json_data = json.load(f1)
                json_data['data_store'] = 'file'
                json_data['data'] = archive_name
            with open(os.path.join('.', 'output', 'depositor', 'test', 'normal_data', file), 'w') as f2:
                f2.write(json.dumps(json_data))
    for i in range(16):
        cleaner.depositor.add_document(normal_header, normal_data_body)

    cleaner.clean_data('test', 'normal_data', '99991115222500000000')
