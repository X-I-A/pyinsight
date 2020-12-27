import pytest
import os
import json
from pyinsight import Caller

@pytest.fixture(scope='module')
def caller():
    caller = Caller(insight_id="www.x-i-a.com")
    yield caller

def test_source_table_init(caller: Caller):
    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        header = {'topic_id': 'test', 'table_id': 'aged_data', 'aged': 'True',
                  'event_type': 'source_table_init', 'event_token': 'dummy_token',
                  'data_encode': 'flat', 'data_format': 'record', 'data_spec': 'x-i-a', 'data_store': 'body',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': {}}
    url, path, data = caller.prepare_call(header, data_body)
    assert url == 'www.x-i-a.com'
    assert path == '/events/source-table-init'
    assert data['start_seq'] == header['start_seq']
    assert data['event_token'] == header['event_token']
    assert data['source_id'] == 'aged_data'

def test_exception(caller: Caller):
    header = {'topic_id': 'test', 'table_id': 'aged_data', 'aged': 'True',
              'event_type': 'dummy_type', 'event_token': 'dummy_token',
              'data_encode': 'flat', 'data_format': 'record', 'data_spec': 'x-i-a', 'data_store': 'body',
              'age': '1', 'start_seq': '20201113222500000000', 'meta-data': {}}
    with pytest.raises(TypeError):
        c = Caller(insight_id="error")
    with pytest.raises(ValueError):
        url, path, data = caller.prepare_call(header, [])
