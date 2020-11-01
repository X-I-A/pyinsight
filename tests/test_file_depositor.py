import os
import json
import pytest
from pyinsight.utils.core import get_current_timestamp, encoder, get_merge_level, get_sort_key_from_dict
from pyinsight.depositor.depositors.file_depositor import FileDepositor

@pytest.fixture(scope='module')
def depositor():
    depositor = FileDepositor()
    yield depositor

def add_aged_header(depositor):
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), 'r') as f:
        body = json.load(f).pop('columns')
        start_seq = get_current_timestamp()
        header = {'topic_id': 'test-001', 'table_id': 'person_simple', 'start_seq': start_seq,
                  'age': '1', 'aged': 'true', 'merge_level': 9, 'merge_status': 'header', 'merge_key': start_seq,
                  'data_encode': 'flat', 'data_format': 'record', 'data_store': 'body'}
        depositor.add_document(header, body)

def add_normal_header(depositor):
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), 'r') as f:
        body = json.load(f).pop('columns')
        start_seq = get_current_timestamp()
        header = {'topic_id': 'test-001', 'table_id': 'person_simple', 'start_seq': start_seq,
                  'age': '1', 'merge_level': 9, 'merge_status': 'header', 'merge_key': start_seq,
                  'data_encode': 'flat', 'data_format': 'record', 'data_store': 'body'}
        depositor.add_document(header, body)

def add_age_document(depositor, start_seq, src_id):
    age = src_id + 1
    src_file = str(src_id).zfill(6) + '.json'
    with open(os.path.join('.', 'input', 'person_simple', src_file), 'r') as f:
        body = json.load(f)
        merge_key = str(int(start_seq) + age)
        header = {'topic_id': 'test-001', 'table_id': 'person_simple', 'start_seq': start_seq,
                  'age': str(age), 'merge_status': 'initial', 'merge_key': merge_key,
                  'data_encode': 'flat', 'data_format': 'record', 'data_store': 'body'}
        header['merge_level'] = get_merge_level(merge_key)
        depositor.add_document(header, body)

def add_normal_document(depositor, src_id):
    src_file = str(src_id).zfill(6) + '.json'
    with open(os.path.join('.', 'input', 'person_simple', src_file), 'r') as f:
        body = json.load(f)
        start_seq = get_current_timestamp()
        header = {'topic_id': 'test-001', 'table_id': 'person_simple', 'start_seq': start_seq,
                  'merge_status': 'initial', 'merge_key': start_seq,
                  'data_encode': 'flat', 'data_format': 'record', 'data_store': 'body'}
        header['merge_level'] = get_merge_level(start_seq)
        depositor.add_document(header, body)

def test_init_topic(depositor):
    depositor.init_topic('test-001')

def test_add_aged_header_document(depositor):
    add_aged_header(depositor)
    header_ref = depositor.get_table_header()
    assert header_ref is not None
    header_dict = depositor.get_dict_from_ref(header_ref)
    header_data = json.loads(encoder(header_dict['data'], depositor.data_encode, 'flat'))
    assert header_dict['topic_id'] == 'test-001'
    assert header_dict['aged'] == True
    assert len(header_data) == 9
    depositor.delete_documents([header_ref])
    header_ref = depositor.get_table_header()
    assert header_ref is None

def test_add_normal_header_document(depositor):
    add_normal_header(depositor)
    header_ref = depositor.get_table_header()
    assert header_ref is not None
    header_dict = depositor.get_dict_from_ref(header_ref)
    header_data = json.loads(encoder(header_dict['data'], depositor.data_encode, 'flat'))
    assert header_dict['topic_id'] == 'test-001'
    assert header_dict['aged'] == False
    assert len(header_data) == 9
    depositor.delete_documents([header_ref])
    header_ref = depositor.get_table_header()
    assert header_ref is None

def test_aged_documents(depositor):
    add_aged_header(depositor)
    del_list = list()
    header_ref = depositor.get_table_header()
    header_dict = depositor.get_dict_from_ref(header_ref)
    del_list.append(header_ref)
    for x in range(1, 10):
        add_age_document(depositor, header_dict['start_seq'], x)
    # Merge first two documents
    base_ref, append_ref = None, None
    for doc_ref in depositor.get_stream_by_sort_key(['initial']):
        append_ref = doc_ref
        break
    append_dict = depositor.get_dict_from_ref(append_ref)
    append_key = get_sort_key_from_dict(append_dict)
    for doc_ref in depositor.get_stream_by_sort_key(['initial'], le_ge_key=append_key, equal=False):
        base_ref = doc_ref
        break
    base_dict = depositor.get_dict_from_ref(base_ref)
    base_key = get_sort_key_from_dict(base_dict)
    data_list = list()
    data_list.extend(json.loads(encoder(base_dict['data'], depositor.data_encode, 'flat')))
    data_list.extend(json.loads(encoder(append_dict['data'], depositor.data_encode, 'flat')))
    depositor.merge_documents(base_ref, True, append_key, base_key, data_list)
    for doc_ref in depositor.get_stream_by_sort_key(['merged']):
        doc_dict = depositor.get_dict_from_ref(doc_ref)
        doc_data = json.loads(encoder(doc_dict['data'], depositor.data_encode, 'flat'))
        assert len(doc_data) == 2000
        del_list.append(doc_ref)
    # Min level / Merge Key Get test
    for doc_ref in depositor.get_stream_by_sort_key(reverse=True, min_merge_level=1):
        doc_dict = depositor.get_dict_from_ref(doc_ref)
        doc_ref2 = depositor.get_ref_by_merge_key(doc_dict['merge_key'])
        doc_ref2_dict = depositor.get_dict_from_ref(doc_ref2)
        assert doc_dict['merge_key'] == doc_ref2_dict['merge_key']
    for doc_ref in depositor.get_stream_by_sort_key(le_ge_key=base_key, reverse=True, equal=False):
        doc_dict = depositor.get_dict_from_ref(doc_ref)
        append_dict = depositor.get_dict_from_ref(append_ref)
        assert doc_dict['merge_key'] == append_dict['merge_key']
        break
    # Delete All Documents
    for doc_ref in depositor.get_stream_by_sort_key(['initial'], le_ge_key=append_key):
        doc_dict = depositor.get_dict_from_ref(doc_ref)
        doc_data = json.loads(encoder(doc_dict['data'], depositor.data_encode, 'flat'))
        assert len(doc_data) == 1000
        del_list.append(doc_ref)
    depositor.delete_documents(del_list)
    header_ref = depositor.get_table_header()
    assert header_ref is None

def test_normal_documents(depositor):
    add_normal_header(depositor)
    del_list = list()
    header_ref = depositor.get_table_header()
    add_normal_document(depositor, 1)
    del_list.append(header_ref)
    for x in range(1, 10):
        add_normal_document(depositor, x)
    # Merge first two documents
    base_ref, append_ref = None, None
    for doc_ref in depositor.get_stream_by_sort_key(['initial']):
        append_ref = doc_ref
        break
    append_dict = depositor.get_dict_from_ref(append_ref)
    append_key = get_sort_key_from_dict(append_dict)
    for doc_ref in depositor.get_stream_by_sort_key(['initial'], le_ge_key=append_key, equal=False):
        base_ref = doc_ref
        break
    base_dict = depositor.get_dict_from_ref(base_ref)
    base_key = get_sort_key_from_dict(base_dict)
    data_list = list()
    data_list.extend(json.loads(encoder(base_dict['data'], depositor.data_encode, 'flat')))
    data_list.extend(json.loads(encoder(append_dict['data'], depositor.data_encode, 'flat')))
    depositor.merge_documents(base_ref, True, append_key, base_key, data_list)
    for doc_ref in depositor.get_stream_by_sort_key(['merged']):
        doc_dict = depositor.get_dict_from_ref(doc_ref)
        doc_data = json.loads(encoder(doc_dict['data'], depositor.data_encode, 'flat'))
        assert len(doc_data) == 2000
        del_list.append(doc_ref)
    # Min level / Merge Key Get test
    for doc_ref in depositor.get_stream_by_sort_key(reverse=True, min_merge_level=1):
        doc_dict = depositor.get_dict_from_ref(doc_ref)
        doc_ref2 = depositor.get_ref_by_merge_key(doc_dict['merge_key'])
        doc_ref2_dict = depositor.get_dict_from_ref(doc_ref2)
        assert doc_dict['merge_key'] == doc_ref2_dict['merge_key']
    # Delete All Documents
    for doc_ref in depositor.get_stream_by_sort_key(['initial'], le_ge_key=append_key):
        doc_dict = depositor.get_dict_from_ref(doc_ref)
        doc_data = json.loads(encoder(doc_dict['data'], depositor.data_encode, 'flat'))
        assert len(doc_data) == 1000
        del_list.append(doc_ref)
    depositor.delete_documents(del_list)
    header_ref = depositor.get_table_header()
    assert header_ref is None