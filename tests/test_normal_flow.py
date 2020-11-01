import os
import json
import pytest
from pyinsight.utils.core import get_current_timestamp, get_merge_level, encoder
from pyinsight import Receiver, Merger, Packager, Cleaner

def get_normal_header():
    start_seq = get_current_timestamp()
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), 'r') as f:
        body = json.load(f).pop('columns')
        header = {'topic_id': 'test-002', 'table_id': 'person_simple', 'start_seq': start_seq,
                  'age': '1', 'merge_level': 9, 'merge_status': 'header', 'merge_key': start_seq,
                  'data_encode': 'flat', 'data_format': 'record', 'data_store': 'body'}
        return header, body

def get_normal_document(src_id):
    age = src_id + 1
    src_file = str(src_id).zfill(6) + '.json'
    start_seq = get_current_timestamp()
    with open(os.path.join('.', 'input', 'person_simple', src_file), 'r') as f:
        body = json.load(f)
        merge_key = start_seq
        header = {'topic_id': 'test-002', 'table_id': 'person_simple', 'start_seq': start_seq,
                  'merge_status': 'initial', 'merge_key': merge_key,
                  'data_encode': 'flat', 'data_format': 'record', 'data_store': 'body'}
        header['merge_level'] = get_merge_level(merge_key)
    return header, body

def test_simple_normal_flow():
    # start_seq = get_current_timestamp()
    # start_seq = '20201031193904651613'
    topic_id = 'test-002'
    r = Receiver()
    m = Merger()
    p = Packager()
    c = Cleaner()
    r.messager.init_topic(topic_id)
    r.depositor.init_topic(topic_id)
    r.archiver.init_topic(topic_id)


    # Step 1: Read Test data and send message
    header, body = get_normal_header()
    r.messager.publish(topic_id, header, body)
    for x in range(1, 51):
        header, body = get_normal_document(x)
        r.messager.publish(topic_id, header, body)

    # Step 2: Receive Data
    for msg in r.messager.pull(topic_id):
        header, body, id = r.messager.extract_message_content(msg)
        r.receive_data(header, body)
        r.messager.ack(topic_id, id)

    # Step 3: Merge Data
    for x in range(18):
        for msg in r.messager.pull(m.messager.topic_merger):
            header, data, id = m.messager.extract_message_content(msg)
            m.merge_data(header['topic_id'], header['table_id'], header['merge_key'], int(header['merge_level']), 2 ** 12)
            m.messager.ack(m.messager.topic_merger, id)

    # Step 4: Package Data
    p.messager.trigger_package(header['topic_id'], header['table_id'])
    for msg in p.messager.pull(p.messager.topic_packager):
        header, data, id = p.messager.extract_message_content(msg)
        p.package_data(header['topic_id'], header['table_id'], 2 ** 20)
        p.messager.ack(p.messager.topic_packager, id)

    # Step 5: All data check
    total_size = 0
    for doc_ref in r.depositor.get_stream_by_sort_key(['initial', 'merged', 'packaged']):
        doc_dict = r.depositor.get_dict_from_ref(doc_ref)
        if doc_dict['data_store'] == 'file':
            r.archiver.load_archive(doc_dict['merge_key'])
            total_size += len(r.archiver.get_data())
        else:
            total_size += len(json.loads(encoder(doc_dict['data'], r.depositor.data_encode, 'flat')))
    assert total_size == 50000

    # Step 6: Clean Test Set
    c.remove_all_data(header['topic_id'], header['table_id'])