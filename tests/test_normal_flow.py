import os
import json
import time
import pytest
from pyinsight.utils.core import get_current_timestamp, get_merge_level, encoder
from pyinsight import Receiver, Merger, Packager, Cleaner, Loader


def get_normal_file_document():
    with open(os.path.join('.', 'input', 'insight_formats', 'normal_package.packaged'), 'r') as f:
        header = json.load(f)
        header.pop('data')
        body = os.path.join('.', 'input', 'insight_formats', 'normal_package.gz')
        return header, body

def test_simple_normal_flow():
    # start_seq = get_current_timestamp()
    # start_seq = '20201031193904651613'
    topic_id = 'test-004'
    r = Receiver()
    m = Merger()
    p = Packager()
    c = Cleaner()
    r.messager.init_topic(topic_id)
    r.depositor.init_topic(topic_id)
    r.archiver.init_topic(topic_id)

    m.set_merge_size(2 ** 12)
    m.set_package_size(2 ** 20)
    p.set_package_size(2 ** 20)

    # Step 1: Read Test data and send message

    # Step 2: Receive Data
    for msg in r.messager.pull(topic_id):
        header, body, id = r.messager.extract_message_content(msg)
        r.receive_data(header, body)

    # Step 3: Merge Data
    for x in range(18):
        for msg in r.messager.pull(m.messager.topic_merger):
            header, data, id = m.messager.extract_message_content(msg)
            m.merge_data(header['topic_id'], header['table_id'], header['merge_key'], int(header['merge_level']))
            m.messager.ack(m.messager.topic_merger, id)

    # Step 4: Package Data
    p.messager.trigger_package(topic_id, header['table_id'])
    for msg in p.messager.pull(p.messager.topic_packager):
        header, data, id = p.messager.extract_message_content(msg)
        p.package_data(header['topic_id'], header['table_id'])
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
    assert total_size == 49000

    # Step 6: Load Test
    # Client Configurations
    sub1 = {(topic_id, 'person_simple'):[{'topic_id':'clnt003', 'table_id':'tall_man',
                                          'fields': ['id', 'first_name', 'last_name', 'email'],
                                          'filters': [[["gender", "=", "Male"], ["height", ">=", 175]]]}]}
    l = Loader()
    l.upsert_client_config('clnt003', sub1)
    load_config = {'src_topic_id': topic_id, 'src_table_id': 'person_simple',
                   'client_id': 'clnt003', 'tar_topic_id': 'clnt003', 'tar_table_id': 'tall_man',
                   'load_type': 'initial'}
    l.load(load_config)

    for x in range(18):
        for msg in l.messager.pull(l.messager.topic_loader):
            header, data, id = l.messager.extract_message_content(msg)
            l.load(header)
            l.messager.ack(l.messager.topic_loader, id)

    r1 = Receiver()
    for msg in r1.messager.pull('clnt003'):
        header, body, id = r1.messager.extract_message_content(msg)
        r1.receive_data(header, body)
        r1.messager.ack('clnt003', id)

    total_size = 0
    for doc_ref in r1.depositor.get_stream_by_sort_key(['initial', 'merged', 'packaged']):
        doc_dict = r1.depositor.get_dict_from_ref(doc_ref)
        doc_data = json.loads(encoder(doc_dict['data'], r.depositor.data_encode, 'flat'))
        for line in doc_data:
            assert 'height' not in line
        total_size += len(doc_data)
    assert total_size == 8296

    # Step 7: Clean Test Set
    c.remove_all_data('clnt003', 'tall_man')
    c.remove_all_data('test-004', 'person_simple')
    for msg in c.messager.pull(c.messager.topic_cleaner):
        header, data, id = c.messager.extract_message_content(msg)
        c.messager.ack(c.messager.topic_cleaner, id)

def test_gapped_normal_flow():
    # start_seq = get_current_timestamp()
    # start_seq = '20201031193904651613'
    topic_id = 'test-004'
    r = Receiver()
    m = Merger()
    p = Packager()
    c = Cleaner()
    r.messager.init_topic(topic_id)
    r.depositor.init_topic(topic_id)
    r.archiver.init_topic(topic_id)

    m.set_merge_size(2 ** 12)
    m.set_package_size(2 ** 20)
    p.set_package_size(2 ** 20)

    # Step 1: Read Test data and send message

    # Step 2.1: Receive Data
    for msg in r.messager.pull(topic_id):
        header, body, id = r.messager.extract_message_content(msg)
        if int(header['start_seq']) % 8 != 0:
            r.receive_data(header, body)

    # Step 2.2: Merge Data
    for x in range(18):
        for msg in r.messager.pull(m.messager.topic_merger):
            header, data, id = m.messager.extract_message_content(msg)
            m.merge_data(header['topic_id'], header['table_id'], header['merge_key'], int(header['merge_level']))

    # Step 3.1: Receive Data
    for msg in r.messager.pull(topic_id):
        header, body, id = r.messager.extract_message_content(msg)
        if int(header['start_seq']) % 8 == 0:
            r.receive_data(header, body)

    # Step 3.2: Merge Data
    for x in range(18):
        for msg in r.messager.pull(m.messager.topic_merger):
            header, data, id = m.messager.extract_message_content(msg)
            m.merge_data(header['topic_id'], header['table_id'], header['merge_key'], int(header['merge_level']))
            m.messager.ack(m.messager.topic_merger, id)

    # Step 4: Package Data
    p.messager.trigger_package(topic_id, header['table_id'])
    for msg in p.messager.pull(p.messager.topic_packager):
        header, data, id = p.messager.extract_message_content(msg)
        p.package_data(header['topic_id'], header['table_id'])
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
    assert total_size == 49000

    # Step 6: Clean Test Set
    c.remove_all_data('test-004', 'person_simple')
    for msg in c.messager.pull(c.messager.topic_cleaner):
        header, data, id = c.messager.extract_message_content(msg)
        c.messager.ack(c.messager.topic_cleaner, id)

def test_dispatch_normal_flow():
    start_seq = get_current_timestamp()
    topic_id = 'test-004'
    r = Receiver()
    m = Merger()
    p = Packager()
    c = Cleaner()
    r.messager.init_topic(topic_id)
    r.depositor.init_topic(topic_id)
    r.archiver.init_topic(topic_id)
    # Client Configurations
    sub1 = {(topic_id, 'person_simple'):[{'topic_id':'clnt003', 'table_id':'tall_man',
                                            'fields': ['id', 'first_name', 'last_name', 'email'],
                                            'filters': [[["gender", "=", "Male"], ["height", ">=", 175]]]}]}
    sub2 = {(topic_id, 'person_simple'):[{'topic_id':'clnt004', 'table_id':'tall_woman',
                                            'fields': ['id', 'first_name', 'last_name', 'email'],
                                            'filters': [[["gender", "=", "Female"], ["height", ">=", 165]]]}]}
    r.upsert_client_config('clnt003', sub1)
    r.upsert_client_config('clnt004', sub2)
    r.set_merge_size(2 ** 12)

    # Step 1: Read Test data and send message

    # Step 2: Receive Message Data
    for msg in r.messager.pull(topic_id):
        header, body, id = r.messager.extract_message_content(msg)
        r.receive_data(header, body)

    # Step 3: Receive Client Data
    r1 = Receiver()
    for msg in r1.messager.pull('clnt003'):
        header, body, id = r1.messager.extract_message_content(msg)
        r1.receive_data(header, body)
        r1.messager.ack('clnt003', id)

    r2 = Receiver()
    for msg in r2.messager.pull('clnt004'):
        header, body, id = r2.messager.extract_message_content(msg)
        r2.receive_data(header, body)
        r2.messager.ack('clnt004', id)

    # Step 4: All data check
    total_size = 0
    for doc_ref in r1.depositor.get_stream_by_sort_key(['initial', 'merged', 'packaged']):
        doc_dict = r1.depositor.get_dict_from_ref(doc_ref)
        total_size += len(json.loads(encoder(doc_dict['data'], r.depositor.data_encode, 'flat')))
    assert total_size == 8296

    total_size = 0
    for doc_ref in r2.depositor.get_stream_by_sort_key(['initial', 'merged', 'packaged']):
        doc_dict = r2.depositor.get_dict_from_ref(doc_ref)
        total_size += len(json.loads(encoder(doc_dict['data'], r.depositor.data_encode, 'flat')))
    assert total_size == 9607

    # Step 5: Clean all data
    for msg in r.messager.pull(m.messager.topic_merger):
        header, data, id = m.messager.extract_message_content(msg)
        m.messager.ack(m.messager.topic_merger, id)

    c.remove_all_data('clnt003', 'tall_man')
    c.remove_all_data('clnt004', 'tall_woman')
    c.remove_all_data('test-004', 'person_simple')
    for msg in c.messager.pull(c.messager.topic_cleaner):
        header, data, id = c.messager.extract_message_content(msg)
        c.messager.ack(c.messager.topic_cleaner, id)