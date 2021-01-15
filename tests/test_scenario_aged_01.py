import os
import json
import base64
import gzip
import asyncio
import logging
import pytest
from xialib import IOListArchiver, FileDepositor, BasicTranslator, BasicPublisher, BasicSubscriber, BasicStorer
from pyinsight.packager import Packager
from pyinsight.merger import Merger
from pyinsight.dispatcher import Dispatcher
from pyinsight.loader import Loader
from pyinsight.cleaner import Cleaner
from pyinsight.insight import Insight
from pyinsight.receiver import Receiver



# Basic Unit definition
depositor = FileDepositor(deposit_path=os.path.join('.', 'output', 'depositor'))
depositor.size_limit = 2 ** 12
archiver = IOListArchiver(archive_path=os.path.join('.', 'output', 'archiver'), fs=BasicStorer())
subscriber = BasicSubscriber()
storer = BasicStorer()
translator = BasicTranslator()
publisher = BasicPublisher()

# Basic Unit group
publisher = {'client-001': publisher,
             'client-002': publisher}
# Actor Definition
receiver = Receiver(depositor=depositor)
merger = Merger(depositor=depositor)
packager = Packager(depositor=depositor, archiver=archiver)
msg_loader = Loader(depositor=depositor, archiver=archiver, publisher=publisher)
file_loader = Loader(depositor=depositor, archiver=archiver, storer=storer, publisher=publisher)
cleaner = Cleaner(depositor=depositor, archiver=archiver)

load_config1 = {
    'publisher_id': 'client-001',
    'src_topic_id': 'scenario_01',
    'src_table_id': 'aged_data',
    'destination': os.path.join('.', 'output', 'loader'),
    'tar_topic_id': 'test_01',
    'tar_table_id': 'aged_01',
    'fields': ['id', 'first_name', 'last_name', 'height', 'children', 'lucky_numbers'],
    'filters': [[['gender', '=', 'Male'], ['height', '>=', 175]],
              [['gender', '=', 'Female'], ['weight', '<=', 100]]],
    'load_type': 'initial'
}

def merger_callback(s: BasicSubscriber, message: dict, source, subscription_id):
    header, data, msg_id = s.unpack_message(message)
    header.pop('data_spec')
    header['merge_level'] = int(header['merge_level'])
    header['target_merge_level'] = int(header['target_merge_level'])
    if merger.merge_data(**header):
        subscriber.ack(source, subscription_id, msg_id)

def purger():
    # Insight.log_level = logging.INFO
    # Insight Level Settings
    messager = BasicPublisher()
    Insight.set_internal_channel(messager=messager,
                                 topic_backlog='backlog',
                                 topic_cleaner='cleaner',
                                 topic_cockpit='cockpit',
                                 topic_loader='loader',
                                 topic_merger='merger',
                                 topic_packager='packager',
                                 channel=os.path.join('.', 'insight', 'messager'))

    for msg in subscriber.pull(Insight.channel, Insight.topic_merger):
        msg_id = subscriber.unpack_message(msg)[2]
        subscriber.ack(Insight.channel, Insight.topic_merger, msg_id)

    for msg in subscriber.pull(Insight.channel, Insight.topic_loader):
        msg_id = subscriber.unpack_message(msg)[2]
        subscriber.ack(Insight.channel, Insight.topic_loader, msg_id)

    for msg in subscriber.pull(Insight.channel, Insight.topic_packager):
        msg_id = subscriber.unpack_message(msg)[2]
        subscriber.ack(Insight.channel, Insight.topic_packager, msg_id)

    for msg in subscriber.pull(Insight.channel, Insight.topic_cleaner):
        msg_id = subscriber.unpack_message(msg)[2]
        subscriber.ack(Insight.channel, Insight.topic_cleaner, msg_id)

    cleaner.clean_data('scenario_01', 'normal_data', '99991231000000000000')

def normal_data_test():
    # Normal Data Header Receive
    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        header = {'topic_id': 'scenario_01', 'table_id': 'normal_data',
                  'data_encode': 'gzip', 'data_format': 'record', 'data_spec': 'x-i-a', 'data_store': 'body',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': {}}
    receiver.receive_data(header, data_header)

    # Normal Data Receive
    with open(os.path.join('.', 'input', 'person_complex', '000003.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        normal_header = {'topic_id': 'scenario_01', 'table_id': 'normal_data',
                      'data_encode': 'gzip', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                      'start_seq': '20201113222500000000'}
    translator.compile(normal_header, data_body)
    normal_data_body = [translator.get_translated_line(item, start_seq='20201113222500001240') for item in data_body]
    normal_header['data_spec'] = 'x-i-a'
    receiver.receive_data(normal_header, normal_data_body)

    # Merge message streaming
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    merge_task = subscriber.stream(Insight.channel, Insight.topic_merger, callback=merger_callback, timeout=2)
    loop.run_until_complete(asyncio.wait([merge_task]))
    loop.close()

    packager.package_size = 2 ** 16
    packager.package_data('scenario_01', 'normal_data')


def aged_data_test():
    # Aged Data Receive
    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        header = {'topic_id': 'scenario_01', 'table_id': 'aged_data', 'aged': 'True',
                  'data_encode': 'gzip', 'data_format': 'record', 'data_spec': 'x-i-a', 'data_store': 'body',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': {}}
    receiver.receive_data(header, data_header)

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        age_header = {'topic_id': 'scenario_01', 'table_id': 'aged_data',
                      'data_encode': 'gzip', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                      'age': '2', 'end_age': '100', 'start_seq': '20201113222500000000'}
    translator.compile(age_header, data_body)
    age_data_body = [translator.get_translated_line(item, age=2) for item in data_body]
    age_header['data_spec'] = 'x-i-a'
    receiver.receive_data(age_header, age_data_body)

    # Merge message streaming
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    merge_task = subscriber.stream(Insight.channel, Insight.topic_merger, callback=merger_callback, timeout=2)
    loop.run_until_complete(asyncio.wait([merge_task]))
    loop.close()

    for msg in subscriber.pull(Insight.channel, Insight.topic_cleaner):
        header, data, msg_id = subscriber.unpack_message(msg)
        subscriber.ack(Insight.channel, Insight.topic_cleaner, msg_id)

    packager.package_data('scenario_01', 'aged_data')

    # Second Data Receive
    with open(os.path.join('.', 'input', 'person_complex', '000003.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        age_header = {'topic_id': 'scenario_01', 'table_id': 'aged_data',
                      'data_encode': 'gzip', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                      'age': '101', 'end_age': '200', 'start_seq': '20201113222500000000'}
    translator.compile(age_header, data_body)
    age_data_body = [translator.get_translated_line(item, age=101) for item in data_body]
    age_header['data_spec'] = 'x-i-a'
    receiver.receive_data(age_header, age_data_body)

    # Merge message streaming
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    merge_task = subscriber.stream(Insight.channel, Insight.topic_merger, callback=merger_callback, timeout=2)
    loop.run_until_complete(asyncio.wait([merge_task]))
    loop.close()

    packager.package_data('scenario_01', 'aged_data')

    # Check data
    header_ref = depositor.get_table_header()
    header_dict = depositor.get_header_from_ref(header_ref)
    counter, merged_c, packaged_c = 0, 0, 0
    for doc_ref in depositor.get_stream_by_sort_key(status_list=['initial', 'merged', 'packaged']):
        doc_dict = depositor.get_header_from_ref(doc_ref)
        # print("{}-{}-{}".format(doc_dict['age'], doc_dict['merge_status'], doc_dict['line_nb']))
        counter += doc_dict['line_nb']
        if doc_dict['merge_status'] in ['merged', 'packaged']:
            merged_c += doc_dict['line_nb']
        if doc_dict['merge_status'] == 'packaged':
            packaged_c += doc_dict['line_nb']
    assert counter == 2000
    assert header_dict['merged_lines'] == merged_c - packaged_c
    # assert header_dict['packaged_lines'] == packaged_c

def load_data_test():

    # Load data 1
    load_config1['load_type'] = 'header'
    msg_loader.load(load_config1)
    load_config1['load_type'] = 'initial'
    msg_loader.load(load_config1)

    for x in range(10):
        for msg in subscriber.pull(Insight.channel, Insight.topic_loader):
            header, data, msg_id = subscriber.unpack_message(msg)
            msg_loader.load(load_config=json.loads(header['load_config']))
            subscriber.ack(Insight.channel, Insight.topic_loader, msg_id)

    # Save new loaded data
    counter = 0
    for msg in subscriber.pull(os.path.join('.', 'output', 'loader'), 'test_01'):
        header, data, msg_id = subscriber.unpack_message(msg)
        record_data = json.loads(gzip.decompress(base64.b64decode(data)).decode())
        if int(header.get('age', 0)) != 1:
            counter += len(record_data)
        receiver.receive_data(header, record_data)
        subscriber.ack(os.path.join('.', 'output', 'loader'), 'test_01', msg_id)
    assert counter == 999

    depositor.set_current_topic_table('test_01', 'aged_01')

    counter = 0
    for doc_ref in depositor.get_stream_by_sort_key(status_list=['initial']):
        doc_dict = depositor.get_header_from_ref(doc_ref)
        doc_data = depositor.get_data_from_header(doc_dict)
        counter += doc_dict['line_nb']
    assert counter == 999

    # Load skip check number >
    load_config3 = load_config1.copy()
    load_config3['load_type'] = 'package'
    load_config3['filters'] = [[['id', '>=', 2000]]]
    load_config3['start_key'] = '20201113222500000016'
    load_config3['end_key'] = '20201113222500000016'
    msg_loader.load(load_config=load_config3)

    # Load skip check number <
    load_config4 = load_config3.copy()
    load_config4['filters'] = [[['id', '<', -1000]]]
    msg_loader.load(load_config=load_config4)

    # Load skip check number <
    load_config5 = load_config3.copy()
    load_config5['filters'] = [[['city', '=', '上海']]]
    msg_loader.load(load_config=load_config5)


def final_clean():
    cleaner.clean_data('scenario_01', 'normal_data', '99991231000000000000')
    cleaner.clean_data('scenario_01', 'aged_data', '99991231000000000000')
    cleaner.clean_data('test_01', 'aged_01', '99991231000000000000')
    cleaner.clean_data('test_02', 'aged_02', '99991231000000000000')


def test_simple_flow():
    """Simple Test Flow

    This test will receive data, trigger merge process, trigger package process, dispatch data and then clean the data

    """
    purger()
    normal_data_test()
    aged_data_test()
    load_data_test()
    aged_data_test()
    final_clean()


