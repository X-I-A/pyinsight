import os
import json
import time
import pytest
from pyinsight.utils.core import get_current_timestamp
from pyinsight.archiver.archivers.file_archiver import FileArchiver

@pytest.fixture(scope='module')
def archiver():
    archiver = FileArchiver()
    yield archiver

def test_init_topic(archiver):
    archiver.init_insight()
    archiver.init_topic('test-005')

def test_scenario(archiver):
    archiver.set_current_topic_table('test-005', 'person_simple')
    merge_key_1 = get_current_timestamp()
    time.sleep(0.001)
    merge_key_2 = get_current_timestamp()
    time.sleep(0.001)
    merge_key_3 = get_current_timestamp()
    field_list_01 = ['id', 'first_name', 'height']
    archiver.set_merge_key(merge_key_1)
    # Step 1 Add Data
    for x in range(1,8):
        src_file = str(x).zfill(6) + '.json'
        with open(os.path.join('.', 'input', 'person_simple', src_file), 'r') as f:
            archiver.add_data(json.load(f))
    assert len(archiver.get_data()) == 7000

    # Step 2 Archive Data
    archiver.archive_data()
    archiver.remove_data()
    assert len(archiver.get_data()) == 0

    # Step 3 Archive More Data
    archiver.set_merge_key(merge_key_2)
    for x in range(8,15):
        src_file = str(x).zfill(6) + '.json'
        with open(os.path.join('.', 'input', 'person_simple', src_file), 'r') as f:
            archiver.add_data(json.load(f))
    assert len(archiver.get_data()) == 7000
    archiver.archive_data()
    archiver.remove_data()

    # Step 4 Load Some Part of file
    archiver.load_archive(merge_key_1, fields=field_list_01)
    archiver.append_archive(merge_key_2, fields=field_list_01)
    records = archiver.get_data()
    assert len(records) == 14000
    for line in records:
        assert 'email' not in line
        break
    archiver.set_merge_key(merge_key_3)
    archiver.archive_data()
    archiver.remove_data()

    # Step 5 Read File Test
    records = archiver.read_data_from_file('gzip', 'record', os.path.join('.', 'input', 'insight_formats',
                                                                       'aged_package.gz'))
    assert len(records) == 7000

    # Step 6 Clean
    archiver.remove_archives([merge_key_1, merge_key_2, merge_key_3])



