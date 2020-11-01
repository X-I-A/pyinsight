import os
import json
import pytest
from pyinsight.utils.core import get_current_timestamp, get_merge_level, encoder
from pyinsight import Receiver, Merger, Packager, Cleaner

if __name__=='__main__':
    topic_id = 'test-002'
    r = Receiver()
    m = Merger()
    p = Packager()
    c = Cleaner()
    r.messager.init_topic(topic_id)
    r.depositor.init_topic(topic_id)
    r.archiver.init_topic(topic_id)

    r.depositor.set_current_topic_table('test-002', 'person_simple')
    r.archiver.set_current_topic_table('test-002', 'person_simple')
    total_size = 0
    counter = dict()
    for doc_ref in r.depositor.get_stream_by_sort_key(['initial', 'merged', 'packaged']):
        doc_dict = r.depositor.get_dict_from_ref(doc_ref)
        if doc_dict['data_store'] == 'file':
            r.archiver.load_archive(doc_dict['merge_key'])
            for line in r.archiver.get_data():
                if '_AGE' in line:
                    counter[line['_AGE']] = counter.get(line['_AGE'], 0) + 1
                else:
                    total_size += 1
        else:
            total_size += len(json.loads(encoder(doc_dict['data'], r.depositor.data_encode, 'flat')))
        print(total_size)
    print(counter)
    # assert total_size == 50000