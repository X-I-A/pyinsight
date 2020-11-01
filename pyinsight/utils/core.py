import os
import gzip
import json
import base64
import hashlib
import time
import datetime
import logging
from .exceptions import InsightDataSpecError

MERGE_SIZE = os.environ.get('INSIGHT_MERGE_SIZE', 3 ** 20)
PACKAGE_SIZE = os.environ.get('INSIGHT_PACKAGE_SIZE', 2 ** 25)

"""
Filter Section :
- Filed List
"""
# DNF Filter Related
def xia_eq(a, b):
    return a is not None and a == b

def xia_ge(a, b):
    return a is not None and a >= b

def xia_gt(a, b):
    return a is not None and a > b

def xia_le(a, b):
    return a is not None and a <= b

def xia_lt(a, b):
    return a is not None and a < b

def xia_ne(a, b):
    return a is not None and a != b

# Operation Dictionary:
oper = {'=': xia_eq, '>=': xia_ge, '>': xia_gt, '<=': xia_le, '<': xia_lt, '!=': xia_ne, '<>': xia_ne}

# Remove all Null values
def remove_none(dict):
    return {key: value for key, value in dict.items() if value}

# Get dnf filter field set
def get_fields_from_filter(ndf_filters):
    return set([x[0] for l1 in ndf_filters for x in l1 if len(x)>0])

# disjunctive normal form filters (DNF)
def _filter_dnf(line: dict, ndf_filters):
    return any([all([oper.get(l2[1])(line.get(l2[0],None),l2[2]) for l2 in l1 if len(l2)>0]) for l1 in ndf_filters])

def filter_table_dnf(dict_list, ndf_filters):
    return [line for line in dict_list if _filter_dnf(line, ndf_filters)]

# Dictionary Related Operation
# retrieve list of keys from
def _filter_column(line: dict, field_list):
    return {key: value for key, value in line.items() if key in field_list}

def filter_table_column(dict_list: list, field_list):
    return [_filter_column(line, field_list) for line in dict_list]

# Field_list + Filter solution => Apply to table
def filter_table(dict_list: list, field_list=list(), filter_list=list(list(list()))):
    if filter_list == list(list(list())) and not field_list:
        return dict_list
    elif filter_list == list(list(list())):
        return filter_table_column(dict_list, field_list)
    elif not field_list:
        return filter_table_dnf(dict_list, filter_list)
    else:
        return filter_table_column(filter_table_dnf(dict_list, filter_list), field_list)

# Allowed Transformation Matrix ['flat', 'b54g', 'gzip']
def encoder(data, src_encode, tar_encode):
    if src_encode == tar_encode:
        return data
    # blob and flat transformation
    if src_encode == 'gzip' and tar_encode == 'flat':
        return gzip.decompress(data).decode()
    elif src_encode == 'b64g' and tar_encode == 'flat':
        return gzip.decompress(base64.b64decode(data.encode())).decode()
    elif src_encode == 'flat' and tar_encode == 'b64g':
        return base64.b64encode(gzip.compress(data.encode())).decode()
    elif src_encode == 'gzip' and tar_encode == 'b64g':
        return base64.b64encode(data).decode()
    elif src_encode == 'flat' and tar_encode == 'gzip':
        return gzip.compress(data.encode())
    elif src_encode == 'b64g' and tar_encode == 'gzip':
        return base64.b64decode(data.encode())
    else:
        logging.error('Data Encode {}-{} not implemented'.format(src_encode, tar_encode))

# Data Cut and Send
def get_data_chunk(input_data: list, input_header: dict):
    tar_content = input_header.copy()
    # Case 1 : Aged Document => Must be cut age by age
    if 'age' in input_header:
        sorted_data = sorted(input_data, key=lambda line: line['_AGE'])
        data_chunk, start_age, cur_age = list(), 0, 0
        for line in sorted_data:
            if start_age == 0:
                start_age = int(input_header['age'])
            if line['_AGE'] != cur_age and data_chunk:
                tar_content['age'] = start_age
                tar_content['end_age'] = line['_AGE'] - 1
                tar_content['data'] = json.dumps(data_chunk)
                yield tar_content
                data_chunk = list()
                start_age = line['_AGE']
            data_chunk.append(line)
            cur_age = line['_AGE']
        tar_content['age'] = start_age
        tar_content['end_age'] = int(input_header.get('end_age', input_header['age']))
        tar_content['data'] = json.dumps(data_chunk)
        yield tar_content
    # Case 2 : Normal Document
    else:
        sorted_data = sorted(input_data, key=lambda line: line['_SEQ'])
        data_chunk, cur_seq = list(), ''
        for line in sorted_data:
            if not cur_seq:
                cur_seq = line['_SEQ']
            if line['_SEQ'] != cur_seq and data_chunk:
                tar_content['start_seq'] = cur_seq
                tar_content['data'] = json.dumps(data_chunk)
                yield tar_content
                data_chunk = list()
            data_chunk.append(line)
            cur_seq = line['_SEQ']
        tar_content['start_seq'] = cur_seq
        tar_content['data'] = json.dumps(data_chunk)
        yield tar_content

# Miscellaneous
def get_current_timestamp():
    return datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')

def get_sort_key_from_dict(doc_dict: dict):
    if 'age' in doc_dict:
        if 'merge_key' in doc_dict:
            return doc_dict['merge_key']
        else:
            return str(int(doc_dict['start_seq'])+int(doc_dict['age']))
    else:
        return doc_dict['deposit_at']

def get_merge_level(merge_key):
    hex_dict = {'2': 1, '4':2, '6':1, '8':3, 'a':1, 'c': 2, 'e': 1}
    prove = hashlib.md5(merge_key.encode()).hexdigest()
    zero_count = (len(prove) - len(prove.rstrip('0')))*4 + hex_dict.get(prove[0], 0)
    if zero_count < 3:
        return 0
    elif zero_count < 5:
        return 1
    elif zero_count < 7:
        return 2
    elif zero_count < 8:
        return 3
    elif zero_count < 9:
        return 4
    elif zero_count < 10:
        return 5
    elif zero_count < 11:
        return 6
    elif zero_count < 12:
        return 7
    else:
        return 8

