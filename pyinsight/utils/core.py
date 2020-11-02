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

# Remove all Null value of records
def remove_none(dict):
    return {key: value for key, value in dict.items() if value}

# Get dnf filter field set
def get_fields_from_filter(ndf_filters):
    return set([x[0] for l1 in ndf_filters for x in l1 if len(x)>0])

def get_needed_fields_from_filter(nfs_filters):
    return dict()

# disjunctive normal form filters (DNF)
def _filter_dnf(line: dict, ndf_filters):
    return any([all([oper.get(l2[1])(line.get(l2[0],None),l2[2]) for l2 in l1 if len(l2)>0]) for l1 in ndf_filters])

def filter_table_dnf(dict_list, ndf_filters):
    return [line for line in dict_list if _filter_dnf(line, ndf_filters)]

# Dictionary Related Operation
# Insight Internal Column
INSIGHT_FIELDS = ['_AGE', '_SEQ', '_NO', '_OP']

# retrieve list of keys from
def _filter_column(line: dict, field_list):
    return {key: value for key, value in line.items() if key in field_list}

def filter_table_column(dict_list: list, field_list):
    if field_list:
        field_list.extend(INSIGHT_FIELDS)
    return [_filter_column(line, field_list) for line in dict_list]

# Field_list + Filter solution => Apply to table
def filter_table(dict_list: list, field_list=list(), filter_list=list(list(list()))):
    if (not filter_list or filter_list == list(list(list()))) and not field_list:
        return dict_list
    elif not filter_list or filter_list == list(list(list())):
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
X_I_HEADER = ['topic_id', 'table_id', 'aged', 'start_seq', 'age', 'end_age',
              'data_encode', 'data_format', 'data_store', 'data_spec']
def get_data_dict_by_field(input_data, field_name):
    result_dict = dict()
    for line in input_data:
        if line[field_name] in result_dict:
            result_dict[line[field_name]].append(line)
        else:
            result_dict[line[field_name]] = [line]
        # result_dict[line[field_name]] = result_dict.get(line[field_name], list()).append(line)
    return result_dict

def get_data_chunk(input_data: list, input_header: dict, merge_size=MERGE_SIZE):
    # Prepare a clent X-I Header
    content = dict()
    total_line, line_counter = len(input_data), 0
    for key in [k for k in X_I_HEADER if k in input_header]:
        content[key] = input_header[key]
    # Case 1 : Aged Document => Must be cut age by age
    if 'age' in content:
        result_dict = get_data_dict_by_field(input_data, '_AGE')
        start_age = int(input_header.get('min_age', input_header['age']))
        end_age = int(input_header.get('end_age', input_header['age']))
        if start_age not in result_dict:
            result_dict[start_age] = []
        key_list = sorted([k for k in result_dict])
        data_chunk, chunk_size, from_age = list(), 0, start_age
        for key in key_list:
            to_age = key
            if not result_dict[key]:
                continue
            data_chunk.extend(result_dict[key])
            chunk_size += len(gzip.compress(json.dumps(result_dict[key]).encode()))
            line_counter += len(result_dict[key])
            if chunk_size >= merge_size:
                content['age'] = from_age
                if total_line == line_counter:
                    to_age = end_age
                content['end_age'] = to_age
                content['data'] = encoder(json.dumps(data_chunk), 'flat', content['data_encode'])
                data_chunk, chunk_size, from_age = list(), 0, to_age + 1
                yield content
        if data_chunk:
            content['age'] = from_age
            content['end_age'] = end_age
            content['data'] = encoder(json.dumps(data_chunk), 'flat', content['data_encode'])
            yield content
    # Case 2; Normal Document
    else:
        result_dict = get_data_dict_by_field(input_data, '_SEQ')
        key_list = sorted([k for k in result_dict])
        data_chunk, chunk_size, start_seq = list(), 0, ''
        for key in key_list:
            start_seq = key
            if not result_dict[key]:
                continue
            data_chunk.extend(result_dict[key])
            chunk_size += len(gzip.compress(json.dumps(result_dict[key]).encode()))
            if chunk_size >= merge_size:
                content['start_seq'] = start_seq
                content['data'] = encoder(json.dumps(data_chunk), 'flat', content['data_encode'])
                data_chunk, chunk_size = list(), 0
                yield content
        if data_chunk:
            content['start_seq'] = start_seq
            content['data'] = encoder(json.dumps(data_chunk), 'flat', content['data_encode'])
            yield content

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

