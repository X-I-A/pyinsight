import os
import gzip
import base64
import hashlib
import datetime
import logging

MERGE_SIZE = os.environ.get('INSIGHT_MERGE_SIZE', 3 ** 20)
PACKAGE_SIZE = os.environ.get('INSIGHT_PACKAGE_SIZE', 2 ** 25)

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

# disjunctive normal form application (DNF)
def filter_dnf(line, ndf_filters):
    return any([all([oper.get(l2[1])(line.get(l2[0],None),l2[2]) for l2 in l1 if len(l2)>0]) for l1 in ndf_filters])

# Get dnf filter field set
def get_fields_from_filter(ndf_filters):
    return set([x[0] for l1 in ndf_filters for x in l1 if len(x)>0])

# Dictionary Related Operation
# retrieve list of keys from
def filter_column(table_line, field_list):
    return {key: value for key, value in table_line.items() if key in field_list}

# Remove all Null values
def remove_none(dict):
    return {key: value for key, value in dict.items() if value}

# Allowed Transformation Matrix ['flat', 'b54g', 'gzip']
def encoder(data, src_encode, tar_encode):
    if src_encode == tar_encode:
        return data
    elif src_encode == 'gzip' and tar_encode == 'flat':
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

# Miscellous
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

