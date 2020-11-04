from pyinsight.utils.exceptions import InsightDataSpecError

def x_i_header_check(header):
    if not all(k in header for k in ['topic_id', 'table_id']):
        raise InsightDataSpecError("INS-000001")
    if not all(k in header for k in ['data_encode', 'data_format', 'data_store']):
        raise InsightDataSpecError("INS-000002")
    return True

def x_i_data_store_check(header, data):
    if header['data_store'] not in ['body', 'file']:
        raise InsightDataSpecError("INS-000004")

def x_i_proto_check(header, data):
    x_i_header_check(header)
    return True
