import os
import json
import pytest
from pyinsight.utils.core import *

class TestFilters():
    def test_dnf_01(self):
        filters_01 = [[["gender", "=", "Male"], ["height", ">=", 175], ['weight', "<", 80]]]
        filters_02 = [[["gender", "=", "Female"], ["height", ">", 175], ['weight', "<=", 80]]]
        filters_03 = [[["gender", "=", "Male"], ["height", ">=", 175], ['weight', "<", 80]],
                      [["gender", "=", "Female"], ["height", ">", 175], ['weight', "<=", 80]]]
        with open(os.path.join('.', 'input', 'person_simple', '000001.json'), 'r') as f:
            raw_data = json.load(f)
            result_01 = (filter_table_dnf(raw_data, filters_01))
            result_02 = (filter_table_dnf(raw_data, filters_02))
            result_03 = (filter_table_dnf(raw_data, filters_03))
            assert len(result_01) + len(result_02) == len(result_03)

    def test_dnf_02(self):
        filters_01 = [[["birthday", ">=", "1980-01-01"], ['weight', "<=", 80]]]
        filters_02 = [[["birthday", ">=", "1980-01-01"]]]
        filters_03 = [[['weight', "<=", 80]]]
        filters_04 = [[["birthday", ">=", "1980-01-01"]], [['weight', "<=", 80]]]
        with open(os.path.join('.', 'input', 'person_simple', '000001.json'), 'r') as f:
            raw_data = json.load(f)
            result_01 = (filter_table_dnf(raw_data, filters_01))
            result_02 = (filter_table_dnf(raw_data, filters_02))
            result_03 = (filter_table_dnf(raw_data, filters_03))
            result_04 = (filter_table_dnf(raw_data, filters_04))
            assert len(result_01) + len(result_04) == len(result_02) + len(result_03)

    def test_field_list_01(self):
        filters_01 = [[["height", ">=", 175], ['weight', "<=", 80]]]
        field_list_01 = ['id', 'first_name', 'email']
        with open(os.path.join('.', 'input', 'person_simple', '000001.json'), 'r') as f:
            raw_data = json.load(f)
            result_01 = filter_table_dnf(raw_data, filters_01)
            result_02 = filter_table_column(result_01, field_list_01)
        assert len(result_01) == len(result_02)

    def test_filter_01(self):
        filters_01 = [[["height", ">=", 175], ['weight', "<=", 80]]]
        field_list_01 = ['id', 'first_name', 'email']
        with open(os.path.join('.', 'input', 'person_simple', '000001.json'), 'r') as f:
            raw_data = json.load(f)
            result_01 = filter_table(raw_data)
            assert result_01 == raw_data
            result_02 = filter_table(raw_data, field_list_01)
            assert len(json.dumps(result_02)) == len(json.dumps(filter_table_column(raw_data, field_list_01)))
            result_03 = filter_table(raw_data, filter_list=filters_01)
            assert len(json.dumps(result_03)) == len(json.dumps(filter_table_dnf(raw_data, filters_01)))
            result_04 = filter_table(raw_data, field_list_01, filters_01)
            assert len(json.dumps(result_04)) == len(json.dumps(filter_table(raw_data, field_list_01, filters_01)))

    def test_get_fields_filter(self):
        filters_03 = [[["gender", "=", "Male"], ["height", ">=", 175], ['weight', "<", 80]],
                      [["birthday", ">=", "1980-01-01"]]]
        field_list = get_fields_from_filter(filters_03)
        assert 'weight' in field_list
        assert 'height' in field_list
        assert 'birthday' in field_list
        assert 'gender' in field_list

    def test_remove_null(self):
        raw_data_with_null = list()
        with open(os.path.join('.', 'input', 'person_simple', '000001.json'), 'r') as f:
            raw_data = json.load(f)
        json_raw_data = json.dumps(raw_data)
        for line in raw_data:
            if 'email' not in line:
                line['email'] = None
        json_data_with_null = json.dumps(raw_data)
        assert json_data_with_null != json_raw_data
        raw_data = json.loads(json_data_with_null, object_hook=remove_none)
        json_data_without_null = json.dumps(raw_data)
        assert json_data_without_null == json_raw_data

class TestEncoder():
    def test_encoder_01(self):
        with open(os.path.join('.', 'input', 'person_simple', '000001.json'), 'r') as f:
            flat_data = f.read()
        gzip_data = encoder(flat_data, 'flat', 'gzip')
        b64g_data = encoder(flat_data, 'flat', 'b64g')
        assert flat_data == encoder(gzip_data, 'gzip', 'flat')
        assert flat_data == encoder(b64g_data, 'b64g', 'flat')
        assert b64g_data == encoder(gzip_data, 'gzip', 'b64g')
        assert gzip_data == encoder(b64g_data, 'b64g', 'gzip')

class TestInsightStandard():
    pass

class TestMiscellaneous():
    def test_get_timestamp(self):
        last_t = ''
        for x in range(10000):
            current_t = get_current_timestamp()
            assert last_t < current_t
            last_t = current_t


if __name__=='__main__':
    fi = TestFilters()
    fi.test_remove_null()
