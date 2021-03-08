import os
import json
import gzip
import base64
from xialib import BasicTranslator

"""
def generate_person_simple():
    url = 'https://my.api.mockaroo.com/person_simple.json?key=3ec0bed0'
    for x in range(3, 52):
        r = requests.get(url)
        filename = str(x).zfill(6) + '.json'
        with open(os.path.join('..', 'input', 'test1', filename), 'wb') as f:
            f.write(r.content)

def generate_person_complex():
    url = 'https://my.api.mockaroo.com/person_complexe.json?key=3ec0bed0'
    for x in range(3, 52):
        r = requests.get(url)
        filename = str(x).zfill(6) + '.json'
        with open(os.path.join('..', 'input', 'test1', filename), 'wb') as f:
            f.write(r.content)
"""
def create_saved_doc():
    translator = BasicTranslator()

    with open(os.path.join('..', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        age_header = {'topic_id': 'test', 'table_id': 'aged_data',
                      'data_encode': 'gzip', 'data_format': 'record', 'data_spec': '', 'data_store': 'body',
                      'age': '2', 'end_age': '100', 'start_seq': '20201113222500000000'}
    translator.compile(age_header, data_body)
    age_data_body = [translator.get_translated_line(item, age=2) for item in data_body]
    gzipped = gzip.compress(json.dumps(age_data_body, ensure_ascii=False).encode())

    with open(os.path.join('..', 'input', 'insight_formats', '000002.gz'), 'wb') as f:
        f.write(gzipped)

def create_saved_header():
    with open(os.path.join('..', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        print(base64.b64encode(gzip.compress(json.dumps(data_header, ensure_ascii=False).encode())).decode())

def generate_routes():
    fields = ['id', 'first_name', 'last_name', 'height', 'children', 'lucky_numbers']
    filters1 = [[['gender', '=', 'Male'], ['height', '>=', 175]],
                [['gender', '=', 'Female'], ['weight', '<=', 100]]]
    filters2 = [[['gender', '=', 'Male'], ['height', '>', 175]],
                [['gender', '!=', 'Male'], ['weight', '<', 100]]]
    route1 = {"src_topic_id": "test", "src_table_id": "aged_data", "tar_topic_id": "t1", "tar_table_id": "aged"}
    route2 = {"src_topic_id": "test", "src_table_id": "aged_data", "tar_topic_id": "t2", "tar_table_id": "aged",
              "fields": fields}
    route3 = {"src_topic_id": "test", "src_table_id": "normal_data", "tar_topic_id": "t3", "tar_table_id": "aged",
              "filters": filters1}
    route4 = {"src_topic_id": "test", "src_table_id": "normal_data", "tar_topic_id": "t4", "tar_table_id": "aged",
              "fields": fields, "filters": filters1}
    with open("aged_data", "w") as fp:
        json.dump({"t1": [route1], "t2": [route2]}, fp)
    with open("normal_data", "w") as fp:
        json.dump({"t3": [route3], "t4": [route4]}, fp)

if __name__=='__main__':
    """"""
    generate_routes()
    # create_saved_header()
    # create_saved_doc()
    # generate_person_complex()
    # generate_person_simple()


