import os
import json
import gzip
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

if __name__=='__main__':
    create_saved_doc()
    # generate_person_complex()
    # generate_person_simple()


