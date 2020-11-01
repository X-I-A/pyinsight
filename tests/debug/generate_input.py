import os
import json
import requests
from pyinsight.utils.core import get_current_timestamp


def generate_person_simple():
    url = 'https://my.api.mockaroo.com/person_simple.json?key=3ec0bed0'
    for x in range(2, 51):
        r = requests.get(url)
        filename = str(x).zfill(6) + '.json'
        with open(os.path.join('.', 'input', 'person_simple', filename), 'w') as f:
            f.write(json.dumps(r.json()))

if __name__=='__main__':
    pass
    # generate_person_simple()


