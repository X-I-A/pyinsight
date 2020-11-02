import json
from pyinsight import translator

class XIATranslator(translator.Translator):
    def __init__(self):
        super().__init__()
        self.spec_list = ['x-i-a']

    def age_line_translator(self, line: dict, age):
        line['_AGE'] = age
        return line

    def normal_line_translator(self, line: dict, start_seq):
        line['_SEQ'] = start_seq
        return line

    # Depositor Scope - Data format = record
    def get_depositor_data(self, data, src_encode, tar_encode, header):
        data_spec = header.get('data_spec', None)
        if data_spec == 'x-i-a':
            return self.encoder(data, src_encode, tar_encode)
        else:
            record_data = json.loads(self.encoder(data, src_encode, 'flat'))
            translated_data = self.get_archive_data(record_data, header)
            return self.encoder(json.dumps(translated_data), 'flat', tar_encode)

    # Archive Scope 
    def get_archive_data(self, data: list, header):
        data_spec = header.get('data_spec', None)
        # Case 1: x-i-a type : Already well-formatted
        if data_spec == 'x-i-a':
            return data
        # Case 2: Header -> No modification
        elif int(header.get('age', 0)) == 1:
            return data
        # Case 3: Standard Aged Document
        elif 'age' in header:
            return [self.age_line_translator(line, int(header['age'])) for line in data]
        # Case 4: Stand Normal Documnet
        elif 'start_seq' in header:
            return [self.normal_line_translator(line, header['start_seq']) for line in data]
