import json
import logging
from pyinsight import translator

class SapTranslator(translator.Translator):
    def __init__(self):
        super().__init__()
        self.spec_list = ['slt', 'ddic']
        self.line_oper = dict()

    def slt_line_translator(self, line: dict, age):
        line['_AGE'] = int(age)
        if 'IUUT_OPERAT_FLAG' not in line:
            line.pop('_RECNO')
        else:
            line['_NO'] = line.pop('_RECNO')
            line['_OP'] = line.pop('IUUT_OPERAT_FLAG')
        return line

    def ddic_line_transaltor(self, line: dict):
        return line

    # Archive Scope - data is a python dictionary list
    def get_archive_data(self, data, header):
        data_spec = header.get('data_spec', None)
        if data_spec == 'slt':
            data_age = header['age']
            return [self.slt_line_translator(line, data_age) for line in data]
        elif data_spec == 'ddic':
            return [self.ddic_line_transaltor(line) for line in data]
        else:
            logging.error("Data Spec {} is supported by SAP Translator".format(data_spec))
            return