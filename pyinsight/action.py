import os
import json
import logging
import pyinsight
from pyinsight.utils.core import MERGE_SIZE, PACKAGE_SIZE
from pyinsight.utils.exceptions import *
from pyinsight.messager.messagers import DummyMessager
from pyinsight.depositor.depositors import FileDepositor
from pyinsight.archiver.archivers import FileArchiver
from pyinsight.translator.translators import SapTranslator, XIATranslator


__all__ = ['Action']

"""
Messager : Send / Parse Message - Default : Local Filesystem based
Depositor : Document Management System - Default : Local Filesystem based
Archive : Package Management System - Default : Local Filesystem based
Translators : A list of customized translator to change the data_spec to xia
"""
class Action():
    """Receive Receive Message and Put it into Depositor, and trigger Merger"""
    logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s')
    def __init__(self, messager=None, depositor=None, archiver=None, translators=list()):
        self.merge_size = MERGE_SIZE
        self.package_size = PACKAGE_SIZE

        if not messager:
            self.messager = DummyMessager()
        elif isinstance(messager, pyinsight.messager.Messager):
            self.messager = messager
        else:
            logging.error("The Choosen Messenger has a wrong Type, Initialization Failed")
            raise InsightTypeError

        if not depositor:
            self.depositor = FileDepositor()
        elif isinstance(depositor, pyinsight.depositor.Depositor):
            self.depositor = depositor
        else:
            logging.error("The Choosen Depositor has a wrong Type, Initialization Failed")
            raise InsightTypeError

        if not archiver:
            self.archiver = FileArchiver()
        elif isinstance(archiver, pyinsight.archiver.Archiver):
            self.archiver = archiver
        else:
            logging.error("The Choosen Archiver has a wrong Type, Initialization Failed")
            raise InsightTypeError

        # Standard Translators
        self.translators = dict()
        xia_trans = XIATranslator()
        sap_trans = SapTranslator()
        for std_trans in [xia_trans, sap_trans]:
            for spec in std_trans.spec_list:
                self.translators[spec] = std_trans
        # Customized Translators (can overwrite standard ones)
        for cust_trans in translators:
            if isinstance(cust_trans, pyinsight.translator.Translator):
                for spec in cust_trans.spec_list:
                    self.translators[spec] = cust_trans
            else:
                logging.error("The Choosen Translator has a wrong Type, Initialization Failed")
                raise InsightTypeError

    def set_merge_size(self, merge_size):
        self.merge_size = merge_size

    def set_package_size(self, package_size):
        self.package_size = package_size