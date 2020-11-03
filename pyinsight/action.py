import json
import logging
from functools import wraps
import pyinsight
from pyinsight.utils.exceptions import *
from pyinsight.utils.core import MERGE_SIZE, PACKAGE_SIZE, LOGGING_LEVEL
from pyinsight.messager.messagers import DummyMessager
from pyinsight.depositor.depositors import FileDepositor
from pyinsight.archiver.archivers import FileArchiver
from pyinsight.translator.translators import SapTranslator, XIATranslator

def backlog(func):
    @wraps(func)
    def wrapper(a, *args, **kwargs):
        try:
            return func(a, *args, **kwargs)
        except Exception as e:
            header = {'action_type': a.__class__.__name__,
                      'function': func.__name__,
                      'exception_type': e.__class__.__name__,
                      'exception_msg': format(e)}
            body = {'args': args,
                    'kwargs': kwargs}
            a.messager.publish(a.messager.topic_backlog, header, body)
    return wrapper


__all__ = ['Action']

class Action():
    """
    Messager : Send / Parse Message - Default : Local Filesystem based
    Depositor : Document Management System - Default : Local Filesystem based
    Archive : Package Management System - Default : Local Filesystem based
    Translators : A list of customized translator to change the data_spec to xia
    """
    def __init__(self, messager=None, depositor=None, archiver=None, translators=list()):
        self.merge_size = MERGE_SIZE
        self.package_size = PACKAGE_SIZE

        self.log_context = {'context': 'init'}
        self.logger = logging.getLogger("Insight.Action")

        if self.logger.hasHandlers():
            self.logger.handlers.clear()
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(funcName)s-%(levelname)s-%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        self.logger.setLevel(LOGGING_LEVEL)

        if not messager:
            self.messager = DummyMessager()
        elif isinstance(messager, pyinsight.messager.Messager):
            self.messager = messager
        else:
            self.logger.error("The Choosen Messenger has a wrong Type", extra=self.log_context)
            raise InsightTypeError

        if not depositor:
            self.depositor = FileDepositor()
        elif isinstance(depositor, pyinsight.depositor.Depositor):
            self.depositor = depositor
        else:
            self.logger.error("The Choosen Depositor has a wrong Type", extra=self.log_context)
            raise InsightTypeError

        if not archiver:
            self.archiver = FileArchiver()
        elif isinstance(archiver, pyinsight.archiver.Archiver):
            self.archiver = archiver
        else:
            self.logger.error("The Choosen Archiver has a wrong Type", extra=self.log_context)
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
                self.logger.error("The Choosen Translator has a wrong Type", extra=self.log_context)
                raise InsightTypeError

    def set_merge_size(self, merge_size):
        self.merge_size = merge_size

    def set_package_size(self, package_size):
        self.package_size = package_size