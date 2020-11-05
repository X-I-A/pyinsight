import json
import logging
import traceback
from functools import wraps
import pyinsight
from pyinsight.insight import Insight
from pyinsight.utils.exceptions import InsightTypeError
from pyinsight.utils.core import encoder, MERGE_SIZE, PACKAGE_SIZE
from pyinsight.messager.messagers.dummy_messager import DummyMessager
from pyinsight.depositor.depositors import FileDepositor
from pyinsight.archiver.archivers import FileArchiver

__all__ = ['Action']


def backlog(func):
    """

    :param func:
    :return: boolean - when False, the data will be sent to backlog to furthur analyse
    """
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
                    'kwargs': kwargs,
                    'trace': traceback.format_exc()}
            if a.messager.blob_support:
                a.messager.publish(a.messager.topic_backlog, header, encoder(json.dumps(body), 'flat', 'gzip'))
            else:
                a.messager.publish(a.messager.topic_backlog, header, encoder(json.dumps(body), 'flat', 'b64g'))
    return wrapper


class Action(Insight):
    """
    Messager : Send / Parse Message - Default : Local Filesystem based
    Depositor : Document Management System - Default : Local Filesystem based
    Archive : Package Management System - Default : Local Filesystem based
    """
    def __init__(self, messager=None, depositor=None, archiver=None):
        super().__init__()
        self.merge_size = MERGE_SIZE
        self.package_size = PACKAGE_SIZE

        self.log_context = {'context': 'init'}
        self.logger = logging.getLogger("Insight.Action")

        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

        if not messager:
            self.messager = DummyMessager()
        elif isinstance(messager, pyinsight.messager.Messager):
            self.messager = messager
        else:
            self.logger.error("The Choosen Messenger has a wrong Type", extra=self.log_context)
            raise InsightTypeError("INS-000005")

        if not depositor:
            self.depositor = FileDepositor()
        elif isinstance(depositor, pyinsight.depositor.Depositor):
            self.depositor = depositor
        else:
            self.logger.error("The Choosen Depositor has a wrong Type", extra=self.log_context)
            raise InsightTypeError("INS-000006")

        if not archiver:
            self.archiver = FileArchiver()
        elif isinstance(archiver, pyinsight.archiver.Archiver):
            self.archiver = archiver
        else:
            self.logger.error("The Choosen Archiver has a wrong Type", extra=self.log_context)
            raise InsightTypeError("INS-000007")


    def set_merge_size(self, merge_size):
        self.merge_size = merge_size

    def set_package_size(self, package_size):
        self.package_size = package_size