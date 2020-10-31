import pytest
from pyinsight.utils.exceptions import *
from pyinsight.action import Action
from pyinsight.archiver.archivers import FileArchiver
from pyinsight.depositor.depositors import FileDepositor
from pyinsight.messager.messagers import DummyMessager
from pyinsight.translator import Translator
from pyinsight.translator.translators import SapTranslator

def test_init_messager_type_error():
    with pytest.raises(InsightTypeError):
        w = Action(messager=1)

def test_init_depositor_type_error():
    with pytest.raises(InsightTypeError):
        w = Action(depositor=1)

def test_init_archiver_type_error():
    with pytest.raises(InsightTypeError):
        w = Action(archiver=1)

def test_init_translator_type_error():
    with pytest.raises(InsightTypeError):
        w = Action(translators=[1])

def test_init_action():
    w = Action(DummyMessager(), FileDepositor(), FileArchiver(), [Translator(), SapTranslator()])
    assert w is not None