from .utils import *

# Module Import
from insight import action
from insight import transfer
from insight import dispatcher
from insight import receiver
from insight import loader
from insight import cleaner
from insight import merger
from insight import packager

# Object Import
from insight.action import Action
from insight.transfer import Transfer
from insight.dispatcher import Dispatcher
from insight.receiver import Receiver
from insight.loader import Loader
from insight.cleaner import Cleaner
from insight.merger import Merger
from insight.packager import Packager

# Element Listing
__all__ = action.__all__ \
        + transfer.__all__ \
        + dispatcher.__all__ \
        + receiver.__all__ \
        + loader.__all__ \
        + cleaner.__all__ \
        + merger.__all__ \
        + packager.__all__

VERSION = (0, 0, 1)

def get_version():
    """Return the VERSION as a string.
    For example, if `VERSION == (0, 10, 7)`, return '0.10.7'.
    """
    return ".".join(map(str, VERSION))

__version__ = get_version()