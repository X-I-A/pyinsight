from .utils import *

# Module Import
from pyinsight import action
from pyinsight import transfer
from pyinsight import dispatcher
from pyinsight import receiver
from pyinsight import loader
from pyinsight import cleaner
from pyinsight import merger
from pyinsight import packager

# Object Import
from pyinsight.action import Action
from pyinsight.transfer import Transfer
from pyinsight.dispatcher import Dispatcher
from pyinsight.receiver import Receiver
from pyinsight.loader import Loader
from pyinsight.cleaner import Cleaner
from pyinsight.merger import Merger
from pyinsight.packager import Packager

# Element Listing
__all__ = action.__all__ \
        + transfer.__all__ \
        + dispatcher.__all__ \
        + receiver.__all__ \
        + loader.__all__ \
        + cleaner.__all__ \
        + merger.__all__ \
        + packager.__all__

VERSION = (0, 0, 4)

def get_version():
    """Return the VERSION as a string.
    For example, if `VERSION == (0, 10, 7)`, return '0.10.7'.
    """
    return ".".join(map(str, VERSION))

__version__ = get_version()