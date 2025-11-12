# This file initializes the `adapters` subpackage and imports the necessary adapter classes.

from .regex_archive_adapter import RegexArchiveAdapter
from .factories import make_telex_adapter, make_index_adapter, make_444_adapter, make_hvg_adapter