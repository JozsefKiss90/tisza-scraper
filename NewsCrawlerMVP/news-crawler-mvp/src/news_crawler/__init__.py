"""
This package provides the core functionality for the news crawler application.
It includes components for fetching articles, storing them in a database,
searching through them, and processing them through a pipeline.
"""

# news_crawler/__init__.py
from .core import NewsCrawlerMVP as MVPApp, NewsCrawlerMVP
from .repository import Repository
from .search import SearchEngine
from .pipeline import Pipeline
from .adapters import make_telex_adapter, make_index_adapter, make_444_adapter, make_hvg_adapter