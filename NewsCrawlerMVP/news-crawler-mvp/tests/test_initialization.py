import unittest
from src.news_crawler.core import MVPApp
from src.news_crawler.adapters.factories import make_telex_adapter, make_index_adapter, make_444_adapter, make_hvg_adapter
from src.news_crawler.repository import Repository
from src.news_crawler.search import SearchEngine
from src.news_crawler.pipeline import Pipeline

class TestInitialization(unittest.TestCase):

    def setUp(self):
        self.app = MVPApp()

    def test_repository_initialization(self):
        self.assertIsInstance(self.app.repo, Repository)

    def test_adapters_initialization(self):
        self.assertEqual(len(self.app.adapters), 4)
        self.assertIsNotNone(self.app.adapters[0])  # Telex adapter
        self.assertIsNotNone(self.app.adapters[1])  # Index adapter
        self.assertIsNotNone(self.app.adapters[2])  # 444 adapter
        self.assertIsNotNone(self.app.adapters[3])  # HVG adapter

    def test_search_engine_initialization(self):
        self.assertIsInstance(self.app.search_engine, SearchEngine)

    def test_pipeline_initialization(self):
        self.assertIsInstance(self.app.pipeline, Pipeline)

if __name__ == '__main__':
    unittest.main()