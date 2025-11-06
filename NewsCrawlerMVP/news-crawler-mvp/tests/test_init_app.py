from src.init_app import MVPApp

def test_mvp_app_initialization():
    app = MVPApp()
    
    # Test that the repository is initialized
    assert app.repo is not None
    
    # Test that the fetcher is initialized
    assert app.fetcher is not None
    
    # Test that the adapters are initialized
    assert len(app.adapters) == 4  # Expecting 4 adapters for 444, Index, HVG, and Telex
    
    # Test that the embedder is initialized
    assert app.embedder is not None
    
    # Test that the pipeline is initialized
    assert app.pipeline is not None
    
    # Test that the search engine is initialized
    assert app.search_engine is not None

    # Optionally, check the names of the adapters to ensure they are correct
    adapter_names = [adapter.name() for adapter in app.adapters]
    expected_names = ["RegexArchiveAdapter<telex.hu>", 
                      "RegexArchiveAdapter<index.hu>", 
                      "RegexArchiveAdapter<444.hu>", 
                      "RegexArchiveAdapter<hvg.hu>"]
    assert adapter_names == expected_names