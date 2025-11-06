from src.NewsCrawlerMVP import (
    make_telex_adapter,
    make_index_adapter,
    make_444_adapter,
    make_hvg_adapter,
    Repository,
    Pipeline,
    EmbedderClassifier,
    SearchEngine,
)

def init_app(db_path: str = "news.sqlite"):
    fetcher = Fetcher()
    adapters = [
        make_telex_adapter(fetcher),
        make_index_adapter(fetcher),
        make_444_adapter(fetcher),
        make_hvg_adapter(fetcher),
    ]
    repo = Repository(db_path)
    embedder = EmbedderClassifier(repo)
    pipeline = Pipeline(adapters, repo, embedder)
    search_engine = SearchEngine(repo)

    return {
        "fetcher": fetcher,
        "adapters": adapters,
        "repository": repo,
        "embedder": embedder,
        "pipeline": pipeline,
        "search_engine": search_engine,
    }

if __name__ == "__main__":
    app_components = init_app()
    print("Application components initialized:", app_components)