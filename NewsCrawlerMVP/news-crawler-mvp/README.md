# News Crawler MVP

## Overview
The News Crawler MVP is a Python-based application designed to aggregate news articles from various sources, including 444, Index, HVG, and Telex. The project aims to provide a unified interface for crawling, storing, and searching news articles, with the potential for further enhancements and customizations.

## Features
- **Crawling**: Automatically fetches articles from specified news sources.
- **Storage**: Utilizes SQLite for storing article data.
- **Search**: Implements a search engine with full-text search capabilities.
- **Extensibility**: Easily add new sources and customize the crawling logic.

## Installation
1. Clone the repository:
   ```
   git clone <repository-url>
   cd news-crawler-mvp
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage
To run the application, execute the following command:
```
python -m news_crawler.cli
```

### Example Commands
- Crawl articles from the last 10 years:
  ```
  python -m news_crawler.cli crawl --years 10
  ```

- Search for articles containing specific keywords:
  ```
  python -m news_crawler.cli search "keyword"
  ```

## Testing
To run the tests, use:
```
pytest tests/test_initialization.py
```

## Contributing
Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.