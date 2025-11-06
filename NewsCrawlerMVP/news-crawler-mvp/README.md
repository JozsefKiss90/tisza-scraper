# News Crawler MVP

## Overview
The News Crawler MVP is a Python-based application designed to aggregate news articles from various sources, including 444, Index, HVG, and Telex. It provides a unified interface for crawling, storing, and searching news articles, making it easier to access and analyze news content.

## Features
- **Article Aggregation**: Collects articles from multiple news sources using customizable adapters.
- **SQLite Repository**: Stores articles in a SQLite database for easy retrieval and management.
- **Search Engine**: Allows users to search for articles based on keywords, dates, and labels.
- **Pipeline Architecture**: Facilitates the end-to-end process of crawling, saving, and processing articles.

## Installation
To set up the project, follow these steps:

1. Clone the repository:
   ```
   git clone <repository-url>
   cd news-crawler-mvp
   ```

2. Create a virtual environment (optional but recommended):
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

## Usage
To run the application, execute the following command:

```
python src/init_app.py
```

This will initialize the application, crawl articles, and store them in the SQLite database.

## Running Tests
To ensure that the application is functioning correctly, run the tests using:

```
pytest tests/test_init_app.py
```

## Contributing
Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for more details.