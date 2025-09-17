# Hotels Merge Crawler

This project is a web crawler designed to scrape hotel data as a part of a data merging initiative. The crawler collects information from various hotel booking websites and consolidates it into a unified format for easier comparison.
For the complete documentation, please visit this [repository](https://github.com/duylamasd/hotels-merge).

## Prequisites
- Python 3
- PostgreSQL

## How to run
- First, install the required packages:
  ```bash
  pip install -r requirements.txt
  ```

- Check if the database for the [API repository](https://github.com/duylamasd/hotels-merge) is configured. Then setup the `.env` file with the necessary environment variables for database connection, following the `.env.example` template. Make sure the database url is the same as the API repository.
- Run the crawler:
  ```bash
  python src/main.py
  ```


Alternatively, you can use Docker to run the crawler:
- Build the Docker image:
  ```bash
  docker build -t hotels-merge-crawler .
  ```
- Run the Docker container with the environment variables contained in the `.env` file:
  ```bash
  docker run --env-file ./.env hotels-merge-crawler
  ```
