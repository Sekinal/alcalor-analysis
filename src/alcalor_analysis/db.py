"""
Database connection and utilities.

Usage:
    1. Start SSH tunnel: ssh -L 15432:localhost:5432 ServaRicaVDSScrapers -N
    2. Use the query functions or get_connection() for custom queries
"""

import os
from functools import lru_cache
from typing import Optional

import polars as pl
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError(
        "DATABASE_URL not set. Create a .env file with:\n"
        "DATABASE_URL=postgresql://scraper:password@localhost:15432/news_scrapers"
    )


def query(sql: str) -> pl.DataFrame:
    """Execute a SQL query and return a Polars DataFrame."""
    return pl.read_database_uri(sql, DATABASE_URL)


@lru_cache(maxsize=1)
def get_stats() -> dict:
    """Get basic database statistics (cached)."""
    df = query("""
        SELECT
            COUNT(*) as total_articles,
            MIN(publication_date) as earliest_date,
            MAX(publication_date) as latest_date,
            COUNT(DISTINCT section) as unique_sections,
            AVG(LENGTH(body))::int as avg_body_length
        FROM articles
    """)
    return df.to_dicts()[0]


def get_connection():
    """Get the database URL for direct connections."""
    return DATABASE_URL


def search(keyword: str, limit: int = 100) -> pl.DataFrame:
    """Full-text search in article bodies (Spanish)."""
    return query(f"""
        SELECT
            title, section, publication_date, author,
            LEFT(body, 300) as preview
        FROM articles
        WHERE to_tsvector('spanish', body) @@ plainto_tsquery('spanish', '{keyword}')
        ORDER BY publication_date DESC
        LIMIT {limit}
    """)


def load_articles(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    section: Optional[str] = None,
    limit: Optional[int] = None,
) -> pl.DataFrame:
    """Load articles with optional filters."""
    conditions = ["publication_date IS NOT NULL"]

    if start_date:
        conditions.append(f"publication_date >= '{start_date}'")
    if end_date:
        conditions.append(f"publication_date <= '{end_date}'")
    if section:
        conditions.append(f"section = '{section}'")

    where_clause = " AND ".join(conditions)
    limit_clause = f"LIMIT {limit}" if limit else ""

    return query(f"""
        SELECT
            id, article_id, url, title, subtitle, section, author,
            location, publication_date, body, keywords, scraped_at
        FROM articles
        WHERE {where_clause}
        ORDER BY publication_date DESC
        {limit_clause}
    """)
