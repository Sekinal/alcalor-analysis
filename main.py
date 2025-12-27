"""
Alcalor Político News Analysis

Usage:
1. Start SSH tunnel: ssh -L 5432:localhost:5432 ServaRicaVDSScrapers -N
2. Run: uv run python main.py
"""

import os
import polars as pl
import duckdb
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


def load_articles(limit: int | None = None) -> pl.DataFrame:
    """Load articles from PostgreSQL using connectorx (fast!)."""
    query = """
        SELECT
            id, article_id, url, title, subtitle, section, author,
            location, publication_date, body, keywords, scraped_at
        FROM articles
        ORDER BY publication_date DESC
    """
    if limit:
        query += f" LIMIT {limit}"

    return pl.read_database_uri(query, DATABASE_URL)


def get_stats() -> dict:
    """Get basic database statistics."""
    stats_query = """
        SELECT
            COUNT(*) as total_articles,
            MIN(publication_date) as earliest_date,
            MAX(publication_date) as latest_date,
            COUNT(DISTINCT section) as unique_sections,
            AVG(LENGTH(body)) as avg_body_length
        FROM articles
    """
    df = pl.read_database_uri(stats_query, DATABASE_URL)
    return df.to_dicts()[0]


def articles_by_year() -> pl.DataFrame:
    """Get article counts by year."""
    query = """
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) as articles
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """
    return pl.read_database_uri(query, DATABASE_URL)


def articles_by_section() -> pl.DataFrame:
    """Get article counts by section."""
    query = """
        SELECT
            section,
            COUNT(*) as articles
        FROM articles
        WHERE section IS NOT NULL
        GROUP BY section
        ORDER BY articles DESC
    """
    return pl.read_database_uri(query, DATABASE_URL)


def search_articles(keyword: str, limit: int = 100) -> pl.DataFrame:
    """Full-text search in article bodies."""
    query = f"""
        SELECT
            title, section, publication_date,
            LEFT(body, 200) as body_preview
        FROM articles
        WHERE to_tsvector('spanish', body) @@ plainto_tsquery('spanish', '{keyword}')
        ORDER BY publication_date DESC
        LIMIT {limit}
    """
    return pl.read_database_uri(query, DATABASE_URL)


def export_to_duckdb(output_path: str = "articles.duckdb", limit: int | None = None):
    """Export articles to local DuckDB for faster analysis."""
    print("Loading articles from PostgreSQL...")
    df = load_articles(limit)

    print(f"Exporting {len(df)} articles to DuckDB...")
    con = duckdb.connect(output_path)
    con.execute("CREATE OR REPLACE TABLE articles AS SELECT * FROM df")
    con.close()

    print(f"Saved to {output_path}")
    return output_path


def analyze_with_duckdb(db_path: str = "articles.duckdb"):
    """Run analysis on local DuckDB file."""
    con = duckdb.connect(db_path)

    print("\n=== Article Stats ===")
    print(con.execute("""
        SELECT
            COUNT(*) as total,
            MIN(publication_date) as earliest,
            MAX(publication_date) as latest
        FROM articles
    """).fetchdf())

    print("\n=== Top Sections ===")
    print(con.execute("""
        SELECT section, COUNT(*) as count
        FROM articles
        GROUP BY section
        ORDER BY count DESC
        LIMIT 10
    """).fetchdf())

    print("\n=== Articles per Year ===")
    print(con.execute("""
        SELECT
            YEAR(publication_date) as year,
            COUNT(*) as articles
        FROM articles
        GROUP BY year
        ORDER BY year
    """).fetchdf())

    con.close()


if __name__ == "__main__":
    print("Connecting to database...")

    # Get basic stats
    stats = get_stats()
    print(f"\n=== Database Stats ===")
    print(f"Total articles: {stats['total_articles']:,}")
    print(f"Date range: {stats['earliest_date']} to {stats['latest_date']}")
    print(f"Unique sections: {stats['unique_sections']}")
    print(f"Avg body length: {stats['avg_body_length']:.0f} chars")

    # Articles by year
    print(f"\n=== Articles by Year ===")
    yearly = articles_by_year()
    print(yearly)

    # Top sections
    print(f"\n=== Top 10 Sections ===")
    sections = articles_by_section().head(10)
    print(sections)
