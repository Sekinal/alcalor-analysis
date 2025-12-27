"""
Temporal pattern analysis queries.

Analyze publishing and news patterns:
- Day of week patterns
- Seasonality
- Article volume trends
- Keyword evolution
"""

import polars as pl
from ..db import query


def articles_by_year() -> pl.DataFrame:
    """Total articles published by year."""
    return query("""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) as articles,
            AVG(LENGTH(body))::int as avg_length,
            COUNT(DISTINCT section) as sections_used
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


def articles_by_month() -> pl.DataFrame:
    """Monthly article counts for trend analysis."""
    return query("""
        SELECT
            DATE_TRUNC('month', publication_date)::date as month,
            COUNT(*) as articles
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """)


def day_of_week_patterns() -> pl.DataFrame:
    """Analyze publishing patterns by day of week."""
    return query("""
        SELECT
            EXTRACT(DOW FROM publication_date)::int as day_num,
            CASE EXTRACT(DOW FROM publication_date)::int
                WHEN 0 THEN 'Domingo'
                WHEN 1 THEN 'Lunes'
                WHEN 2 THEN 'Martes'
                WHEN 3 THEN 'Miércoles'
                WHEN 4 THEN 'Jueves'
                WHEN 5 THEN 'Viernes'
                WHEN 6 THEN 'Sábado'
            END as dia,
            COUNT(*) as articles,
            AVG(LENGTH(body))::int as avg_length
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY day_num
        ORDER BY day_num
    """)


def monthly_seasonality() -> pl.DataFrame:
    """Analyze seasonality by month of year."""
    return query("""
        SELECT
            EXTRACT(MONTH FROM publication_date)::int as month_num,
            CASE EXTRACT(MONTH FROM publication_date)::int
                WHEN 1 THEN 'Enero'
                WHEN 2 THEN 'Febrero'
                WHEN 3 THEN 'Marzo'
                WHEN 4 THEN 'Abril'
                WHEN 5 THEN 'Mayo'
                WHEN 6 THEN 'Junio'
                WHEN 7 THEN 'Julio'
                WHEN 8 THEN 'Agosto'
                WHEN 9 THEN 'Septiembre'
                WHEN 10 THEN 'Octubre'
                WHEN 11 THEN 'Noviembre'
                WHEN 12 THEN 'Diciembre'
            END as mes,
            COUNT(*) as articles,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%huracán%') as huracan,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%elección%') as eleccion
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY month_num
        ORDER BY month_num
    """)


def sections_distribution() -> pl.DataFrame:
    """Analyze article distribution by section."""
    return query("""
        SELECT
            COALESCE(section, 'Sin sección') as section,
            COUNT(*) as articles,
            MIN(publication_date) as first_article,
            MAX(publication_date) as last_article
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY section
        ORDER BY articles DESC
    """)


def keyword_frequency(limit: int = 50) -> pl.DataFrame:
    """Most common keywords across all articles."""
    return query(f"""
        SELECT
            UNNEST(keywords) as keyword,
            COUNT(*) as frequency
        FROM articles
        WHERE keywords IS NOT NULL
        GROUP BY keyword
        ORDER BY frequency DESC
        LIMIT {limit}
    """)


def keyword_by_year(keyword: str) -> pl.DataFrame:
    """Track a specific keyword's usage over time."""
    return query(f"""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) as mentions
        FROM articles
        WHERE '{keyword}' = ANY(keywords)
            AND publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


def trending_topics(start_date: str, end_date: str, limit: int = 20) -> pl.DataFrame:
    """Get top keywords for a specific time period."""
    return query(f"""
        SELECT
            UNNEST(keywords) as keyword,
            COUNT(*) as frequency
        FROM articles
        WHERE publication_date BETWEEN '{start_date}' AND '{end_date}'
            AND keywords IS NOT NULL
        GROUP BY keyword
        ORDER BY frequency DESC
        LIMIT {limit}
    """)
