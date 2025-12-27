"""
Political analysis queries.

Analyze Mexican politics:
- Political party coverage (PRI, PAN, PRD, Morena, PVEM, MC)
- Presidential mentions (Fox → Sheinbaum)
- Veracruz governors (Herrera → Nahle)
- Election cycles
"""

import polars as pl
from ..db import query

# =============================================================================
# REFERENCE DATA
# =============================================================================

VERACRUZ_GOVERNORS = {
    "Fidel Herrera": ("2004-12-01", "2010-11-30", "PRI"),
    "Javier Duarte": ("2010-12-01", "2016-10-12", "PRI"),  # Left early
    "Flavino Ríos": ("2016-10-12", "2016-11-30", "PRI"),  # Interim
    "Miguel Ángel Yunes": ("2016-12-01", "2018-11-30", "PAN"),
    "Cuitláhuac García": ("2018-12-01", "2024-11-30", "Morena"),
    "Rocío Nahle": ("2024-12-01", "2030-11-30", "Morena"),
}

MEXICAN_PRESIDENTS = {
    "Vicente Fox": ("2000-12-01", "2006-11-30", "PAN"),
    "Felipe Calderón": ("2006-12-01", "2012-11-30", "PAN"),
    "Enrique Peña Nieto": ("2012-12-01", "2018-11-30", "PRI"),
    "AMLO": ("2018-12-01", "2024-09-30", "Morena"),
    "Claudia Sheinbaum": ("2024-10-01", "2030-09-30", "Morena"),
}


# =============================================================================
# PARTY ANALYSIS
# =============================================================================

def party_mentions_by_year() -> pl.DataFrame:
    """Compare mentions of major political parties over time."""
    return query("""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%morena%'
                AND LOWER(body) NOT LIKE '%morena valle%') as morena,
            COUNT(*) FILTER (WHERE body LIKE '% PRI %'
                OR body LIKE '%,PRI,%' OR body LIKE '%(PRI)%'
                OR body LIKE 'PRI %' OR body LIKE '% PRI.%'
                OR body LIKE '% PRI,%') as pri,
            COUNT(*) FILTER (WHERE body LIKE '% PAN %'
                OR body LIKE '%,PAN,%' OR body LIKE '%(PAN)%'
                OR body LIKE 'PAN %' OR body LIKE '% PAN.%'
                OR body LIKE '% PAN,%') as pan,
            COUNT(*) FILTER (WHERE body LIKE '% PRD %'
                OR body LIKE '%,PRD,%' OR body LIKE '%(PRD)%'
                OR body LIKE 'PRD %' OR body LIKE '% PRD.%'
                OR body LIKE '% PRD,%') as prd,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%partido verde%'
                OR body LIKE '% PVEM %' OR body LIKE '%(PVEM)%') as pvem,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%movimiento ciudadano%'
                OR body LIKE '% MC %') as mc
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


def party_mentions_by_month() -> pl.DataFrame:
    """Monthly party mentions for detailed trend analysis."""
    return query("""
        SELECT
            DATE_TRUNC('month', publication_date)::date as month,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%morena%'
                AND LOWER(body) NOT LIKE '%morena valle%') as morena,
            COUNT(*) FILTER (WHERE body LIKE '% PRI %'
                OR body LIKE '%(PRI)%') as pri,
            COUNT(*) FILTER (WHERE body LIKE '% PAN %'
                OR body LIKE '%(PAN)%') as pan,
            COUNT(*) FILTER (WHERE body LIKE '% PRD %'
                OR body LIKE '%(PRD)%') as prd
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """)


# =============================================================================
# PRESIDENT ANALYSIS
# =============================================================================

def president_mentions_by_year() -> pl.DataFrame:
    """Track mentions of Mexican presidents by year."""
    return query("""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%vicente fox%'
                OR (LOWER(body) LIKE '%fox%'
                    AND LOWER(body) LIKE '%presidente%')) as fox,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%calderón%'
                OR LOWER(body) LIKE '%calderon%'
                OR LOWER(body) LIKE '%felipe calderón%') as calderon,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%peña nieto%'
                OR LOWER(body) LIKE '%pena nieto%'
                OR LOWER(body) LIKE '%epn%') as pena_nieto,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%lópez obrador%'
                OR LOWER(body) LIKE '%lopez obrador%'
                OR LOWER(body) LIKE '%amlo%') as amlo,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%sheinbaum%'
                OR LOWER(body) LIKE '%claudia sheinbaum%') as sheinbaum
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


def president_mentions_by_month() -> pl.DataFrame:
    """Monthly presidential mentions for transition analysis."""
    return query("""
        SELECT
            DATE_TRUNC('month', publication_date)::date as month,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%calderón%'
                OR LOWER(body) LIKE '%calderon%') as calderon,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%peña nieto%'
                OR LOWER(body) LIKE '%pena nieto%') as pena_nieto,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%lópez obrador%'
                OR LOWER(body) LIKE '%lopez obrador%'
                OR LOWER(body) LIKE '%amlo%') as amlo,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%sheinbaum%') as sheinbaum
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """)


# =============================================================================
# GOVERNOR ANALYSIS
# =============================================================================

def governor_mentions_by_year() -> pl.DataFrame:
    """Track mentions of Veracruz governors."""
    return query("""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%fidel herrera%'
                OR (LOWER(body) LIKE '%herrera%'
                    AND LOWER(body) LIKE '%gobernador%')) as fidel_herrera,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%javier duarte%'
                OR LOWER(body) LIKE '%duarte%') as javier_duarte,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%yunes%'
                OR LOWER(body) LIKE '%miguel ángel yunes%') as yunes,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%cuitláhuac%'
                OR LOWER(body) LIKE '%cuitlahuac%') as cuitlahuac,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%rocío nahle%'
                OR LOWER(body) LIKE '%rocio nahle%'
                OR LOWER(body) LIKE '%nahle%') as nahle
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


# =============================================================================
# ELECTION ANALYSIS
# =============================================================================

def election_coverage_by_year() -> pl.DataFrame:
    """Analyze election-related coverage by year."""
    return query("""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) as total_articles,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%elección%'
                OR LOWER(body) LIKE '%eleccion%'
                OR LOWER(body) LIKE '%electorales%') as election_mentions,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%candidat%') as candidate_mentions,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%campaña%'
                OR LOWER(body) LIKE '%campana%') as campaign_mentions,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%voto%'
                OR LOWER(body) LIKE '%votar%'
                OR LOWER(body) LIKE '%votación%') as voting_mentions,
            CASE
                WHEN EXTRACT(YEAR FROM publication_date)::int IN (2006, 2012, 2018, 2024)
                THEN 'Presidencial'
                WHEN EXTRACT(YEAR FROM publication_date)::int IN (2010, 2016)
                THEN 'Gubernatorial'
                WHEN EXTRACT(YEAR FROM publication_date)::int IN (2009, 2015, 2021)
                THEN 'Intermedia'
                ELSE 'No electoral'
            END as election_type
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


def term_mentions(term: str, granularity: str = "year") -> pl.DataFrame:
    """Track any political term over time."""
    date_fn = "EXTRACT(YEAR FROM publication_date)::int" if granularity == "year" else \
              "DATE_TRUNC('month', publication_date)::date"

    return query(f"""
        SELECT
            {date_fn} as period,
            COUNT(*) as mentions
        FROM articles
        WHERE LOWER(body) LIKE LOWER('%{term}%')
            AND publication_date IS NOT NULL
        GROUP BY period
        ORDER BY period
    """)
