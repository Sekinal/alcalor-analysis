"""
Natural disasters and health crises analysis.

Track coverage of:
- Hurricanes (Veracruz is highly vulnerable)
- Floods and inundations
- Earthquakes
- Droughts
- COVID-19 pandemic
"""

import polars as pl
from ..db import query


# =============================================================================
# NATURAL DISASTERS
# =============================================================================

def disasters_by_type_yearly() -> pl.DataFrame:
    """Break down natural disaster coverage by type."""
    return query("""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%huracán%'
                OR LOWER(body) LIKE '%huracan%') as huracanes,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%inundación%'
                OR LOWER(body) LIKE '%inundacion%'
                OR LOWER(body) LIKE '%inunda%') as inundaciones,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%tormenta tropical%') as tormentas,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%sismo%'
                OR LOWER(body) LIKE '%terremoto%'
                OR LOWER(body) LIKE '%temblor%') as sismos,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%sequía%'
                OR LOWER(body) LIKE '%sequia%') as sequias,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%incendio forestal%'
                OR LOWER(body) LIKE '%incendios forestales%') as incendios,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%frente frío%'
                OR LOWER(body) LIKE '%frente frio%'
                OR LOWER(body) LIKE '%norte%') as frentes_frios
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


def disasters_by_month() -> pl.DataFrame:
    """Monthly disaster coverage for seasonality analysis."""
    return query("""
        SELECT
            DATE_TRUNC('month', publication_date)::date as month,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%huracán%'
                OR LOWER(body) LIKE '%huracan%') as huracanes,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%inundación%'
                OR LOWER(body) LIKE '%inundacion%') as inundaciones,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%sismo%'
                OR LOWER(body) LIKE '%terremoto%') as sismos
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """)


def hurricane_seasons() -> pl.DataFrame:
    """Analyze hurricane season coverage (June-November)."""
    return query("""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            EXTRACT(MONTH FROM publication_date)::int as month,
            COUNT(*) as articles,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%evacuación%'
                OR LOWER(body) LIKE '%evacuacion%') as evacuaciones,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%damnificado%') as damnificados,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%fonden%') as fonden_mentions
        FROM articles
        WHERE (LOWER(body) LIKE '%huracán%' OR LOWER(body) LIKE '%huracan%')
            AND publication_date IS NOT NULL
        GROUP BY year, month
        HAVING COUNT(*) >= 5
        ORDER BY year, month
    """)


# =============================================================================
# COVID-19 PANDEMIC
# =============================================================================

def covid_weekly() -> pl.DataFrame:
    """Weekly COVID-19 coverage analysis."""
    return query("""
        SELECT
            DATE_TRUNC('week', publication_date)::date as week,
            COUNT(*) as covid_articles,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%muerte%'
                OR LOWER(body) LIKE '%fallec%'
                OR LOWER(body) LIKE '%defunción%') as deaths_mentioned,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%contagio%'
                OR LOWER(body) LIKE '%caso positivo%'
                OR LOWER(body) LIKE '%infectado%') as cases_mentioned,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%vacuna%'
                OR LOWER(body) LIKE '%vacunación%') as vaccine_mentioned,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%hospital%'
                OR LOWER(body) LIKE '%saturación%'
                OR LOWER(body) LIKE '%terapia intensiva%') as hospital_mentioned,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%cubrebocas%'
                OR LOWER(body) LIKE '%mascarilla%') as masks_mentioned,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%cuarentena%'
                OR LOWER(body) LIKE '%confinamiento%'
                OR LOWER(body) LIKE '%semáforo%') as restrictions_mentioned
        FROM articles
        WHERE (LOWER(body) LIKE '%covid%'
            OR LOWER(body) LIKE '%coronavirus%'
            OR LOWER(body) LIKE '%pandemia%'
            OR LOWER(body) LIKE '%sars-cov%')
            AND publication_date >= '2020-01-01'
            AND publication_date IS NOT NULL
        GROUP BY week
        ORDER BY week
    """)


def covid_by_phase() -> pl.DataFrame:
    """Analyze COVID by pandemic phases."""
    return query("""
        SELECT
            CASE
                WHEN publication_date BETWEEN '2020-03-01' AND '2020-05-31' THEN '1. Inicio (Mar-May 2020)'
                WHEN publication_date BETWEEN '2020-06-01' AND '2020-12-31' THEN '2. Primera ola (Jun-Dic 2020)'
                WHEN publication_date BETWEEN '2021-01-01' AND '2021-06-30' THEN '3. Vacunación (Ene-Jun 2021)'
                WHEN publication_date BETWEEN '2021-07-01' AND '2021-12-31' THEN '4. Delta (Jul-Dic 2021)'
                WHEN publication_date BETWEEN '2022-01-01' AND '2022-06-30' THEN '5. Ómicron (Ene-Jun 2022)'
                ELSE '6. Endemia (Jul 2022+)'
            END as phase,
            COUNT(*) as total_articles,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%muerte%'
                OR LOWER(body) LIKE '%fallec%') as death_focus,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%vacuna%') as vaccine_focus,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%económ%'
                OR LOWER(body) LIKE '%desempleo%') as economic_focus
        FROM articles
        WHERE (LOWER(body) LIKE '%covid%'
            OR LOWER(body) LIKE '%coronavirus%'
            OR LOWER(body) LIKE '%pandemia%')
            AND publication_date >= '2020-03-01'
            AND publication_date IS NOT NULL
        GROUP BY phase
        ORDER BY phase
    """)


def covid_vs_regular_news() -> pl.DataFrame:
    """Compare COVID coverage to total news volume."""
    return query("""
        SELECT
            DATE_TRUNC('month', publication_date)::date as month,
            COUNT(*) as total_articles,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%covid%'
                OR LOWER(body) LIKE '%coronavirus%'
                OR LOWER(body) LIKE '%pandemia%') as covid_articles,
            ROUND(COUNT(*) FILTER (WHERE LOWER(body) LIKE '%covid%'
                OR LOWER(body) LIKE '%coronavirus%'
                OR LOWER(body) LIKE '%pandemia%') * 100.0 / COUNT(*), 1) as covid_pct
        FROM articles
        WHERE publication_date >= '2020-01-01'
            AND publication_date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """)


# =============================================================================
# OTHER HEALTH CRISES
# =============================================================================

def health_crises_timeline() -> pl.DataFrame:
    """Track major health crises over time."""
    return query("""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%dengue%') as dengue,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%chikungunya%') as chikungunya,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%zika%') as zika,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%influenza%'
                OR LOWER(body) LIKE '%h1n1%') as influenza,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%covid%'
                OR LOWER(body) LIKE '%coronavirus%') as covid
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)
