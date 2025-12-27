"""
Security and crime analysis queries.

Analyze violence and organized crime in Veracruz:
- Crime types (homicides, kidnappings, extortion, femicides)
- Cartel mentions (Zetas, CJNG, Gulf Cartel)
- Municipal crime hotspots
- Corruption coverage
"""

import polars as pl
from ..db import query


# =============================================================================
# CRIME BY TYPE
# =============================================================================

def crime_by_type_yearly() -> pl.DataFrame:
    """Break down crime coverage by type per year."""
    return query("""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%homicidio%'
                OR LOWER(body) LIKE '%asesinato%'
                OR LOWER(body) LIKE '%asesinado%') as homicidios,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%secuestro%'
                OR LOWER(body) LIKE '%secuestrado%'
                OR LOWER(body) LIKE '%plagio%') as secuestros,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%extorsión%'
                OR LOWER(body) LIKE '%extorsion%') as extorsiones,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%feminicidio%') as feminicidios,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%desaparecido%'
                OR LOWER(body) LIKE '%desaparición forzada%') as desapariciones,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%robo%'
                OR LOWER(body) LIKE '%asalto%') as robos,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%violación%'
                OR LOWER(body) LIKE '%abuso sexual%') as violencia_sexual
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


def crime_by_type_monthly() -> pl.DataFrame:
    """Monthly crime coverage for trend analysis."""
    return query("""
        SELECT
            DATE_TRUNC('month', publication_date)::date as month,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%homicidio%'
                OR LOWER(body) LIKE '%asesinato%') as homicidios,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%secuestro%') as secuestros,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%feminicidio%') as feminicidios,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%desaparecido%') as desapariciones
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """)


# =============================================================================
# ORGANIZED CRIME
# =============================================================================

def cartel_mentions_yearly() -> pl.DataFrame:
    """Track mentions of cartels and organized crime groups."""
    return query("""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%zetas%'
                OR LOWER(body) LIKE '%los zetas%') as zetas,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%cártel del golfo%'
                OR LOWER(body) LIKE '%cartel del golfo%'
                OR LOWER(body) LIKE '%cdg%') as cartel_golfo,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%cjng%'
                OR LOWER(body) LIKE '%jalisco nueva generación%'
                OR LOWER(body) LIKE '%jalisco nueva generacion%') as cjng,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%narco%'
                OR LOWER(body) LIKE '%narcotráfico%'
                OR LOWER(body) LIKE '%narcotrafico%') as narco_general,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%crimen organizado%') as crimen_organizado,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%sicario%'
                OR LOWER(body) LIKE '%sicarios%') as sicarios,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%huachicol%'
                OR LOWER(body) LIKE '%robo de combustible%') as huachicoleros
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


def cartel_mentions_monthly() -> pl.DataFrame:
    """Monthly cartel activity for detailed analysis."""
    return query("""
        SELECT
            DATE_TRUNC('month', publication_date)::date as month,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%zetas%') as zetas,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%cjng%'
                OR LOWER(body) LIKE '%jalisco nueva generación%') as cjng,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%narco%') as narco_general
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """)


# =============================================================================
# CORRUPTION (DUARTE SCANDAL)
# =============================================================================

def corruption_timeline() -> pl.DataFrame:
    """Track corruption-related news over time."""
    return query("""
        SELECT
            DATE_TRUNC('month', publication_date)::date as month,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%corrupción%'
                OR LOWER(body) LIKE '%corrupcion%') as corrupcion,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%desvío%'
                OR LOWER(body) LIKE '%desvio%') as desvio,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%peculado%') as peculado,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%lavado de dinero%') as lavado,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%impunidad%') as impunidad
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """)


def duarte_scandal_timeline() -> pl.DataFrame:
    """Deep dive into the Javier Duarte corruption scandal."""
    return query("""
        SELECT
            DATE_TRUNC('month', publication_date)::date as month,
            COUNT(*) as total_mentions,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%desvío%'
                OR LOWER(body) LIKE '%desvio%') as desvio_recursos,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%fuga%'
                OR LOWER(body) LIKE '%prófugo%'
                OR LOWER(body) LIKE '%profugo%') as fuga,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%guatemala%') as guatemala,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%extradición%'
                OR LOWER(body) LIKE '%extradicion%') as extradicion,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%sentencia%'
                OR LOWER(body) LIKE '%condena%') as sentencia,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%karime%') as karime_macias
        FROM articles
        WHERE LOWER(body) LIKE '%duarte%'
            AND publication_date >= '2014-01-01'
            AND publication_date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """)


# =============================================================================
# MUNICIPAL HOTSPOTS
# =============================================================================

VERACRUZ_MUNICIPALITIES = [
    "Xalapa", "Veracruz", "Coatzacoalcos", "Córdoba", "Orizaba",
    "Poza Rica", "Boca del Río", "Minatitlán", "Tuxpan", "Papantla",
    "Martínez de la Torre", "Coatepec", "Tierra Blanca", "San Andrés Tuxtla",
    "Cosamaloapan", "Acayucan", "Tantoyuca", "Perote", "Fortín"
]


def crime_by_municipality() -> pl.DataFrame:
    """Identify crime hotspots by municipality."""
    municipalities = VERACRUZ_MUNICIPALITIES[:10]  # Top 10

    cases = [f"""
        COUNT(*) FILTER (WHERE LOWER(body) LIKE LOWER('%{muni}%')
            AND (LOWER(body) LIKE '%homicidio%'
                OR LOWER(body) LIKE '%secuestro%'
                OR LOWER(body) LIKE '%balacera%'
                OR LOWER(body) LIKE '%asesinato%')) as "{muni}"
    """ for muni in municipalities]

    return query(f"""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            {', '.join(cases)}
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


def violence_severity_index() -> pl.DataFrame:
    """Calculate a violence severity index by year."""
    return query("""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) as total_articles,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%homicidio%'
                OR LOWER(body) LIKE '%asesinato%') as homicidios,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%secuestro%') as secuestros,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%feminicidio%') as feminicidios,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%masacre%'
                OR LOWER(body) LIKE '%ejecución masiva%') as masacres,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%fosa%'
                OR LOWER(body) LIKE '%fosas clandestinas%') as fosas,
            -- Violence index: weighted sum
            (COUNT(*) FILTER (WHERE LOWER(body) LIKE '%homicidio%') * 1.0 +
             COUNT(*) FILTER (WHERE LOWER(body) LIKE '%secuestro%') * 1.5 +
             COUNT(*) FILTER (WHERE LOWER(body) LIKE '%feminicidio%') * 2.0 +
             COUNT(*) FILTER (WHERE LOWER(body) LIKE '%masacre%') * 5.0 +
             COUNT(*) FILTER (WHERE LOWER(body) LIKE '%fosa%') * 3.0
            ) / NULLIF(COUNT(*), 0) * 100 as violence_index
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)
