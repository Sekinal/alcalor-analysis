"""
Mexico & Veracruz Deep Analysis Module

Comprehensive analysis of news patterns specific to:
- Veracruz state politics and governors
- Mexican federal politics
- Security and crime trends
- Natural disasters
- Economic indicators
- Corruption patterns
- COVID-19 coverage
"""

import os
import polars as pl
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


# =============================================================================
# VERACRUZ GOVERNORS (for political cycle analysis)
# =============================================================================
VERACRUZ_GOVERNORS = {
    "Fidel Herrera": ("2004-12-01", "2010-11-30"),
    "Javier Duarte": ("2010-12-01", "2016-10-12"),  # Left early due to scandal
    "Flavino Ríos": ("2016-10-12", "2016-11-30"),  # Interim
    "Miguel Ángel Yunes": ("2016-12-01", "2018-11-30"),
    "Cuitláhuac García": ("2018-12-01", "2024-11-30"),
    "Rocío Nahle": ("2024-12-01", "2030-11-30"),  # Current
}

# Mexican Presidents
MEXICAN_PRESIDENTS = {
    "Vicente Fox": ("2000-12-01", "2006-11-30"),
    "Felipe Calderón": ("2006-12-01", "2012-11-30"),
    "Enrique Peña Nieto": ("2012-12-01", "2018-11-30"),
    "AMLO": ("2018-12-01", "2024-09-30"),
    "Claudia Sheinbaum": ("2024-10-01", "2030-09-30"),
}

# Key search terms for different analyses
SECURITY_TERMS = [
    "homicidio", "asesinato", "secuestro", "extorsión", "narco",
    "cártel", "zetas", "crimen organizado", "balacera", "ejecutado",
    "levantón", "desaparecido", "fosa", "narcotráfico", "sicario"
]

CORRUPTION_TERMS = [
    "corrupción", "desvío", "peculado", "soborno", "moches",
    "lavado de dinero", "enriquecimiento ilícito", "fraude",
    "malversación", "nepotismo", "conflicto de interés"
]

NATURAL_DISASTER_TERMS = [
    "huracán", "inundación", "desbordamiento", "tormenta tropical",
    "norte", "frente frío", "sequía", "incendio forestal", "sismo"
]

ECONOMIC_TERMS = [
    "desempleo", "inflación", "salario mínimo", "pobreza",
    "inversión", "empleo", "economía", "PIB", "crisis económica"
]


# =============================================================================
# POLITICAL ANALYSIS
# =============================================================================

def mentions_over_time(term: str, granularity: str = "month") -> pl.DataFrame:
    """Track mentions of a term over time."""
    if granularity == "month":
        date_trunc = "DATE_TRUNC('month', publication_date)"
    elif granularity == "year":
        date_trunc = "DATE_TRUNC('year', publication_date)"
    else:
        date_trunc = "publication_date"

    query = f"""
        SELECT
            {date_trunc}::date as period,
            COUNT(*) as mentions
        FROM articles
        WHERE LOWER(body) LIKE LOWER('%{term}%')
            AND publication_date IS NOT NULL
        GROUP BY period
        ORDER BY period
    """
    return pl.read_database_uri(query, DATABASE_URL)


def governor_coverage() -> pl.DataFrame:
    """Analyze coverage of each Veracruz governor."""
    results = []
    for governor, (start, end) in VERACRUZ_GOVERNORS.items():
        # Get first name/last name for search
        search_term = governor.split()[-1]  # Use last name
        query = f"""
            SELECT
                '{governor}' as governor,
                '{start}' as term_start,
                '{end}' as term_end,
                COUNT(*) as total_mentions,
                COUNT(*) FILTER (WHERE publication_date BETWEEN '{start}' AND '{end}') as during_term,
                COUNT(*) FILTER (WHERE publication_date > '{end}') as after_term
            FROM articles
            WHERE LOWER(body) LIKE LOWER('%{search_term}%')
        """
        df = pl.read_database_uri(query, DATABASE_URL)
        results.append(df)

    return pl.concat(results)


def party_mentions() -> pl.DataFrame:
    """Compare mentions of major political parties over time."""
    query = """
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
    """
    return pl.read_database_uri(query, DATABASE_URL)


def president_mentions() -> pl.DataFrame:
    """Track mentions of Mexican presidents."""
    query = """
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%fox%'
                AND LOWER(body) LIKE '%presidente%') as fox,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%calderón%'
                OR LOWER(body) LIKE '%calderon%') as calderon,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%peña nieto%'
                OR LOWER(body) LIKE '%epn%') as pena_nieto,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%lópez obrador%'
                OR LOWER(body) LIKE '%amlo%'
                OR LOWER(body) LIKE '%lopez obrador%') as amlo,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%sheinbaum%') as sheinbaum
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """
    return pl.read_database_uri(query, DATABASE_URL)


# =============================================================================
# SECURITY & CRIME ANALYSIS
# =============================================================================

def security_timeline() -> pl.DataFrame:
    """Track security-related news over time."""
    conditions = " OR ".join([f"LOWER(body) LIKE '%{term}%'" for term in SECURITY_TERMS])
    query = f"""
        SELECT
            DATE_TRUNC('month', publication_date)::date as month,
            COUNT(*) as security_articles,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as pct_of_total
        FROM articles
        WHERE ({conditions})
            AND publication_date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """
    return pl.read_database_uri(query, DATABASE_URL)


def crime_by_type() -> pl.DataFrame:
    """Break down crime coverage by type."""
    query = """
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%homicidio%'
                OR LOWER(body) LIKE '%asesinato%') as homicidios,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%secuestro%') as secuestros,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%extorsión%'
                OR LOWER(body) LIKE '%extorsion%') as extorsiones,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%robo%') as robos,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%feminicidio%') as feminicidios,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%desaparecido%'
                OR LOWER(body) LIKE '%desaparición%') as desapariciones
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """
    return pl.read_database_uri(query, DATABASE_URL)


def cartel_mentions() -> pl.DataFrame:
    """Track mentions of cartels and organized crime groups."""
    query = """
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%zetas%') as zetas,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%cártel del golfo%'
                OR LOWER(body) LIKE '%cartel del golfo%') as golfo,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%cjng%'
                OR LOWER(body) LIKE '%jalisco nueva generación%') as cjng,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%narco%') as narco_general,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%crimen organizado%') as crimen_organizado
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """
    return pl.read_database_uri(query, DATABASE_URL)


# =============================================================================
# CORRUPTION ANALYSIS (especially Duarte era)
# =============================================================================

def corruption_timeline() -> pl.DataFrame:
    """Track corruption-related coverage."""
    conditions = " OR ".join([f"LOWER(body) LIKE '%{term}%'" for term in CORRUPTION_TERMS])
    query = f"""
        SELECT
            DATE_TRUNC('month', publication_date)::date as month,
            COUNT(*) as corruption_articles
        FROM articles
        WHERE ({conditions})
            AND publication_date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """
    return pl.read_database_uri(query, DATABASE_URL)


def duarte_scandal_analysis() -> pl.DataFrame:
    """Deep dive into Javier Duarte corruption scandal coverage."""
    query = """
        SELECT
            DATE_TRUNC('month', publication_date)::date as month,
            COUNT(*) as mentions,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%desvío%') as desvio,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%fuga%'
                OR LOWER(body) LIKE '%prófugo%') as fuga,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%extradición%') as extradicion,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%sentencia%'
                OR LOWER(body) LIKE '%condena%') as sentencia
        FROM articles
        WHERE LOWER(body) LIKE '%duarte%'
            AND publication_date IS NOT NULL
            AND publication_date >= '2016-01-01'
        GROUP BY month
        ORDER BY month
    """
    return pl.read_database_uri(query, DATABASE_URL)


# =============================================================================
# NATURAL DISASTERS
# =============================================================================

def hurricane_coverage() -> pl.DataFrame:
    """Analyze hurricane coverage (Veracruz is highly vulnerable)."""
    query = """
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            EXTRACT(MONTH FROM publication_date)::int as month,
            COUNT(*) as articles,
            STRING_AGG(DISTINCT
                CASE
                    WHEN LOWER(title) LIKE '%huracán%' THEN
                        SUBSTRING(title FROM 'huracán\\s+(\\w+)')
                    ELSE NULL
                END, ', ') as hurricane_names
        FROM articles
        WHERE (LOWER(body) LIKE '%huracán%' OR LOWER(body) LIKE '%huracan%')
            AND publication_date IS NOT NULL
        GROUP BY year, month
        HAVING COUNT(*) > 5
        ORDER BY year, month
    """
    return pl.read_database_uri(query, DATABASE_URL)


def disaster_by_type() -> pl.DataFrame:
    """Break down natural disaster coverage by type."""
    query = """
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%huracán%'
                OR LOWER(body) LIKE '%huracan%') as huracanes,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%inundación%'
                OR LOWER(body) LIKE '%inundacion%'
                OR LOWER(body) LIKE '%inunda%') as inundaciones,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%sismo%'
                OR LOWER(body) LIKE '%terremoto%') as sismos,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%sequía%'
                OR LOWER(body) LIKE '%sequia%') as sequias,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%incendio forestal%') as incendios
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """
    return pl.read_database_uri(query, DATABASE_URL)


# =============================================================================
# COVID-19 ANALYSIS
# =============================================================================

def covid_coverage() -> pl.DataFrame:
    """Analyze COVID-19 pandemic coverage."""
    query = """
        SELECT
            DATE_TRUNC('week', publication_date)::date as week,
            COUNT(*) as covid_articles,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%muerte%'
                OR LOWER(body) LIKE '%fallec%') as deaths_mentioned,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%vacuna%') as vaccine_mentioned,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%hospital%'
                OR LOWER(body) LIKE '%saturación%') as hospital_mentioned,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%cubrebocas%'
                OR LOWER(body) LIKE '%mascarilla%') as masks_mentioned
        FROM articles
        WHERE (LOWER(body) LIKE '%covid%'
            OR LOWER(body) LIKE '%coronavirus%'
            OR LOWER(body) LIKE '%pandemia%'
            OR LOWER(body) LIKE '%sars-cov%')
            AND publication_date >= '2020-01-01'
            AND publication_date IS NOT NULL
        GROUP BY week
        ORDER BY week
    """
    return pl.read_database_uri(query, DATABASE_URL)


# =============================================================================
# MUNICIPAL ANALYSIS
# =============================================================================

def top_municipalities() -> pl.DataFrame:
    """Find most mentioned Veracruz municipalities."""
    # Major Veracruz municipalities
    municipalities = [
        "Xalapa", "Veracruz", "Coatzacoalcos", "Córdoba", "Orizaba",
        "Poza Rica", "Boca del Río", "Minatitlán", "Tuxpan", "Papantla",
        "Martínez de la Torre", "Coatepec", "Tierra Blanca", "San Andrés Tuxtla",
        "Cosamaloapan", "Acayucan", "Tantoyuca", "Álamo", "Perote", "Fortín"
    ]

    cases = []
    for muni in municipalities:
        cases.append(f"COUNT(*) FILTER (WHERE LOWER(body) LIKE LOWER('%{muni}%')) as \"{muni}\"")

    query = f"""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            {', '.join(cases)}
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """
    return pl.read_database_uri(query, DATABASE_URL)


def municipality_crime_hotspots() -> pl.DataFrame:
    """Identify crime hotspots by municipality."""
    municipalities = ["Xalapa", "Veracruz", "Coatzacoalcos", "Córdoba", "Orizaba",
                      "Poza Rica", "Minatitlán", "Tuxpan", "Papantla", "Acayucan"]

    cases = []
    for muni in municipalities:
        cases.append(f"""
            COUNT(*) FILTER (WHERE LOWER(body) LIKE LOWER('%{muni}%')
                AND (LOWER(body) LIKE '%homicidio%'
                    OR LOWER(body) LIKE '%asesinato%'
                    OR LOWER(body) LIKE '%secuestro%'
                    OR LOWER(body) LIKE '%balacera%')) as \"{muni}\"
        """)

    query = f"""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            {', '.join(cases)}
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """
    return pl.read_database_uri(query, DATABASE_URL)


# =============================================================================
# ECONOMIC ANALYSIS
# =============================================================================

def economic_indicators() -> pl.DataFrame:
    """Track economic-related coverage."""
    query = """
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%desempleo%') as desempleo,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%inflación%'
                OR LOWER(body) LIKE '%inflacion%') as inflacion,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%salario mínimo%'
                OR LOWER(body) LIKE '%salario minimo%') as salario_minimo,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%pobreza%') as pobreza,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%inversión%'
                OR LOWER(body) LIKE '%inversion%') as inversion,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%pemex%') as pemex,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%puerto%'
                AND LOWER(body) LIKE '%veracruz%') as puerto_veracruz
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """
    return pl.read_database_uri(query, DATABASE_URL)


# =============================================================================
# TEMPORAL PATTERNS
# =============================================================================

def day_of_week_patterns() -> pl.DataFrame:
    """Analyze publishing patterns by day of week."""
    query = """
        SELECT
            EXTRACT(DOW FROM publication_date)::int as day_of_week,
            CASE EXTRACT(DOW FROM publication_date)::int
                WHEN 0 THEN 'Domingo'
                WHEN 1 THEN 'Lunes'
                WHEN 2 THEN 'Martes'
                WHEN 3 THEN 'Miércoles'
                WHEN 4 THEN 'Jueves'
                WHEN 5 THEN 'Viernes'
                WHEN 6 THEN 'Sábado'
            END as day_name,
            COUNT(*) as articles,
            AVG(LENGTH(body))::int as avg_length
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY day_of_week
        ORDER BY day_of_week
    """
    return pl.read_database_uri(query, DATABASE_URL)


def election_years_analysis() -> pl.DataFrame:
    """Compare coverage in election vs non-election years."""
    # Mexican federal elections: 2006, 2012, 2018, 2024
    # Veracruz gubernatorial: 2010, 2016, 2018 (realigned), 2024
    query = """
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) as total_articles,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%elección%'
                OR LOWER(body) LIKE '%eleccion%'
                OR LOWER(body) LIKE '%voto%'
                OR LOWER(body) LIKE '%candidat%') as election_articles,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%campaña%'
                OR LOWER(body) LIKE '%campana%') as campaign_articles,
            CASE
                WHEN EXTRACT(YEAR FROM publication_date)::int IN (2006, 2012, 2018, 2024)
                THEN 'Federal'
                WHEN EXTRACT(YEAR FROM publication_date)::int IN (2010, 2016)
                THEN 'Gubernatorial'
                ELSE 'Non-election'
            END as election_type
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """
    return pl.read_database_uri(query, DATABASE_URL)


# =============================================================================
# KEYWORD EXTRACTION & TRENDING
# =============================================================================

def trending_topics_by_period(start_date: str, end_date: str, limit: int = 20) -> pl.DataFrame:
    """Extract most common keywords for a period."""
    query = f"""
        SELECT
            UNNEST(keywords) as keyword,
            COUNT(*) as frequency
        FROM articles
        WHERE publication_date BETWEEN '{start_date}' AND '{end_date}'
            AND keywords IS NOT NULL
        GROUP BY keyword
        ORDER BY frequency DESC
        LIMIT {limit}
    """
    return pl.read_database_uri(query, DATABASE_URL)


def keyword_evolution(keyword: str) -> pl.DataFrame:
    """Track how a keyword's usage evolved over time."""
    query = f"""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) as mentions,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY EXTRACT(YEAR FROM publication_date)) as pct_of_year
        FROM articles
        WHERE '{keyword}' = ANY(keywords)
            AND publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """
    return pl.read_database_uri(query, DATABASE_URL)


# =============================================================================
# FULL REPORT GENERATION
# =============================================================================

def generate_full_report():
    """Generate comprehensive analysis report."""
    print("=" * 70)
    print("ANÁLISIS PROFUNDO: AL CALOR POLÍTICO - VERACRUZ")
    print("=" * 70)

    # Basic stats
    from main import get_stats
    stats = get_stats()
    print(f"\n📊 ESTADÍSTICAS GENERALES")
    print(f"   Total de artículos: {stats['total_articles']:,}")
    print(f"   Período: {stats['earliest_date']} → {stats['latest_date']}")
    print(f"   Longitud promedio: {stats['avg_body_length']:.0f} caracteres")

    # Political parties
    print(f"\n🏛️  MENCIONES DE PARTIDOS POLÍTICOS POR AÑO")
    parties = party_mentions()
    print(parties)

    # Presidents
    print(f"\n🇲🇽 MENCIONES DE PRESIDENTES POR AÑO")
    presidents = president_mentions()
    print(presidents)

    # Crime analysis
    print(f"\n🚨 COBERTURA DE SEGURIDAD Y CRIMEN POR AÑO")
    crime = crime_by_type()
    print(crime)

    # Cartels
    print(f"\n⚠️  MENCIONES DE GRUPOS DEL CRIMEN ORGANIZADO")
    cartels = cartel_mentions()
    print(cartels)

    # Corruption (Duarte)
    print(f"\n💰 ANÁLISIS DEL ESCÁNDALO DUARTE (2016+)")
    duarte = duarte_scandal_analysis()
    print(duarte.head(20))

    # Natural disasters
    print(f"\n🌀 DESASTRES NATURALES POR AÑO")
    disasters = disaster_by_type()
    print(disasters)

    # COVID
    print(f"\n🦠 COBERTURA COVID-19 (semanas con más artículos)")
    covid = covid_coverage()
    print(covid.sort("covid_articles", descending=True).head(15))

    # Economic
    print(f"\n💼 INDICADORES ECONÓMICOS")
    economic = economic_indicators()
    print(economic)

    # Day of week
    print(f"\n📅 PATRONES POR DÍA DE LA SEMANA")
    dow = day_of_week_patterns()
    print(dow)

    # Election years
    print(f"\n🗳️  AÑOS ELECTORALES VS NO ELECTORALES")
    elections = election_years_analysis()
    print(elections)

    print("\n" + "=" * 70)
    print("Reporte completo generado.")
    print("=" * 70)


if __name__ == "__main__":
    generate_full_report()
