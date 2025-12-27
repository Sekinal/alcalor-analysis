"""
Economic analysis queries.

Track economic coverage:
- Employment and unemployment
- Inflation and cost of living
- PEMEX and energy sector
- Port of Veracruz activity
- Poverty and inequality
"""

import polars as pl
from ..db import query


def economic_indicators_yearly() -> pl.DataFrame:
    """Track economic topic coverage by year."""
    return query("""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%desempleo%'
                OR LOWER(body) LIKE '%desempleado%') as desempleo,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%empleo%'
                AND LOWER(body) NOT LIKE '%desempleo%') as empleo,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%inflación%'
                OR LOWER(body) LIKE '%inflacion%') as inflacion,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%salario mínimo%'
                OR LOWER(body) LIKE '%salario minimo%') as salario_minimo,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%pobreza%'
                OR LOWER(body) LIKE '%marginación%') as pobreza,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%inversión%'
                OR LOWER(body) LIKE '%inversion%') as inversion,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%crisis económica%'
                OR LOWER(body) LIKE '%recesión%') as crisis
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


def pemex_coverage() -> pl.DataFrame:
    """Track PEMEX and energy sector coverage."""
    return query("""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%pemex%') as pemex,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%refinería%'
                OR LOWER(body) LIKE '%refineria%') as refineria,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%gasolina%'
                OR LOWER(body) LIKE '%gasolinazo%') as gasolina,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%huachicol%'
                OR LOWER(body) LIKE '%robo de combustible%') as huachicoleros,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%dos bocas%') as dos_bocas,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%cfe%'
                OR LOWER(body) LIKE '%comisión federal de electricidad%') as cfe
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


def port_of_veracruz() -> pl.DataFrame:
    """Track Port of Veracruz coverage."""
    return query("""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%puerto de veracruz%'
                OR (LOWER(body) LIKE '%puerto%'
                    AND LOWER(body) LIKE '%veracruz%')) as puerto_mentions,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%exportación%'
                OR LOWER(body) LIKE '%exportacion%') as exportaciones,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%importación%'
                OR LOWER(body) LIKE '%importacion%') as importaciones,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%contenedor%') as contenedores,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%ampliación del puerto%'
                OR LOWER(body) LIKE '%nuevo puerto%') as expansion
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


def tourism_coverage() -> pl.DataFrame:
    """Track tourism sector coverage."""
    return query("""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%turismo%'
                OR LOWER(body) LIKE '%turista%') as turismo,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%hotel%'
                OR LOWER(body) LIKE '%ocupación hotelera%') as hoteles,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%carnaval%') as carnaval,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%semana santa%') as semana_santa,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%crucero%') as cruceros
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


def agriculture_coverage() -> pl.DataFrame:
    """Track agricultural sector (key for Veracruz)."""
    return query("""
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as year,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%café%'
                OR LOWER(body) LIKE '%cafeticultor%') as cafe,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%caña%'
                OR LOWER(body) LIKE '%azúcar%'
                OR LOWER(body) LIKE '%ingenio%') as cana_azucar,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%ganadería%'
                OR LOWER(body) LIKE '%ganadero%') as ganaderia,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%limón%'
                OR LOWER(body) LIKE '%cítrico%') as citricos,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%vainilla%') as vainilla,
            COUNT(*) FILTER (WHERE LOWER(body) LIKE '%campesino%'
                OR LOWER(body) LIKE '%agricultor%') as campesinos
        FROM articles
        WHERE publication_date IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)
