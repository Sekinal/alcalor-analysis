"""
Beautiful Plotly visualizations for Alcalor analysis.

All plots use the custom Alcalor theme for consistency.
"""

import polars as pl
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

from .theme import (
    COLORS, PARTY_COLORS, CATEGORICAL, SEQUENTIAL,
    create_figure, apply_theme
)
from ..queries import political, security, disasters, economic, temporal


# =============================================================================
# POLITICAL VISUALIZATIONS
# =============================================================================

def plot_party_evolution() -> go.Figure:
    """Beautiful timeline of political party mentions."""
    df = political.party_mentions_by_year().to_pandas()

    fig = go.Figure()

    parties = [
        ("pri", "PRI", PARTY_COLORS["pri"]),
        ("pan", "PAN", PARTY_COLORS["pan"]),
        ("prd", "PRD", PARTY_COLORS["prd"]),
        ("morena", "Morena", PARTY_COLORS["morena"]),
        ("pvem", "PVEM", PARTY_COLORS["pvem"]),
        ("mc", "MC", PARTY_COLORS["mc"]),
    ]

    for col, name, color in parties:
        fig.add_trace(go.Scatter(
            x=df["year"],
            y=df[col],
            name=name,
            mode="lines+markers",
            line=dict(width=2.5, color=color),
            marker=dict(size=6),
            hovertemplate=f"<b>{name}</b><br>%{{x}}: %{{y:,}} menciones<extra></extra>",
        ))

    fig.update_layout(
        title=dict(text="<b>Evolución de Partidos Políticos en las Noticias</b><br><sup>Menciones anuales en Al Calor Político (2005-2025)</sup>"),
        xaxis_title="Año",
        yaxis_title="Menciones",
        height=550,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
        ),
        hovermode="x unified",
    )

    # Add election year annotations
    election_years = [2006, 2012, 2018, 2024]
    for year in election_years:
        if year in df["year"].values:
            fig.add_vline(
                x=year, line_dash="dash", line_color=COLORS["neutral"],
                annotation_text=f"Elección {year}",
                annotation_position="top",
            )

    return apply_theme(fig)


def plot_presidential_transitions() -> go.Figure:
    """Visualize presidential coverage and transitions."""
    df = political.president_mentions_by_year().to_pandas()

    fig = go.Figure()

    presidents = [
        ("fox", "Fox", "#0066b3"),
        ("calderon", "Calderón", "#0066b3"),
        ("pena_nieto", "Peña Nieto", "#00923f"),
        ("amlo", "AMLO", "#8b0000"),
        ("sheinbaum", "Sheinbaum", "#8b0000"),
    ]

    for col, name, color in presidents:
        fig.add_trace(go.Bar(
            x=df["year"],
            y=df[col],
            name=name,
            marker_color=color,
            hovertemplate=f"<b>{name}</b><br>%{{x}}: %{{y:,}} menciones<extra></extra>",
        ))

    fig.update_layout(
        title=dict(text="<b>Cobertura Presidencial por Sexenio</b><br><sup>Menciones de presidentes en Al Calor Político</sup>"),
        xaxis_title="Año",
        yaxis_title="Menciones",
        barmode="stack",
        height=500,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    )

    return apply_theme(fig)


# =============================================================================
# SECURITY VISUALIZATIONS
# =============================================================================

def plot_crime_trends() -> go.Figure:
    """Multi-panel crime trends visualization."""
    df = security.crime_by_type_yearly().to_pandas()

    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            "Homicidios y Asesinatos",
            "Secuestros",
            "Feminicidios",
            "Desapariciones"
        ),
        vertical_spacing=0.12,
        horizontal_spacing=0.08,
    )

    # Homicides
    fig.add_trace(go.Scatter(
        x=df["year"], y=df["homicidios"],
        mode="lines+markers", name="Homicidios",
        line=dict(color=COLORS["secondary"], width=2),
        fill="tozeroy", fillcolor="rgba(206, 17, 38, 0.1)",
    ), row=1, col=1)

    # Kidnappings
    fig.add_trace(go.Scatter(
        x=df["year"], y=df["secuestros"],
        mode="lines+markers", name="Secuestros",
        line=dict(color=COLORS["accent"], width=2),
        fill="tozeroy", fillcolor="rgba(12, 74, 110, 0.1)",
    ), row=1, col=2)

    # Femicides
    fig.add_trace(go.Scatter(
        x=df["year"], y=df["feminicidios"],
        mode="lines+markers", name="Feminicidios",
        line=dict(color="#be185d", width=2),
        fill="tozeroy", fillcolor="rgba(190, 24, 93, 0.1)",
    ), row=2, col=1)

    # Disappearances
    fig.add_trace(go.Scatter(
        x=df["year"], y=df["desapariciones"],
        mode="lines+markers", name="Desapariciones",
        line=dict(color="#7c3aed", width=2),
        fill="tozeroy", fillcolor="rgba(124, 58, 237, 0.1)",
    ), row=2, col=2)

    fig.update_layout(
        title=dict(text="<b>Tendencias de Violencia en Veracruz</b><br><sup>Cobertura de crímenes en Al Calor Político (2005-2025)</sup>"),
        height=600,
        showlegend=False,
    )

    return apply_theme(fig)


def plot_cartel_evolution() -> go.Figure:
    """Stacked area chart of cartel mentions."""
    df = security.cartel_mentions_yearly().to_pandas()

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["year"], y=df["zetas"],
        name="Los Zetas", mode="lines",
        stackgroup="one",
        line=dict(width=0.5),
        fillcolor="rgba(206, 17, 38, 0.7)",
    ))

    fig.add_trace(go.Scatter(
        x=df["year"], y=df["cartel_golfo"],
        name="Cártel del Golfo", mode="lines",
        stackgroup="one",
        line=dict(width=0.5),
        fillcolor="rgba(12, 74, 110, 0.7)",
    ))

    fig.add_trace(go.Scatter(
        x=df["year"], y=df["cjng"],
        name="CJNG", mode="lines",
        stackgroup="one",
        line=dict(width=0.5),
        fillcolor="rgba(217, 119, 6, 0.7)",
    ))

    fig.update_layout(
        title=dict(text="<b>Evolución del Crimen Organizado en las Noticias</b><br><sup>Menciones de cárteles en Al Calor Político</sup>"),
        xaxis_title="Año",
        yaxis_title="Menciones",
        height=450,
        hovermode="x unified",
    )

    return apply_theme(fig)


def plot_duarte_scandal() -> go.Figure:
    """Timeline of the Duarte scandal coverage."""
    df = security.duarte_scandal_timeline().to_pandas()

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["month"], y=df["total_mentions"],
        name="Menciones totales",
        mode="lines",
        line=dict(width=2, color=COLORS["primary"]),
        fill="tozeroy",
        fillcolor="rgba(26, 71, 42, 0.1)",
    ))

    fig.add_trace(go.Scatter(
        x=df["month"], y=df["fuga"],
        name="Fuga/Prófugo",
        mode="lines+markers",
        line=dict(width=2, color=COLORS["secondary"]),
    ))

    fig.add_trace(go.Scatter(
        x=df["month"], y=df["extradicion"],
        name="Extradición",
        mode="lines+markers",
        line=dict(width=2, color=COLORS["accent"]),
    ))

    # Key events
    events = [
        ("2016-10-01", "Duarte renuncia"),
        ("2017-04-01", "Captura en Guatemala"),
        ("2017-07-01", "Extradición"),
    ]

    for date, label in events:
        fig.add_vline(x=date, line_dash="dash", line_color=COLORS["neutral"])
        fig.add_annotation(x=date, y=1, yref="paper", text=label, showarrow=False, textangle=-90)

    fig.update_layout(
        title=dict(text="<b>El Escándalo Duarte: Cronología de la Cobertura</b><br><sup>Menciones de Javier Duarte en Al Calor Político (2014-2020)</sup>"),
        xaxis_title="",
        yaxis_title="Menciones",
        height=500,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    )

    return apply_theme(fig)


# =============================================================================
# COVID & DISASTERS
# =============================================================================

def plot_covid_timeline() -> go.Figure:
    """COVID-19 coverage timeline with annotations."""
    df = disasters.covid_weekly().to_pandas()

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=("Artículos COVID-19 por Semana", "Temas Específicos"),
        row_heights=[0.6, 0.4],
    )

    # Main COVID articles
    fig.add_trace(go.Scatter(
        x=df["week"], y=df["covid_articles"],
        name="Total COVID",
        mode="lines",
        line=dict(width=2, color=COLORS["secondary"]),
        fill="tozeroy",
        fillcolor="rgba(206, 17, 38, 0.15)",
    ), row=1, col=1)

    # Subtopics
    topics = [
        ("deaths_mentioned", "Muertes", "#1e293b"),
        ("vaccine_mentioned", "Vacunas", "#15803d"),
        ("hospital_mentioned", "Hospitales", "#0284c7"),
    ]

    for col, name, color in topics:
        fig.add_trace(go.Scatter(
            x=df["week"], y=df[col],
            name=name,
            mode="lines",
            line=dict(width=1.5, color=color),
        ), row=2, col=1)

    fig.update_layout(
        title=dict(text="<b>Cobertura de la Pandemia COVID-19</b><br><sup>Análisis semanal de Al Calor Político (2020-2023)</sup>"),
        height=600,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    )

    return apply_theme(fig)


def plot_natural_disasters() -> go.Figure:
    """Heatmap of natural disaster coverage by year and type."""
    df = disasters.disasters_by_type_yearly().to_pandas()

    disaster_types = ["huracanes", "inundaciones", "sismos", "sequias", "incendios"]
    labels = ["Huracanes", "Inundaciones", "Sismos", "Sequías", "Incendios"]

    z = [df[col].values for col in disaster_types]

    fig = go.Figure(data=go.Heatmap(
        z=z,
        x=df["year"],
        y=labels,
        colorscale="YlOrRd",
        hovertemplate="<b>%{y}</b><br>%{x}: %{z} artículos<extra></extra>",
    ))

    fig.update_layout(
        title=dict(text="<b>Desastres Naturales en las Noticias</b><br><sup>Intensidad de cobertura por tipo y año</sup>"),
        height=400,
        xaxis_title="Año",
    )

    return apply_theme(fig)


# =============================================================================
# ECONOMIC
# =============================================================================

def plot_economic_indicators() -> go.Figure:
    """Economic topic coverage over time."""
    df = economic.economic_indicators_yearly().to_pandas()

    fig = go.Figure()

    indicators = [
        ("desempleo", "Desempleo", CATEGORICAL[0]),
        ("inflacion", "Inflación", CATEGORICAL[1]),
        ("pobreza", "Pobreza", CATEGORICAL[2]),
        ("salario_minimo", "Salario Mínimo", CATEGORICAL[3]),
        ("crisis", "Crisis Económica", CATEGORICAL[4]),
    ]

    for col, name, color in indicators:
        fig.add_trace(go.Scatter(
            x=df["year"], y=df[col],
            name=name,
            mode="lines+markers",
            line=dict(width=2, color=color),
        ))

    fig.update_layout(
        title=dict(text="<b>Cobertura de Temas Económicos</b><br><sup>Menciones en Al Calor Político (2005-2025)</sup>"),
        xaxis_title="Año",
        yaxis_title="Menciones",
        height=500,
        hovermode="x unified",
    )

    return apply_theme(fig)


# =============================================================================
# TEMPORAL PATTERNS
# =============================================================================

def plot_articles_timeline() -> go.Figure:
    """Article volume over time with trend."""
    df = temporal.articles_by_month().to_pandas()

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["month"], y=df["articles"],
        mode="lines",
        name="Artículos",
        line=dict(width=1, color=COLORS["accent"]),
        fill="tozeroy",
        fillcolor="rgba(12, 74, 110, 0.1)",
    ))

    # Add 12-month rolling average
    df["rolling"] = df["articles"].rolling(12, min_periods=1).mean()
    fig.add_trace(go.Scatter(
        x=df["month"], y=df["rolling"],
        mode="lines",
        name="Promedio móvil (12 meses)",
        line=dict(width=3, color=COLORS["secondary"]),
    ))

    fig.update_layout(
        title=dict(text="<b>Volumen de Publicación</b><br><sup>Artículos mensuales en Al Calor Político</sup>"),
        xaxis_title="",
        yaxis_title="Artículos por mes",
        height=450,
    )

    return apply_theme(fig)


def plot_day_of_week() -> go.Figure:
    """Publishing patterns by day of week."""
    df = temporal.day_of_week_patterns().to_pandas()

    fig = go.Figure(data=go.Bar(
        x=df["dia"],
        y=df["articles"],
        marker_color=[COLORS["accent"] if d in [0, 6] else COLORS["primary"]
                      for d in df["day_num"]],
        hovertemplate="<b>%{x}</b><br>%{y:,} artículos<extra></extra>",
    ))

    fig.update_layout(
        title=dict(text="<b>Patrones de Publicación por Día</b><br><sup>Total de artículos por día de la semana</sup>"),
        xaxis_title="",
        yaxis_title="Artículos",
        height=400,
    )

    return apply_theme(fig)


# =============================================================================
# DASHBOARD / OVERVIEW
# =============================================================================

def plot_overview_dashboard() -> go.Figure:
    """Create a comprehensive overview dashboard."""
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            "Partidos Políticos",
            "Tendencias de Violencia",
            "Desastres Naturales",
            "Volumen de Publicación"
        ),
        specs=[
            [{"type": "scatter"}, {"type": "scatter"}],
            [{"type": "scatter"}, {"type": "scatter"}],
        ],
        vertical_spacing=0.12,
        horizontal_spacing=0.08,
    )

    # 1. Party mentions
    parties = political.party_mentions_by_year().to_pandas()
    for col, color in [("morena", PARTY_COLORS["morena"]), ("pri", PARTY_COLORS["pri"])]:
        fig.add_trace(go.Scatter(
            x=parties["year"], y=parties[col],
            name=col.upper(), mode="lines",
            line=dict(color=color, width=2),
        ), row=1, col=1)

    # 2. Violence
    crime = security.crime_by_type_yearly().to_pandas()
    fig.add_trace(go.Scatter(
        x=crime["year"], y=crime["homicidios"],
        name="Homicidios", mode="lines",
        line=dict(color=COLORS["secondary"], width=2),
    ), row=1, col=2)

    # 3. Disasters
    dis = disasters.disasters_by_type_yearly().to_pandas()
    fig.add_trace(go.Scatter(
        x=dis["year"], y=dis["huracanes"],
        name="Huracanes", mode="lines",
        line=dict(color=COLORS["accent"], width=2),
    ), row=2, col=1)

    # 4. Volume
    vol = temporal.articles_by_year().to_pandas()
    fig.add_trace(go.Bar(
        x=vol["year"], y=vol["articles"],
        name="Artículos",
        marker_color=COLORS["primary"],
    ), row=2, col=2)

    fig.update_layout(
        title=dict(text="<b>Al Calor Político: Resumen de 20 Años</b><br><sup>Análisis de cobertura periodística (2005-2025)</sup>"),
        height=700,
        showlegend=False,
    )

    return apply_theme(fig)
