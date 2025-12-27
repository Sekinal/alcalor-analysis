"""
Custom Plotly theme for Alcalor Analysis.

A beautiful, professional theme inspired by FiveThirtyEight and The Economist.
"""

import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

# =============================================================================
# COLOR PALETTES
# =============================================================================

# Main palette - inspired by Mexican flag + modern data viz
COLORS = {
    "primary": "#1a472a",      # Dark green (Mexico)
    "secondary": "#ce1126",     # Red (Mexico)
    "accent": "#0c4a6e",        # Deep blue
    "neutral": "#64748b",       # Slate gray
    "background": "#fafafa",    # Off-white
    "grid": "#e2e8f0",          # Light gray
    "text": "#1e293b",          # Dark slate
    "text_light": "#64748b",    # Medium slate
}

# Political party colors (official-ish)
PARTY_COLORS = {
    "pri": "#00923f",           # Green
    "pan": "#0066b3",           # Blue
    "prd": "#ffd700",           # Yellow/Gold
    "morena": "#8b0000",        # Dark red/maroon
    "pvem": "#39b54a",          # Light green
    "mc": "#ff6600",            # Orange
}

# Sequential palette for heatmaps/gradients
SEQUENTIAL = [
    "#f0fdf4", "#dcfce7", "#bbf7d0", "#86efac",
    "#4ade80", "#22c55e", "#16a34a", "#15803d", "#166534", "#14532d"
]

# Diverging palette (for comparisons)
DIVERGING = ["#ce1126", "#f87171", "#fecaca", "#fafafa", "#bae6fd", "#38bdf8", "#0284c7"]

# Categorical palette for multi-series
CATEGORICAL = [
    "#0c4a6e",  # Blue
    "#ce1126",  # Red
    "#15803d",  # Green
    "#d97706",  # Orange
    "#7c3aed",  # Purple
    "#0891b2",  # Cyan
    "#be185d",  # Pink
    "#65a30d",  # Lime
]


# =============================================================================
# THEME CONFIGURATION
# =============================================================================

ALCALOR_TEMPLATE = go.layout.Template(
    layout=go.Layout(
        # Colors
        colorway=CATEGORICAL,
        paper_bgcolor=COLORS["background"],
        plot_bgcolor=COLORS["background"],

        # Fonts
        font=dict(
            family="Inter, -apple-system, BlinkMacSystemFont, sans-serif",
            size=12,
            color=COLORS["text"],
        ),
        title=dict(
            font=dict(size=20, color=COLORS["text"]),
            x=0.02,
            xanchor="left",
        ),

        # Axes
        xaxis=dict(
            showgrid=True,
            gridcolor=COLORS["grid"],
            gridwidth=1,
            showline=True,
            linecolor=COLORS["grid"],
            linewidth=1,
            tickfont=dict(size=11),
            title_font=dict(size=12),
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor=COLORS["grid"],
            gridwidth=1,
            showline=False,
            tickfont=dict(size=11),
            title_font=dict(size=12),
        ),

        # Legend
        legend=dict(
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor=COLORS["grid"],
            borderwidth=1,
            font=dict(size=11),
        ),

        # Margins
        margin=dict(l=60, r=30, t=80, b=60),

        # Hover
        hoverlabel=dict(
            bgcolor="white",
            font_size=12,
            font_family="Inter, sans-serif",
        ),
    )
)

# Register the template
pio.templates["alcalor"] = ALCALOR_TEMPLATE
pio.templates.default = "alcalor"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def apply_theme(fig: go.Figure) -> go.Figure:
    """Apply the Alcalor theme to a figure."""
    fig.update_layout(template="alcalor")
    return fig


def create_figure(
    title: str = "",
    subtitle: str = "",
    height: int = 500,
    width: int = 900,
) -> go.Figure:
    """Create a figure with the Alcalor theme."""
    fig = go.Figure()
    fig.update_layout(
        template="alcalor",
        height=height,
        width=width,
        title=dict(
            text=f"<b>{title}</b>" + (f"<br><sup>{subtitle}</sup>" if subtitle else ""),
        ),
    )
    return fig


def create_subplots(
    rows: int,
    cols: int,
    titles: list[str] = None,
    **kwargs
) -> go.Figure:
    """Create subplots with the Alcalor theme."""
    fig = make_subplots(rows=rows, cols=cols, subplot_titles=titles, **kwargs)
    fig.update_layout(template="alcalor")
    return fig


def save_figure(fig: go.Figure, filename: str, format: str = "html"):
    """Save figure in various formats."""
    if format == "html":
        fig.write_html(f"{filename}.html", include_plotlyjs="cdn")
    elif format == "png":
        fig.write_image(f"{filename}.png", scale=2)
    elif format == "pdf":
        fig.write_image(f"{filename}.pdf")
    elif format == "svg":
        fig.write_image(f"{filename}.svg")
