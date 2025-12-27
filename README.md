# Al Calor Politico Analysis

Data analysis toolkit for exploring 20+ years of news articles from [Al Calor Politico](https://www.alcalorpolitico.com/), a regional news outlet covering Veracruz, Mexico.

## Features

- **Political Analysis**: Track mentions of political parties (PRI, PAN, PRD, Morena, etc.), presidents, and governors over time
- **Security Analysis**: Monitor violence trends, cartel mentions, and crime coverage patterns
- **Economic Indicators**: Analyze coverage of economic topics, employment, and investment
- **Disaster Tracking**: Natural disasters, health crises, and emergency coverage
- **NLP Tools**: TF-IDF analysis, sentiment tracking, emerging term detection, and co-occurrence patterns
- **Interactive Visualizations**: Generate Plotly charts with a custom Mexican-inspired theme
- **Report Generation**: Create comprehensive HTML reports with embedded visualizations

## Installation

Requires Python 3.12+ and [uv](https://github.com/astral-sh/uv).

```bash
git clone https://github.com/Sekinal/alcalor-analysis.git
cd alcalor-analysis
uv sync
```

## Configuration

Create a `.env` file with your database connection:

```env
DATABASE_URL=postgresql://user:password@host:port/database
```

If accessing a remote database via SSH tunnel:

```bash
ssh -L 5432:localhost:5432 your-server -N &
```

## Usage

### CLI Commands

```bash
# Show database statistics
uv run alcalor stats

# Generate a specific visualization
uv run alcalor generate parties      # Political party mentions
uv run alcalor generate presidents   # Presidential coverage
uv run alcalor generate crime        # Violence trends
uv run alcalor generate cartels      # Organized crime
uv run alcalor generate covid        # COVID-19 coverage
uv run alcalor generate disasters    # Natural disasters
uv run alcalor generate economic     # Economic indicators
uv run alcalor generate timeline     # Publication volume
uv run alcalor generate dashboard    # Summary dashboard

# Generate all visualizations
uv run alcalor generate all
```

### Python API

```python
from alcalor_analysis.db import query
from alcalor_analysis.queries import political, security, disasters
from alcalor_analysis.nlp import find_emerging_terms, analyze_sentiment_by_year

# Query the database
df = query("SELECT COUNT(*) FROM articles")

# Get political party mentions by year
parties = political.party_mentions_by_year()

# Find terms that emerged in a specific year
emerging = find_emerging_terms(2020, comparison_years=2, top_n=15)

# Analyze sentiment trends
sentiment = analyze_sentiment_by_year()
```

### Generate Full Report

```python
from alcalor_analysis.report import generate_full_report

generate_full_report("reports/my_report.html")
```

## Project Structure

```
src/alcalor_analysis/
├── db.py           # Database connection utilities
├── cli.py          # Command-line interface
├── nlp.py          # NLP analysis (TF-IDF, sentiment, etc.)
├── report.py       # HTML report generation
├── queries/
│   ├── political.py    # Political analysis queries
│   ├── security.py     # Crime and security queries
│   ├── disasters.py    # Natural disaster queries
│   ├── economic.py     # Economic indicator queries
│   └── temporal.py     # Time-based analysis
└── viz/
    ├── theme.py        # Custom Plotly theme
    └── plots.py        # Visualization functions
```

## Dependencies

- **polars** - Fast DataFrame operations
- **plotly** - Interactive visualizations
- **scikit-learn** - TF-IDF and NLP utilities
- **nltk** - Natural language processing
- **connectorx** - Fast database queries
- **duckdb** - Local analytical queries

## License

MIT
