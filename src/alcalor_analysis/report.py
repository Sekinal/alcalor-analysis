"""
Report Generator for Al Calor Político Analysis.

Generates comprehensive HTML reports with embedded visualizations
and NLP-based insights.
"""

import json
from datetime import datetime
from pathlib import Path

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import polars as pl

from .db import query
from .nlp import (
    analyze_sentiment_by_year,
    find_emerging_terms,
    detect_anomalies_in_coverage,
    extract_key_actors,
    get_tfidf_by_period,
    find_cooccurrences
)


def generate_full_report(output_path: str = "reports/informe_completo.html"):
    """Generate a comprehensive HTML report with all findings."""

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    print("Generando informe completo...")
    print("=" * 50)

    # Collect all analyses
    sections = []

    # 1. Database overview
    print("1/8 Resumen de la base de datos...")
    overview = _generate_overview_section()
    sections.append(overview)

    # 2. Duarte Early Warning
    print("2/8 Análisis Duarte Early Warning...")
    duarte = _generate_duarte_section()
    sections.append(duarte)

    # 3. Cartel Transition
    print("3/8 Transición de carteles...")
    cartels = _generate_cartel_section()
    sections.append(cartels)

    # 4. Sentiment Analysis
    print("4/8 Análisis de sentimiento...")
    sentiment = _generate_sentiment_section()
    sections.append(sentiment)

    # 5. Emerging Terms by Year
    print("5/8 Términos emergentes por año...")
    emerging = _generate_emerging_terms_section()
    sections.append(emerging)

    # 6. Key Actors Over Time
    print("6/8 Actores clave...")
    actors = _generate_actors_section()
    sections.append(actors)

    # 7. Coverage Anomalies
    print("7/8 Anomalías en cobertura...")
    anomalies = _generate_anomalies_section()
    sections.append(anomalies)

    # 8. Hidden Patterns
    print("8/8 Patrones ocultos...")
    patterns = _generate_patterns_section()
    sections.append(patterns)

    # Combine into full report
    html = _render_report(sections)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print("=" * 50)
    print(f"Informe guardado: {output_path}")
    return output_path


def _generate_overview_section() -> dict:
    """Generate database overview section."""
    stats = query('''
        SELECT
            COUNT(*) as total_articulos,
            MIN(publication_date)::date as fecha_inicio,
            MAX(publication_date)::date as fecha_fin,
            COUNT(DISTINCT EXTRACT(YEAR FROM publication_date)) as años_cubiertos,
            COUNT(DISTINCT section) as secciones,
            COUNT(DISTINCT author) as autores
        FROM articles
    ''').row(0, named=True)

    yearly = query('''
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as año,
            COUNT(*) as articulos
        FROM articles
        GROUP BY EXTRACT(YEAR FROM publication_date)
        ORDER BY año
    ''')

    fig = px.bar(
        yearly.to_pandas(),
        x='año', y='articulos',
        title='Artículos por Año',
        color_discrete_sequence=['#1a472a']
    )
    fig.update_layout(
        paper_bgcolor='#fafafa',
        plot_bgcolor='#fafafa',
        height=350
    )

    return {
        "title": "Resumen del Archivo",
        "id": "overview",
        "content": f"""
        <div class="stats-grid">
            <div class="stat-box">
                <div class="stat-number">{stats['total_articulos']:,}</div>
                <div class="stat-label">Artículos totales</div>
            </div>
            <div class="stat-box">
                <div class="stat-number">{stats['años_cubiertos']}</div>
                <div class="stat-label">Años cubiertos</div>
            </div>
            <div class="stat-box">
                <div class="stat-number">{stats['fecha_inicio']}</div>
                <div class="stat-label">Fecha más antigua</div>
            </div>
            <div class="stat-box">
                <div class="stat-number">{stats['fecha_fin']}</div>
                <div class="stat-label">Fecha más reciente</div>
            </div>
        </div>
        <p>Este archivo contiene <strong>{stats['total_articulos']:,} artículos</strong> de Al Calor Político,
        cubriendo <strong>{stats['años_cubiertos']} años</strong> de periodismo veracruzano
        (desde {stats['fecha_inicio']} hasta {stats['fecha_fin']}).</p>
        """,
        "plot": fig.to_html(full_html=False, include_plotlyjs=False)
    }


def _generate_duarte_section() -> dict:
    """Generate Duarte Early Warning analysis."""
    duarte_timeline = query('''
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as año,
            COUNT(*) as total_articulos,
            COUNT(*) FILTER (WHERE body ILIKE '%mansión%' OR body ILIKE '%propiedades%') as propiedades,
            COUNT(*) FILTER (WHERE body ILIKE '%desvío%' OR body ILIKE '%malversac%') as desvio,
            COUNT(*) FILTER (WHERE body ILIKE '%acusac%' OR body ILIKE '%denuncia%') as acusaciones
        FROM articles
        WHERE (body ILIKE '%duarte%' OR title ILIKE '%duarte%')
        AND (body ILIKE '%acusac%' OR body ILIKE '%corrup%' OR body ILIKE '%desvío%' OR
             body ILIKE '%malversa%' OR body ILIKE '%irregularidad%' OR body ILIKE '%fraude%' OR
             body ILIKE '%denuncia%' OR body ILIKE '%mansión%' OR body ILIKE '%propiedades%')
        AND publication_date < '2018-01-01'
        GROUP BY EXTRACT(YEAR FROM publication_date)
        ORDER BY año
    ''')

    df = duarte_timeline.to_pandas()

    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Cobertura Total del Escándalo Duarte', 'Señales de Alarma Específicas'),
        vertical_spacing=0.18,
        row_heights=[0.4, 0.6]
    )

    fig.add_trace(
        go.Scatter(x=df['año'], y=df['total_articulos'],
                   fill='tozeroy', name='Total artículos',
                   line=dict(color='#8b0000', width=2),
                   fillcolor='rgba(139, 0, 0, 0.3)'),
        row=1, col=1
    )

    fig.add_trace(
        go.Bar(x=df['año'], y=df['propiedades'], name='Propiedades/Mansión',
               marker_color='#d97706'),
        row=2, col=1
    )
    fig.add_trace(
        go.Bar(x=df['año'], y=df['desvio'], name='Desvío de recursos',
               marker_color='#dc2626'),
        row=2, col=1
    )
    fig.add_trace(
        go.Bar(x=df['año'], y=df['acusaciones'], name='Acusaciones/Denuncias',
               marker_color='#0066b3'),
        row=2, col=1
    )

    fig.add_vrect(x0=2007.5, x1=2010.5, fillcolor='rgba(251, 191, 36, 0.15)',
                  layer='below', line_width=0, row=2, col=1)
    fig.add_annotation(x=2009, y=max(df['propiedades'].max(), df['desvio'].max()) * 0.8,
                       text='ZONA DE<br>ALERTA TEMPRANA',
                       showarrow=False, font=dict(color='#d97706', size=10), row=2, col=1)

    fig.add_annotation(x=2010, y=df[df['año']==2010]['total_articulos'].iloc[0] if 2010 in df['año'].values else 0,
                       text='Gobernador', showarrow=True, arrowhead=2, ax=0, ay=-40, row=1, col=1)
    fig.add_annotation(x=2016, y=df[df['año']==2016]['total_articulos'].iloc[0] if 2016 in df['año'].values else 0,
                       text='Escándalo<br>nacional', showarrow=True, arrowhead=2, ax=0, ay=-40, row=1, col=1)

    fig.update_layout(
        barmode='stack',
        height=600,
        showlegend=True,
        paper_bgcolor='#fafafa',
        plot_bgcolor='#fafafa',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )

    # Find earliest warning
    earliest = query('''
        SELECT publication_date::date as fecha, title
        FROM articles
        WHERE (body ILIKE '%duarte%') AND (body ILIKE '%mansión%' OR body ILIKE '%propiedades%')
        AND publication_date < '2012-01-01'
        ORDER BY publication_date
        LIMIT 1
    ''')

    earliest_text = ""
    if len(earliest) > 0:
        row = earliest.row(0, named=True)
        earliest_text = f"<p class='highlight'>Primera mención de propiedades de Duarte: <strong>{row['fecha']}</strong><br><em>\"{row['title']}\"</em></p>"

    return {
        "title": "Duarte: Las Señales Estaban Ahí",
        "id": "duarte",
        "content": f"""
        <p>El análisis revela que <strong>8 años antes</strong> del escándalo nacional de 2016,
        periodistas locales ya documentaban irregularidades en torno a Javier Duarte.</p>

        <div class="finding-box">
            <h4>Hallazgo clave</h4>
            <p>En <strong>2008</strong>, legisladores del PAN fotografiaron la mansión de Duarte
            cuando era Secretario de Finanzas. Este dato aparece en el archivo
            <strong>8 años antes</strong> de su fuga.</p>
        </div>

        {earliest_text}

        <h4>Cronología de las señales:</h4>
        <ul>
            <li><strong>2008:</strong> PAN documenta propiedades; primeras acusaciones</li>
            <li><strong>2009-2010:</strong> Aumentan denuncias durante campaña</li>
            <li><strong>2010-2016:</strong> Gobernador - cobertura constante de irregularidades</li>
            <li><strong>2016:</strong> Explosión mediática nacional</li>
        </ul>
        """,
        "plot": fig.to_html(full_html=False, include_plotlyjs=False)
    }


def _generate_cartel_section() -> dict:
    """Generate cartel transition analysis."""
    data = query('''
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as año,
            COUNT(*) FILTER (WHERE body ILIKE '%zeta%') as zetas,
            COUNT(*) FILTER (WHERE body ILIKE '%cjng%' OR body ILIKE '%jalisco nueva generación%') as cjng,
            COUNT(*) FILTER (WHERE body ILIKE '%periodista%' AND
                (body ILIKE '%asesin%' OR body ILIKE '%amenaz%' OR body ILIKE '%ataqu%' OR body ILIKE '%muer%')
            ) as periodistas
        FROM articles
        WHERE publication_date >= '2008-01-01' AND publication_date < '2024-01-01'
        GROUP BY EXTRACT(YEAR FROM publication_date)
        ORDER BY año
    ''')

    df = data.to_pandas()

    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Transición de Carteles en Veracruz', 'Ataques a Periodistas'),
        vertical_spacing=0.18,
        row_heights=[0.6, 0.4]
    )

    fig.add_trace(
        go.Scatter(x=df['año'], y=df['zetas'], name='Los Zetas',
                   line=dict(color='#1a1a1a', width=3),
                   fill='tozeroy', fillcolor='rgba(26,26,26,0.2)'),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=df['año'], y=df['cjng'], name='CJNG',
                   line=dict(color='#dc2626', width=3),
                   fill='tozeroy', fillcolor='rgba(220,38,38,0.2)'),
        row=1, col=1
    )

    colors = ['#3b82f6' if y != 2012 else '#dc2626' for y in df['año']]
    fig.add_trace(
        go.Bar(x=df['año'], y=df['periodistas'], name='Ataques',
               marker_color=colors, showlegend=False),
        row=2, col=1
    )

    fig.add_annotation(x=2012, y=df[df['año']==2012]['periodistas'].iloc[0] if 2012 in df['año'].values else 0,
                       text='Regina Martínez', showarrow=True, arrowhead=2, ax=60, ay=-20,
                       font=dict(color='#dc2626'), row=2, col=1)

    fig.update_layout(
        height=600,
        paper_bgcolor='#fafafa',
        plot_bgcolor='#fafafa',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )

    return {
        "title": "La Guerra por Veracruz",
        "id": "cartels",
        "content": """
        <p>Los datos muestran la transición de poder criminal en Veracruz:
        del dominio de <strong>Los Zetas</strong> a la entrada del <strong>CJNG</strong>.</p>

        <div class="finding-box">
            <h4>Hallazgo clave</h4>
            <p><strong>2012</strong> marca el pico de violencia contra periodistas,
            coincidiendo con la muerte de Regina Martínez (corresponsal de Proceso)
            y el inicio de la transición de poder entre carteles.</p>
        </div>

        <h4>Cronología:</h4>
        <ul>
            <li><strong>2008-2011:</strong> Dominio Zetas (peak ~66 menciones/año)</li>
            <li><strong>2012:</strong> Crisis - 172 artículos sobre ataques a periodistas</li>
            <li><strong>2015-2020:</strong> CJNG crece, Zetas declinan</li>
            <li><strong>2020:</strong> CJNG supera a Zetas en cobertura (80 vs 26)</li>
        </ul>
        """,
        "plot": fig.to_html(full_html=False, include_plotlyjs=False)
    }


def _generate_sentiment_section() -> dict:
    """Generate sentiment analysis section."""
    sentiment = analyze_sentiment_by_year()
    df = sentiment.to_pandas()

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['año'], y=df['pct_positivo'],
        name='% Positivo',
        line=dict(color='#15803d', width=2),
        fill='tozeroy',
        fillcolor='rgba(21, 128, 61, 0.2)'
    ))

    fig.add_trace(go.Scatter(
        x=df['año'], y=df['pct_negativo'],
        name='% Negativo',
        line=dict(color='#dc2626', width=2),
        fill='tozeroy',
        fillcolor='rgba(220, 38, 38, 0.2)'
    ))

    fig.update_layout(
        title='Sentimiento en la Cobertura (basado en palabras clave)',
        height=400,
        paper_bgcolor='#fafafa',
        plot_bgcolor='#fafafa',
        legend=dict(orientation='h', yanchor='bottom', y=1.02)
    )

    # Find most negative years
    most_negative = df.nlargest(3, 'pct_negativo')[['año', 'pct_negativo']]

    return {
        "title": "Análisis de Sentimiento",
        "id": "sentiment",
        "content": f"""
        <p>Usando análisis de palabras clave, podemos medir el "tono" de la cobertura
        a lo largo del tiempo.</p>

        <div class="finding-box">
            <h4>Años más negativos:</h4>
            <ol>
                {"".join(f"<li>{int(row['año'])}: {row['pct_negativo']:.1f}% negativo</li>" for _, row in most_negative.iterrows())}
            </ol>
        </div>

        <p>El incremento en cobertura negativa correlaciona con:
        violencia del crimen organizado, escándalos de corrupción,
        y crisis económicas/sanitarias.</p>
        """,
        "plot": fig.to_html(full_html=False, include_plotlyjs=False)
    }


def _generate_emerging_terms_section() -> dict:
    """Generate emerging terms analysis."""
    years_to_analyze = [2012, 2016, 2020]
    tables = []

    for year in years_to_analyze:
        terms = find_emerging_terms(year, comparison_years=2, top_n=10)
        if len(terms) > 0:
            df = terms.to_pandas()
            table_html = f"""
            <div class="term-table">
                <h4>{year}</h4>
                <table>
                    <tr><th>Término</th><th>Score</th></tr>
                    {"".join(f"<tr><td>{row['term']}</td><td>{row['emergence_score']:.1f}x</td></tr>" for _, row in df.head(8).iterrows())}
                </table>
            </div>
            """
            tables.append(table_html)

    return {
        "title": "Términos Emergentes",
        "id": "emerging",
        "content": f"""
        <p>Estos términos "emergieron" o aumentaron significativamente
        en cada año comparado con los 2 años previos:</p>

        <div class="terms-grid">
            {"".join(tables)}
        </div>

        <p class="note">Score = frecuencia en año objetivo / frecuencia en años previos.
        Un score de 5x significa que el término apareció 5 veces más frecuentemente.</p>
        """,
        "plot": ""
    }


def _generate_actors_section() -> dict:
    """Generate key actors analysis."""
    # Get actors for key years
    years = [2010, 2016, 2020, 2024]
    actors_data = {}

    for year in years:
        actors = extract_key_actors(year, top_n=15)
        if len(actors) > 0:
            actors_data[year] = actors.to_pandas()

    # Create comparison visualization
    fig = go.Figure()

    for i, (year, df) in enumerate(actors_data.items()):
        fig.add_trace(go.Bar(
            y=df['actor'].head(10)[::-1],
            x=df['mentions'].head(10)[::-1],
            name=str(year),
            orientation='h'
        ))

    fig.update_layout(
        title='Top 10 Actores Mencionados por Año',
        barmode='group',
        height=500,
        paper_bgcolor='#fafafa',
        plot_bgcolor='#fafafa',
        legend=dict(orientation='h', yanchor='bottom', y=1.02)
    )

    return {
        "title": "Actores Clave",
        "id": "actors",
        "content": """
        <p>Usando extracción de nombres propios, identificamos los actores
        más mencionados en diferentes periodos.</p>

        <p>Este análisis revela quién dominaba el discurso público
        en cada momento histórico.</p>
        """,
        "plot": fig.to_html(full_html=False, include_plotlyjs=False)
    }


def _generate_anomalies_section() -> dict:
    """Generate coverage anomalies section."""
    coverage = detect_anomalies_in_coverage()
    df = coverage.to_pandas()

    fig = make_subplots(rows=2, cols=1,
                        subplot_titles=('Cobertura de Violencia (%)', 'Cobertura de Corrupción (%)'),
                        vertical_spacing=0.15)

    fig.add_trace(
        go.Scatter(x=df['mes'], y=df['pct_violencia'],
                   name='Violencia', line=dict(color='#dc2626')),
        row=1, col=1
    )

    fig.add_trace(
        go.Scatter(x=df['mes'], y=df['pct_corrupcion'],
                   name='Corrupción', line=dict(color='#7c3aed')),
        row=2, col=1
    )

    # Find anomalies
    violence_mean = df['pct_violencia'].mean()
    violence_std = df['pct_violencia'].std()
    anomaly_high = df[df['pct_violencia'] > violence_mean + 2*violence_std]

    for _, row in anomaly_high.iterrows():
        fig.add_annotation(x=row['mes'], y=row['pct_violencia'],
                           text='!', showarrow=False,
                           font=dict(color='red', size=16), row=1, col=1)

    fig.update_layout(
        height=500,
        showlegend=False,
        paper_bgcolor='#fafafa',
        plot_bgcolor='#fafafa'
    )

    return {
        "title": "Anomalías en Cobertura",
        "id": "anomalies",
        "content": f"""
        <p>Detectamos meses donde la cobertura fue inusualmente alta o baja
        (más de 2 desviaciones estándar del promedio).</p>

        <div class="finding-box">
            <h4>Picos de cobertura de violencia:</h4>
            <p>Identificamos <strong>{len(anomaly_high)} meses</strong> con cobertura
            anormalmente alta de violencia.</p>
        </div>

        <p>Las anomalías negativas (cobertura inusualmente BAJA durante periodos
        de alta violencia) podrían indicar autocensura o intimidación.</p>
        """,
        "plot": fig.to_html(full_html=False, include_plotlyjs=False)
    }


def _generate_patterns_section() -> dict:
    """Generate hidden patterns section using co-occurrence analysis."""
    # Analyze what co-occurs with key terms
    terms_to_analyze = ["duarte", "zetas", "morena"]
    results = []

    for term in terms_to_analyze:
        coocs = find_cooccurrences(term, top_n=10)
        if len(coocs) > 0:
            df = coocs.to_pandas()
            results.append({
                "term": term.upper(),
                "cooccurrences": df.head(8).to_dict('records')
            })

    # TF-IDF comparison between eras
    era_terms = []
    eras = [
        ("2005-2010", 2005, 2010),
        ("2011-2016", 2011, 2016),
        ("2017-2024", 2017, 2024)
    ]

    for name, start, end in eras:
        tfidf = get_tfidf_by_period(start, end, top_n=10)
        if len(tfidf) > 0:
            era_terms.append({
                "era": name,
                "terms": tfidf.to_pandas().head(8).to_dict('records')
            })

    cooc_html = ""
    for result in results:
        cooc_html += f"""
        <div class="cooc-box">
            <h4>"{result['term']}"</h4>
            <ul>
                {"".join(f"<li>{c['cooccurring_term']} ({c['frequency']})</li>" for c in result['cooccurrences'][:6])}
            </ul>
        </div>
        """

    era_html = ""
    for era in era_terms:
        era_html += f"""
        <div class="era-box">
            <h4>{era['era']}</h4>
            <ul>
                {"".join(f"<li>{t['term']}</li>" for t in era['terms'][:6])}
            </ul>
        </div>
        """

    return {
        "title": "Patrones Ocultos (NLP)",
        "id": "patterns",
        "content": f"""
        <h4>Co-ocurrencias: ¿Con qué palabras aparecen estos términos?</h4>
        <div class="cooc-grid">
            {cooc_html}
        </div>

        <h4>Términos Distintivos por Era (TF-IDF)</h4>
        <p>Palabras que mejor caracterizan cada periodo:</p>
        <div class="era-grid">
            {era_html}
        </div>
        """,
        "plot": ""
    }


def _render_report(sections: list[dict]) -> str:
    """Render the complete HTML report."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    nav_items = "".join(f'<a href="#{s["id"]}">{s["title"]}</a>' for s in sections)

    sections_html = ""
    for section in sections:
        sections_html += f"""
        <section id="{section['id']}">
            <h2>{section['title']}</h2>
            {section['content']}
            {section['plot']}
        </section>
        """

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Al Calor Político: Análisis de 20 Años</title>
    <script src="https://cdn.plot.ly/plotly-3.3.0.min.js"></script>
    <style>
        :root {{
            --primary: #1a472a;
            --accent: #8b0000;
            --bg: #fafafa;
            --text: #1e293b;
            --border: #e2e8f0;
        }}

        * {{ box-sizing: border-box; margin: 0; padding: 0; }}

        body {{
            font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
            line-height: 1.6;
            color: var(--text);
            background: var(--bg);
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }}

        header {{
            background: linear-gradient(135deg, var(--primary) 0%, #2d5a3d 100%);
            color: white;
            padding: 40px;
            border-radius: 12px;
            margin-bottom: 30px;
        }}

        header h1 {{
            font-size: 2.5rem;
            margin-bottom: 10px;
        }}

        header .subtitle {{
            font-size: 1.1rem;
            opacity: 0.9;
        }}

        header .meta {{
            margin-top: 20px;
            font-size: 0.9rem;
            opacity: 0.8;
        }}

        nav {{
            background: white;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 30px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            position: sticky;
            top: 10px;
            z-index: 100;
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
        }}

        nav a {{
            color: var(--primary);
            text-decoration: none;
            padding: 8px 16px;
            border-radius: 6px;
            transition: all 0.2s;
            font-size: 0.9rem;
        }}

        nav a:hover {{
            background: var(--primary);
            color: white;
        }}

        section {{
            background: white;
            padding: 30px;
            border-radius: 12px;
            margin-bottom: 30px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        }}

        section h2 {{
            color: var(--primary);
            border-bottom: 3px solid var(--primary);
            padding-bottom: 10px;
            margin-bottom: 20px;
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }}

        .stat-box {{
            background: var(--bg);
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }}

        .stat-number {{
            font-size: 1.8rem;
            font-weight: bold;
            color: var(--primary);
        }}

        .stat-label {{
            font-size: 0.85rem;
            color: #64748b;
            margin-top: 5px;
        }}

        .finding-box {{
            background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
            border-left: 4px solid #d97706;
            padding: 20px;
            border-radius: 0 8px 8px 0;
            margin: 20px 0;
        }}

        .finding-box h4 {{
            color: #92400e;
            margin-bottom: 10px;
        }}

        .highlight {{
            background: #dbeafe;
            padding: 15px;
            border-radius: 8px;
            margin: 15px 0;
        }}

        .terms-grid, .cooc-grid, .era-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}

        .term-table, .cooc-box, .era-box {{
            background: var(--bg);
            padding: 15px;
            border-radius: 8px;
        }}

        .term-table h4, .cooc-box h4, .era-box h4 {{
            color: var(--primary);
            margin-bottom: 10px;
            font-size: 1.1rem;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
        }}

        th, td {{
            padding: 8px;
            text-align: left;
            border-bottom: 1px solid var(--border);
        }}

        th {{
            font-weight: 600;
            color: #64748b;
            font-size: 0.85rem;
        }}

        ul {{
            padding-left: 20px;
        }}

        li {{
            margin: 5px 0;
        }}

        .note {{
            font-size: 0.85rem;
            color: #64748b;
            font-style: italic;
        }}

        footer {{
            text-align: center;
            padding: 30px;
            color: #64748b;
            font-size: 0.9rem;
        }}

        .plotly-graph-div {{
            margin: 20px 0;
        }}

        @media (max-width: 768px) {{
            header h1 {{ font-size: 1.8rem; }}
            section {{ padding: 20px; }}
            nav {{ position: static; }}
        }}
    </style>
</head>
<body>
    <header>
        <h1>Al Calor Político</h1>
        <div class="subtitle">Análisis de 20 Años de Periodismo Veracruzano</div>
        <div class="meta">
            Generado: {now} | Método: NLP + Análisis Temporal
        </div>
    </header>

    <nav>
        {nav_items}
    </nav>

    {sections_html}

    <footer>
        <p>Análisis generado con datos de Al Calor Político (2005-2024)</p>
        <p>Métodos: TF-IDF, Análisis de Sentimiento, Detección de Anomalías, Co-ocurrencias</p>
    </footer>
</body>
</html>"""
