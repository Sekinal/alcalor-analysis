"""
NLP Analysis Module for Al Calor Político articles.

Uses basic NLP methods to find hidden patterns:
- TF-IDF for important terms by period
- Sentiment analysis (keyword-based)
- N-gram analysis
- Co-occurrence patterns
"""

import re
from collections import Counter
from typing import Optional

import polars as pl
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
import numpy as np

from .db import query


# Spanish stopwords (extended list)
STOPWORDS_ES = {
    "de", "la", "que", "el", "en", "y", "a", "los", "del", "se", "las", "por",
    "un", "para", "con", "no", "una", "su", "al", "lo", "como", "más", "pero",
    "sus", "le", "ya", "o", "este", "sí", "porque", "esta", "entre", "cuando",
    "muy", "sin", "sobre", "también", "me", "hasta", "hay", "donde", "quien",
    "desde", "todo", "nos", "durante", "todos", "uno", "les", "ni", "contra",
    "otros", "ese", "eso", "ante", "ellos", "e", "esto", "mí", "antes", "algunos",
    "qué", "unos", "yo", "otro", "otras", "otra", "él", "tanto", "esa", "estos",
    "mucho", "quienes", "nada", "muchos", "cual", "poco", "ella", "estar", "estas",
    "algunas", "algo", "nosotros", "mi", "mis", "tú", "te", "ti", "tu", "tus",
    "ellas", "nosotras", "vosotros", "vosotras", "os", "mío", "mía", "míos", "mías",
    "tuyo", "tuya", "tuyos", "tuyas", "suyo", "suya", "suyos", "suyas", "nuestro",
    "nuestra", "nuestros", "nuestras", "vuestro", "vuestra", "vuestros", "vuestras",
    "esos", "esas", "estoy", "estás", "está", "estamos", "estáis", "están", "esté",
    "estés", "estemos", "estéis", "estén", "estaré", "estarás", "estará", "estaremos",
    "estaréis", "estarán", "estaría", "estarías", "estaríamos", "estaríais", "estarían",
    "estaba", "estabas", "estábamos", "estabais", "estaban", "estuve", "estuviste",
    "estuvo", "estuvimos", "estuvisteis", "estuvieron", "estuviera", "estuvieras",
    "estuviéramos", "estuvierais", "estuvieran", "estuviese", "estuvieses",
    "estuviésemos", "estuvieseis", "estuviesen", "estando", "estado", "estada",
    "estados", "estadas", "estad", "he", "has", "ha", "hemos", "habéis", "han",
    "haya", "hayas", "hayamos", "hayáis", "hayan", "habré", "habrás", "habrá",
    "habremos", "habréis", "habrán", "habría", "habrías", "habríamos", "habríais",
    "habrían", "había", "habías", "habíamos", "habíais", "habían", "hube", "hubiste",
    "hubo", "hubimos", "hubisteis", "hubieron", "hubiera", "hubieras", "hubiéramos",
    "hubierais", "hubieran", "hubiese", "hubieses", "hubiésemos", "hubieseis",
    "hubiesen", "habiendo", "habido", "habida", "habidos", "habidas", "soy", "eres",
    "es", "somos", "sois", "son", "sea", "seas", "seamos", "seáis", "sean", "seré",
    "serás", "será", "seremos", "seréis", "serán", "sería", "serías", "seríamos",
    "seríais", "serían", "era", "eras", "éramos", "erais", "eran", "fui", "fuiste",
    "fue", "fuimos", "fuisteis", "fueron", "fuera", "fueras", "fuéramos", "fuerais",
    "fueran", "fuese", "fueses", "fuésemos", "fueseis", "fuesen", "siendo", "sido",
    "tengo", "tienes", "tiene", "tenemos", "tenéis", "tienen", "tenga", "tengas",
    "tengamos", "tengáis", "tengan", "tendré", "tendrás", "tendrá", "tendremos",
    "tendréis", "tendrán", "tendría", "tendrías", "tendríamos", "tendríais", "tendrían",
    "tenía", "tenías", "teníamos", "teníais", "tenían", "tuve", "tuviste", "tuvo",
    "tuvimos", "tuvisteis", "tuvieron", "tuviera", "tuvieras", "tuviéramos", "tuvierais",
    "tuvieran", "tuviese", "tuvieses", "tuviésemos", "tuvieseis", "tuviesen", "teniendo",
    "tenido", "tenida", "tenidos", "tenidas", "tened", "así", "cada", "hacer", "hecho",
    "sido", "puede", "pueden", "podría", "dijo", "señaló", "indicó", "afirmó", "explicó",
    "comentó", "añadió", "aseguró", "manifestó", "expresó", "destacó", "informó", "dijo",
    "año", "años", "día", "días", "vez", "veces", "parte", "además", "ahora", "después",
    "dos", "tres", "primer", "primera", "segundo", "nueva", "nuevo", "solo", "tras",
    "siempre", "menos", "según", "ser", "sido", "ver", "hacer", "ir", "dar", "decir",
    "solo", "mismo", "misma", "mismos", "mismas", "luego", "bien", "manera", "forma",
    "caso", "hecho", "entonces", "mientras", "aunque", "embargo", "debe", "hacia",
    "pues", "pasado", "sido", "haber", "través", "medio", "cuenta", "punto", "general",
    "tan", "sido", "vez", "veces", "ciento", "mil", "millones", "pesos", "ciudad"
}

# Sentiment lexicons (simplified Spanish)
POSITIVE_WORDS = {
    "éxito", "exito", "logro", "lograr", "avance", "mejora", "mejorar", "beneficio",
    "beneficiar", "positivo", "crecimiento", "crecer", "desarrollo", "desarrollar",
    "apoyo", "apoyar", "acuerdo", "colaboración", "colaborar", "progreso", "progresar",
    "inversión", "invertir", "oportunidad", "solución", "resolver", "victoria",
    "ganar", "celebrar", "celebración", "inaugurar", "inauguración", "reconocimiento",
    "reconocer", "premio", "premiar", "felicitar", "felicitación", "bienestar",
    "esperanza", "optimismo", "optimista", "satisfacción", "satisfecho", "excelente",
    "extraordinario", "notable", "destacado", "sobresaliente", "impresionante",
    "maravilloso", "fantástico", "increíble", "espectacular", "triunfo", "triunfar"
}

NEGATIVE_WORDS = {
    "crisis", "problema", "problemático", "conflicto", "violencia", "violento",
    "muerte", "muerto", "matar", "asesinar", "asesinato", "homicidio", "ejecutar",
    "ejecutado", "secuestro", "secuestrar", "robo", "robar", "asalto", "asaltar",
    "corrupción", "corrupto", "fraude", "fraudulento", "desvío", "malversación",
    "escándalo", "acusación", "acusar", "denunciar", "denuncia", "delito", "crimen",
    "criminal", "narcotráfico", "narco", "cartel", "zetas", "cjng", "balacera",
    "enfrentamiento", "inseguridad", "peligro", "peligroso", "amenaza", "amenazar",
    "extorsión", "extorsionar", "desaparición", "desaparecer", "desaparecido",
    "víctima", "tragedia", "trágico", "desastre", "devastación", "destrucción",
    "destruir", "daño", "dañar", "pérdida", "perder", "fracaso", "fracasar",
    "rechazo", "rechazar", "protesta", "protestar", "manifestación", "bloqueo",
    "bloquear", "huelga", "paro", "negligencia", "negligente", "incompetencia",
    "incompetente", "impunidad", "injusticia", "ilegal", "irregularidad"
}


def clean_text(text: str) -> str:
    """Clean and normalize text for analysis."""
    if not text:
        return ""
    # Lowercase
    text = text.lower()
    # Remove URLs
    text = re.sub(r'http\S+|www\S+', '', text)
    # Remove email addresses
    text = re.sub(r'\S+@\S+', '', text)
    # Remove numbers
    text = re.sub(r'\d+', '', text)
    # Remove special characters but keep accents
    text = re.sub(r'[^\w\sáéíóúüñ]', ' ', text)
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def get_tfidf_by_period(start_year: int, end_year: int, top_n: int = 20) -> pl.DataFrame:
    """
    Get top TF-IDF terms for a specific time period.

    Returns terms that are distinctively important for that period.
    """
    # Get articles from period
    articles = query(f'''
        SELECT body
        FROM articles
        WHERE EXTRACT(YEAR FROM publication_date) BETWEEN {start_year} AND {end_year}
        AND LENGTH(body) > 100
        LIMIT 10000
    ''')

    if len(articles) == 0:
        return pl.DataFrame({"term": [], "tfidf_score": []})

    texts = [clean_text(row["body"]) for row in articles.iter_rows(named=True)]

    vectorizer = TfidfVectorizer(
        max_features=1000,
        stop_words=list(STOPWORDS_ES),
        min_df=5,
        max_df=0.7,
        ngram_range=(1, 2)
    )

    try:
        tfidf_matrix = vectorizer.fit_transform(texts)
        feature_names = vectorizer.get_feature_names_out()

        # Average TF-IDF scores across documents
        avg_scores = np.asarray(tfidf_matrix.mean(axis=0)).flatten()
        top_indices = avg_scores.argsort()[-top_n:][::-1]

        return pl.DataFrame({
            "term": [feature_names[i] for i in top_indices],
            "tfidf_score": [float(avg_scores[i]) for i in top_indices]
        })
    except Exception as e:
        print(f"TF-IDF error: {e}")
        return pl.DataFrame({"term": [], "tfidf_score": []})


def analyze_sentiment_by_year() -> pl.DataFrame:
    """
    Analyze sentiment trends over time using keyword-based approach.
    """
    sentiment_data = query('''
        SELECT
            EXTRACT(YEAR FROM publication_date)::int as año,
            COUNT(*) as total_articulos,
            COUNT(*) FILTER (WHERE
                body ILIKE '%éxito%' OR body ILIKE '%logro%' OR body ILIKE '%avance%' OR
                body ILIKE '%mejora%' OR body ILIKE '%beneficio%' OR body ILIKE '%crecimiento%' OR
                body ILIKE '%desarrollo%' OR body ILIKE '%inversión%' OR body ILIKE '%progreso%' OR
                body ILIKE '%victoria%' OR body ILIKE '%celebra%' OR body ILIKE '%inaugura%'
            ) as articulos_positivos,
            COUNT(*) FILTER (WHERE
                body ILIKE '%crisis%' OR body ILIKE '%problema%' OR body ILIKE '%violencia%' OR
                body ILIKE '%muerte%' OR body ILIKE '%asesin%' OR body ILIKE '%secuestr%' OR
                body ILIKE '%corrupción%' OR body ILIKE '%fraude%' OR body ILIKE '%escándalo%' OR
                body ILIKE '%denuncia%' OR body ILIKE '%delito%' OR body ILIKE '%crimen%' OR
                body ILIKE '%narcotráfico%' OR body ILIKE '%cartel%' OR body ILIKE '%inseguridad%'
            ) as articulos_negativos
        FROM articles
        GROUP BY EXTRACT(YEAR FROM publication_date)
        ORDER BY año
    ''')

    # Calculate ratios
    df = sentiment_data.with_columns([
        (pl.col("articulos_positivos") / pl.col("total_articulos") * 100).alias("pct_positivo"),
        (pl.col("articulos_negativos") / pl.col("total_articulos") * 100).alias("pct_negativo"),
        ((pl.col("articulos_positivos") - pl.col("articulos_negativos")) / pl.col("total_articulos") * 100).alias("balance_sentimiento")
    ])

    return df


def find_emerging_terms(year: int, comparison_years: int = 3, top_n: int = 15) -> pl.DataFrame:
    """
    Find terms that emerged or spiked in a specific year compared to previous years.

    This helps identify new topics, events, or phenomena.
    """
    # Get term frequencies for target year
    target_articles = query(f'''
        SELECT body
        FROM articles
        WHERE EXTRACT(YEAR FROM publication_date) = {year}
        AND LENGTH(body) > 100
        LIMIT 8000
    ''')

    # Get term frequencies for comparison period
    comparison_articles = query(f'''
        SELECT body
        FROM articles
        WHERE EXTRACT(YEAR FROM publication_date) BETWEEN {year - comparison_years} AND {year - 1}
        AND LENGTH(body) > 100
        LIMIT 15000
    ''')

    if len(target_articles) == 0 or len(comparison_articles) == 0:
        return pl.DataFrame({"term": [], "emergence_score": [], "target_freq": [], "baseline_freq": []})

    target_texts = [clean_text(row["body"]) for row in target_articles.iter_rows(named=True)]
    comparison_texts = [clean_text(row["body"]) for row in comparison_articles.iter_rows(named=True)]

    vectorizer = CountVectorizer(
        max_features=2000,
        stop_words=list(STOPWORDS_ES),
        min_df=3,
        ngram_range=(1, 2)
    )

    try:
        # Fit on combined corpus
        all_texts = target_texts + comparison_texts
        vectorizer.fit(all_texts)

        target_matrix = vectorizer.transform(target_texts)
        comparison_matrix = vectorizer.transform(comparison_texts)

        feature_names = vectorizer.get_feature_names_out()

        # Calculate frequencies (normalized by document count)
        target_freq = np.asarray(target_matrix.sum(axis=0)).flatten() / len(target_texts)
        comparison_freq = np.asarray(comparison_matrix.sum(axis=0)).flatten() / len(comparison_texts)

        # Calculate emergence score (ratio with smoothing)
        emergence_score = (target_freq + 0.001) / (comparison_freq + 0.001)

        # Filter for terms that actually appear enough in target year
        min_target_freq = 0.01
        valid_indices = np.where(target_freq > min_target_freq)[0]

        # Sort by emergence score
        sorted_indices = valid_indices[emergence_score[valid_indices].argsort()[::-1]][:top_n]

        return pl.DataFrame({
            "term": [feature_names[i] for i in sorted_indices],
            "emergence_score": [float(emergence_score[i]) for i in sorted_indices],
            "target_freq": [float(target_freq[i]) for i in sorted_indices],
            "baseline_freq": [float(comparison_freq[i]) for i in sorted_indices]
        })
    except Exception as e:
        print(f"Emerging terms error: {e}")
        return pl.DataFrame({"term": [], "emergence_score": [], "target_freq": [], "baseline_freq": []})


def find_cooccurrences(term: str, year: Optional[int] = None, top_n: int = 20) -> pl.DataFrame:
    """
    Find terms that frequently co-occur with a given term.

    Useful for understanding context and associations.
    """
    year_filter = f"AND EXTRACT(YEAR FROM publication_date) = {year}" if year else ""

    articles = query(f'''
        SELECT body
        FROM articles
        WHERE body ILIKE '%{term}%'
        {year_filter}
        LIMIT 5000
    ''')

    if len(articles) == 0:
        return pl.DataFrame({"cooccurring_term": [], "frequency": []})

    texts = [clean_text(row["body"]) for row in articles.iter_rows(named=True)]

    vectorizer = CountVectorizer(
        max_features=500,
        stop_words=list(STOPWORDS_ES),
        min_df=3,
        ngram_range=(1, 1)
    )

    try:
        count_matrix = vectorizer.fit_transform(texts)
        feature_names = vectorizer.get_feature_names_out()

        # Sum frequencies
        frequencies = np.asarray(count_matrix.sum(axis=0)).flatten()

        # Remove the search term itself
        term_lower = term.lower()
        valid_indices = [i for i, name in enumerate(feature_names) if term_lower not in name]

        # Sort by frequency
        sorted_indices = sorted(valid_indices, key=lambda i: frequencies[i], reverse=True)[:top_n]

        return pl.DataFrame({
            "cooccurring_term": [feature_names[i] for i in sorted_indices],
            "frequency": [int(frequencies[i]) for i in sorted_indices]
        })
    except Exception as e:
        print(f"Co-occurrence error: {e}")
        return pl.DataFrame({"cooccurring_term": [], "frequency": []})


def detect_anomalies_in_coverage() -> pl.DataFrame:
    """
    Detect unusual patterns in coverage - sudden spikes or drops in topics.
    """
    # Get monthly topic coverage
    coverage = query('''
        SELECT
            DATE_TRUNC('month', publication_date)::date as mes,
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE body ILIKE '%violencia%' OR body ILIKE '%homicidio%') as violencia,
            COUNT(*) FILTER (WHERE body ILIKE '%corrupción%' OR body ILIKE '%fraude%') as corrupcion,
            COUNT(*) FILTER (WHERE body ILIKE '%economía%' OR body ILIKE '%empleo%' OR body ILIKE '%inversión%') as economia,
            COUNT(*) FILTER (WHERE body ILIKE '%salud%' OR body ILIKE '%hospital%' OR body ILIKE '%médico%') as salud,
            COUNT(*) FILTER (WHERE body ILIKE '%educación%' OR body ILIKE '%escuela%' OR body ILIKE '%universidad%') as educacion
        FROM articles
        GROUP BY DATE_TRUNC('month', publication_date)
        ORDER BY mes
    ''')

    # Calculate ratios and find anomalies
    df = coverage.with_columns([
        (pl.col("violencia") / pl.col("total") * 100).alias("pct_violencia"),
        (pl.col("corrupcion") / pl.col("total") * 100).alias("pct_corrupcion"),
        (pl.col("economia") / pl.col("total") * 100).alias("pct_economia"),
        (pl.col("salud") / pl.col("total") * 100).alias("pct_salud"),
        (pl.col("educacion") / pl.col("total") * 100).alias("pct_educacion")
    ])

    return df


def extract_key_actors(year: int, top_n: int = 30) -> pl.DataFrame:
    """
    Extract frequently mentioned actors (people, organizations) for a year.
    Uses capitalized word patterns as a simple NER approach.
    """
    articles = query(f'''
        SELECT title, body
        FROM articles
        WHERE EXTRACT(YEAR FROM publication_date) = {year}
        LIMIT 10000
    ''')

    # Pattern for potential names (two capitalized words)
    name_pattern = re.compile(r'\b([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)\b')

    name_counts: Counter = Counter()

    # Common false positives to filter
    false_positives = {
        "Boca Del", "Del Río", "De La", "La Cruz", "El Puerto", "Las Vegas",
        "Los Angeles", "San Juan", "Nueva York", "Estados Unidos", "Al Calor",
        "Calor Político", "Monte Alto", "Paso Del", "Zona Norte", "Zona Centro"
    }

    for row in articles.iter_rows(named=True):
        text = (row.get("title", "") or "") + " " + (row.get("body", "") or "")
        matches = name_pattern.findall(text)
        for first, last in matches:
            name = f"{first} {last}"
            if name not in false_positives and len(first) > 2 and len(last) > 2:
                name_counts[name] += 1

    top_names = name_counts.most_common(top_n)

    return pl.DataFrame({
        "actor": [name for name, _ in top_names],
        "mentions": [count for _, count in top_names]
    })
