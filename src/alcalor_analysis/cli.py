"""
Command-line interface for Alcalor Analysis.

Usage:
    alcalor stats          # Show database statistics
    alcalor parties        # Generate party evolution plot
    alcalor crime          # Generate crime trends plot
    alcalor covid          # Generate COVID timeline
    alcalor duarte         # Generate Duarte scandal analysis
    alcalor dashboard      # Generate full dashboard
    alcalor all            # Generate all plots
"""

import argparse
import sys
from pathlib import Path

from .db import get_stats
from .viz import plots


def cmd_stats():
    """Print database statistics."""
    stats = get_stats()
    print("\n" + "=" * 60)
    print("📊 AL CALOR POLÍTICO - ESTADÍSTICAS")
    print("=" * 60)
    print(f"   Total de artículos: {stats['total_articles']:,}")
    print(f"   Período: {stats['earliest_date']} → {stats['latest_date']}")
    print(f"   Secciones únicas: {stats['unique_sections']}")
    print(f"   Longitud promedio: {stats['avg_body_length']:,} caracteres")
    print("=" * 60 + "\n")


def cmd_generate(plot_name: str, output_dir: Path, format: str = "html"):
    """Generate a specific plot."""
    output_dir.mkdir(exist_ok=True)

    plot_functions = {
        "parties": ("plot_party_evolution", "partidos_politicos"),
        "presidents": ("plot_presidential_transitions", "presidentes"),
        "crime": ("plot_crime_trends", "tendencias_violencia"),
        "cartels": ("plot_cartel_evolution", "crimen_organizado"),
        "duarte": ("plot_duarte_scandal", "escandalo_duarte"),
        "covid": ("plot_covid_timeline", "covid19"),
        "disasters": ("plot_natural_disasters", "desastres_naturales"),
        "economic": ("plot_economic_indicators", "indicadores_economicos"),
        "timeline": ("plot_articles_timeline", "volumen_publicacion"),
        "weekday": ("plot_day_of_week", "dias_semana"),
        "dashboard": ("plot_overview_dashboard", "dashboard_resumen"),
    }

    if plot_name not in plot_functions:
        print(f"❌ Plot desconocido: {plot_name}")
        print(f"   Opciones: {', '.join(plot_functions.keys())}")
        return False

    func_name, filename = plot_functions[plot_name]
    func = getattr(plots, func_name)

    print(f"📈 Generando: {plot_name}...")
    fig = func()

    output_path = output_dir / filename
    if format == "html":
        fig.write_html(f"{output_path}.html", include_plotlyjs="cdn")
        print(f"   ✅ Guardado: {output_path}.html")
    elif format == "png":
        fig.write_image(f"{output_path}.png", scale=2, width=1200, height=700)
        print(f"   ✅ Guardado: {output_path}.png")

    return True


def cmd_all(output_dir: Path, format: str = "html"):
    """Generate all plots."""
    plot_names = [
        "parties", "presidents", "crime", "cartels", "duarte",
        "covid", "disasters", "economic", "timeline", "weekday", "dashboard"
    ]

    print("\n🚀 Generando todos los gráficos...\n")

    for name in plot_names:
        try:
            cmd_generate(name, output_dir, format)
        except Exception as e:
            print(f"   ❌ Error en {name}: {e}")

    print(f"\n✅ ¡Listo! Gráficos guardados en: {output_dir}/\n")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Análisis de Al Calor Político - 20 años de noticias de Veracruz",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  alcalor stats                    # Ver estadísticas
  alcalor generate parties         # Generar gráfico de partidos
  alcalor generate all             # Generar todos los gráficos
  alcalor generate crime --png     # Generar en PNG
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Comandos disponibles")

    # Stats command
    subparsers.add_parser("stats", help="Mostrar estadísticas de la base de datos")

    # Generate command
    gen_parser = subparsers.add_parser("generate", help="Generar visualizaciones")
    gen_parser.add_argument(
        "plot",
        choices=[
            "parties", "presidents", "crime", "cartels", "duarte",
            "covid", "disasters", "economic", "timeline", "weekday",
            "dashboard", "all"
        ],
        help="Gráfico a generar (o 'all' para todos)",
    )
    gen_parser.add_argument(
        "-o", "--output",
        type=Path,
        default=Path("plots"),
        help="Directorio de salida (default: plots/)",
    )
    gen_parser.add_argument(
        "--png",
        action="store_true",
        help="Exportar como PNG en lugar de HTML",
    )

    args = parser.parse_args()

    if args.command == "stats":
        cmd_stats()
    elif args.command == "generate":
        format = "png" if args.png else "html"
        if args.plot == "all":
            cmd_all(args.output, format)
        else:
            cmd_generate(args.plot, args.output, format)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
