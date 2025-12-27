"""
Alcalor Político Analysis - Deep Mexico/Veracruz News Analysis

A comprehensive toolkit for analyzing 20+ years of Mexican news data
from Al Calor Político, Veracruz's leading digital newspaper.
"""

__version__ = "0.1.0"

from .db import get_connection, DATABASE_URL
from .queries import political, security, disasters, economic, temporal
from .viz import theme, plots

__all__ = [
    "get_connection",
    "DATABASE_URL",
    "political",
    "security",
    "disasters",
    "economic",
    "temporal",
    "theme",
    "plots",
]
