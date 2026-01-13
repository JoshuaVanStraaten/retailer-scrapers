"""Scraper modules for Savvy Grocery."""

from .base import BaseScraper, ScrapeResult, ScraperStats
from .checkers import CheckersScraper, run_checkers_scraper
from .shoprite import ShopriteScraper, run_shoprite_scraper
from .pnp import PnPScraper, run_pnp_scraper
from .woolworths import WoolworthsScraper, run_woolworths_scraper

__all__ = [
    "BaseScraper",
    "ScrapeResult",
    "ScraperStats",
    "CheckersScraper",
    "run_checkers_scraper",
    "ShopriteScraper",
    "run_shoprite_scraper",
    "PnPScraper",
    "run_pnp_scraper",
    "WoolworthsScraper",
    "run_woolworths_scraper",
]