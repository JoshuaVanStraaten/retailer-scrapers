"""Configuration module for Savvy Grocery Scraper."""

from .settings import (
    Config,
    config,
    SupabaseConfig,
    DiscordConfig,
    ScraperConfig,
    PlaywrightConfig,
    CHECKERS_STORES,
    SHOPRITE_STORES,
    PNP_STORES,
    WOOLWORTHS_STORES,
    WOOLWORTHS_CATEGORIES,
    INDEX_RANGES,
    PLACEHOLDER_IMAGES,
    USER_AGENTS,
)

__all__ = [
    "Config",
    "config",
    "SupabaseConfig",
    "DiscordConfig",
    "ScraperConfig",
    "PlaywrightConfig",
    "CHECKERS_STORES",
    "SHOPRITE_STORES",
    "PNP_STORES",
    "WOOLWORTHS_STORES",
    "WOOLWORTHS_CATEGORIES",
    "INDEX_RANGES",
    "PLACEHOLDER_IMAGES",
    "USER_AGENTS",
]
