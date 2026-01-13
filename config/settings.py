"""
Savvy Grocery Scraper - Centralized Configuration

All settings are loaded from environment variables with sensible defaults.
Create a .env file in the project root for local development.
"""

import os
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, rely on actual environment variables


@dataclass
class SupabaseConfig:
    """Supabase connection settings."""
    url: str = field(default_factory=lambda: os.getenv("SUPABASE_URL", ""))
    key: str = field(default_factory=lambda: os.getenv("SUPABASE_KEY", ""))
    bucket_name: str = field(default_factory=lambda: os.getenv("SUPABASE_BUCKET", "product_images"))

    def validate(self) -> bool:
        """Check if required credentials are set."""
        if not self.url or not self.key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_KEY environment variables must be set. "
                "Create a .env file or set them in your environment."
            )
        return True


@dataclass
class DiscordConfig:
    """Discord webhook settings for notifications."""
    webhook_url: str = field(default_factory=lambda: os.getenv("DISCORD_WEBHOOK_URL", ""))
    enabled: bool = field(default_factory=lambda: os.getenv("DISCORD_NOTIFICATIONS_ENABLED", "true").lower() == "true")

    def is_configured(self) -> bool:
        """Check if Discord notifications are properly configured."""
        return bool(self.webhook_url) and self.enabled


@dataclass
class ScraperConfig:
    """General scraper settings."""
    # Threading
    max_threads: int = field(default_factory=lambda: int(os.getenv("MAX_THREADS", "4")))

    # Timeouts and delays
    request_timeout: int = field(default_factory=lambda: int(os.getenv("REQUEST_TIMEOUT", "30")))
    page_delay: float = field(default_factory=lambda: float(os.getenv("PAGE_DELAY", "2.0")))
    retry_delay_base: float = field(default_factory=lambda: float(os.getenv("RETRY_DELAY_BASE", "2.0")))
    max_retries: int = field(default_factory=lambda: int(os.getenv("MAX_RETRIES", "3")))

    # Image handling
    delete_local_images: bool = field(
        default_factory=lambda: os.getenv("DELETE_LOCAL_IMAGES", "true").lower() == "true"
    )
    use_cdn_urls: bool = field(
        default_factory=lambda: os.getenv("USE_CDN_URLS", "false").lower() == "true"
    )

    # Paths
    data_dir: Path = field(default_factory=lambda: Path(os.getenv("DATA_DIR", "./data")))
    log_dir: Path = field(default_factory=lambda: Path(os.getenv("LOG_DIR", "./logs")))
    image_dir: Path = field(default_factory=lambda: Path(os.getenv("IMAGE_DIR", "./images")))

    # Batch sizes
    supabase_batch_size: int = field(default_factory=lambda: int(os.getenv("SUPABASE_BATCH_SIZE", "500")))

    def ensure_directories(self):
        """Create required directories if they don't exist."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.image_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class PlaywrightConfig:
    """Playwright browser settings for session management."""
    headless: bool = field(default_factory=lambda: os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true")
    slow_mo: int = field(default_factory=lambda: int(os.getenv("PLAYWRIGHT_SLOW_MO", "0")))
    timeout: int = field(default_factory=lambda: int(os.getenv("PLAYWRIGHT_TIMEOUT", "30000")))


# Store configurations for each retailer
CHECKERS_STORES = {
    "6551": "Western Cape",
    "7303": "Gauteng",
    "40206": "KwaZulu-Natal",
    "52798": "Northern Cape",
    "30562": "Eastern Cape",
    "7727": "Free State",
    "52772": "North West",
    "85363": "Limpopo",
    "45036": "Mpumalanga",
}

SHOPRITE_STORES = {
    "46197": "Western Cape",
    "880": "Northern Cape",
    "6292": "Eastern Cape",
    "6721": "KwaZulu-Natal",
    "52837": "Free State",
    "814": "Gauteng",
    "44933": "Mpumalanga",
    "953": "North West",
    "52691": "Limpopo",
}

PNP_STORES = {
    "WC44": "Western Cape",
    "EC29": "Eastern Cape",
    "GC61": "Northern Cape",
    "GH45": "Free State",
    "HC08": "North West",
    "GC13": "Gauteng",
    "KC16": "KwaZulu-Natal",
    "NC38": "Limpopo",
    "NC12": "Mpumalanga",
}

WOOLWORTHS_STORES = {
    "Gauteng": ("2 Saltus Street", "ChIJt3cT6lVmlR4RQhVr-hreuuU"),
    "Western Cape": ("210 Paarl Rock Rd", "EiYyMTAgUGFhcmwgUm9jayBSZCwgUGFhcmwsIFNvdXRoIEFmcmljYSIuKiwKFAoSCTkfD-nNqc0dEZQ9_uwoVwscEhQKEgmti0uqgAfNHRG56KZ2EE8RbQ"),
    "Eastern Cape": ("1 Ring Road", "ChIJ5aSUWdLTeh4RK_SVTxHmLMo"),
    "KwaZulu-Natal": ("1 Premium Promenade", "ChIJ4SJXgjkj-h4RaBu9Y63uwIA"),
    # Add more provinces as you find valid place IDs
}

WOOLWORTHS_CATEGORIES = {
    "Fruit-Vegetables-Salads": "lllnam",
    "Meat-Poultry-Fish": "d87rb7",
    "Milk-Dairy-Eggs": "1sqo44p",
    "Ready-Meals": "s2csbp",
    "Deli-Entertaining": "13b8g51",
    "Food-To-Go": "11buko0",
    "Bakery": "1bm2new",
    "Frozen-Food": "j8pkwq",
    "Pantry": "1lw4dzx",
    "Chocolates-Sweets-Snacks": "1yz1i0m",
    "Beverages-Juices": "mnxddc",
    "Household": "vvikef",
    "Cleaning": "o1v4pe",
    "Toiletries-Health": "1q1wl1r",
    "Flowers-Plants": "1z13rv1",
    "Kids": "ymaf0z",
    "Baby": "1rij75n",
    "Pets": "l1demz",
}

# Index ranges for each retailer (to prevent overlapping)
# With 9 provinces, we need much larger ranges
INDEX_RANGES = {
    "pnp": {
        "start": 0,
        "products_per_province": 20000,  # 20k products * 9 provinces = 180k max
    },
    "shoprite": {
        "start": 200000,
        "products_per_province": 20000,
    },
    "checkers": {
        "start": 400000,
        "products_per_province": 20000,
    },
    "woolworths": {
        "start": 600000,
        "products_per_province": 20000,
    },
}


# Placeholder image URLs
PLACEHOLDER_IMAGES = {
    "checkers": "https://sfnavipqilqgzmtedfuh.supabase.co/storage/v1/object/public/product_images/checkers/checkers_image_placeholder.png",
    "shoprite": "https://sfnavipqilqgzmtedfuh.supabase.co/storage/v1/object/public/product_images/shoprite/shoprite_image_placeholder.png",
    "pnp": "https://sfnavipqilqgzmtedfuh.supabase.co/storage/v1/object/public/product_images/pnp/pnp_image_placeholder.png",
    "woolworths": "https://sfnavipqilqgzmtedfuh.supabase.co/storage/v1/object/public/product_images/woolworths/woolworths_image_placeholder.png",
}


# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]


class Config:
    """Main configuration class that combines all settings."""

    _instance: Optional["Config"] = None

    def __new__(cls):
        """Singleton pattern to ensure only one config instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initialize all configuration sections."""
        self.supabase = SupabaseConfig()
        self.discord = DiscordConfig()
        self.scraper = ScraperConfig()
        self.playwright = PlaywrightConfig()

        # Ensure directories exist
        self.scraper.ensure_directories()

    def validate(self) -> bool:
        """Validate all required configuration."""
        self.supabase.validate()
        return True

    def get_store_config(self, retailer: str) -> dict:
        """Get store configuration for a specific retailer."""
        stores = {
            "checkers": CHECKERS_STORES,
            "shoprite": SHOPRITE_STORES,
            "pnp": PNP_STORES,
            "woolworths": WOOLWORTHS_STORES,
        }
        return stores.get(retailer.lower(), {})

    def get_index_range(self, retailer: str, province_index: int = 0) -> int:
        """Calculate starting index for a retailer and province."""
        retailer_config = INDEX_RANGES.get(retailer.lower(), {"start": 0, "products_per_province": 20000})
        return retailer_config["start"] + (province_index * retailer_config["products_per_province"])


# Global config instance
config = Config()
