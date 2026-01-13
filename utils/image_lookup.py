"""
Image Lookup Utility

Maps product names to existing Supabase storage URLs.
This avoids re-downloading images that already exist in storage.
"""

import json
import logging
import re
import unicodedata
from pathlib import Path
from typing import Dict, Optional

from supabase import create_client

from config import config

logger = logging.getLogger(__name__)

# Cache file location
LOOKUP_CACHE_FILE = Path("data/image_lookup_cache.json")


def normalize_product_name(product_name: str) -> str:
    """
    Normalize product name to match Supabase storage filename format.

    This matches the original scraper's normalization logic:
    - Replace spaces with underscores
    - Remove non-ASCII characters
    - Replace special chars with underscores
    """
    # Replace spaces with underscores
    normalized = product_name.replace(" ", "_")
    # Remove non-ASCII characters
    normalized = unicodedata.normalize('NFKD', normalized).encode('ascii', 'ignore').decode('ascii')
    # Replace special characters with underscores
    sanitized = re.sub(r'[^\w\.-]', '_', normalized)
    return sanitized


def build_expected_url(product_name: str, retailer: str) -> str:
    """Build the expected Supabase storage URL for a product."""
    retailer_lower = retailer.lower()
    sanitized_name = normalize_product_name(product_name)
    filename = f"{retailer_lower}_image_{sanitized_name}.jpg"

    base_url = config.supabase.url.rstrip('/')
    return f"{base_url}/storage/v1/object/public/product_images/{retailer_lower}/{filename}"


class ImageLookup:
    """
    Manages lookup of existing product images in Supabase storage.

    Caches the list of existing images locally to avoid repeated API calls.
    """

    def __init__(self):
        self._cache: Dict[str, Dict[str, str]] = {}  # {retailer: {normalized_name: url}}
        self._loaded = False

    def load_from_supabase(self, retailers: list = None) -> int:
        """
        Load existing image URLs from Supabase storage.

        Args:
            retailers: List of retailers to load (default: all)

        Returns:
            Total number of images found
        """
        if retailers is None:
            retailers = ["checkers", "shoprite", "pnp", "woolworths"]

        try:
            # Ensure URL has trailing slash to avoid SDK warning
            supabase_url = config.supabase.url
            if not supabase_url.endswith('/'):
                supabase_url = supabase_url + '/'
            supabase = create_client(supabase_url, config.supabase.key)
            total_images = 0

            for retailer in retailers:
                logger.info(f"Loading existing images for {retailer}...")
                self._cache[retailer] = {}

                try:
                    # Paginate through all files in the retailer's folder
                    offset = 0
                    limit = 1000  # Max per request

                    while True:
                        response = supabase.storage.from_("product_images").list(
                            retailer,
                            {"limit": limit, "offset": offset}
                        )

                        if not response:
                            break

                        for file_info in response:
                            filename = file_info.get("name", "")
                            if filename.endswith(('.jpg', '.png', '.webp')):
                                # Extract normalized name from filename
                                # Format: {retailer}_image_{normalized_name}.jpg
                                prefix = f"{retailer}_image_"
                                if filename.startswith(prefix):
                                    normalized_name = filename[len(prefix):].rsplit('.', 1)[0]
                                    url = f"{config.supabase.url.rstrip('/')}/storage/v1/object/public/product_images/{retailer}/{filename}"
                                    self._cache[retailer][normalized_name.lower()] = url

                        # Check if we got fewer results than limit (last page)
                        if len(response) < limit:
                            break

                        offset += limit
                        logger.debug(f"  Loaded {offset} images so far for {retailer}...")

                    count = len(self._cache[retailer])
                    total_images += count
                    logger.info(f"  Found {count} images for {retailer}")

                except Exception as e:
                    logger.warning(f"  Error loading {retailer} images: {e}")

            self._loaded = True
            self._save_cache()

            logger.info(f"Total images loaded: {total_images}")
            return total_images

        except Exception as e:
            logger.error(f"Failed to load images from Supabase: {e}")
            return 0

    def load_from_cache(self) -> bool:
        """
        Load image lookup from local cache file.

        Returns:
            True if cache was loaded successfully
        """
        if not LOOKUP_CACHE_FILE.exists():
            return False

        try:
            with open(LOOKUP_CACHE_FILE, 'r') as f:
                self._cache = json.load(f)
            self._loaded = True

            total = sum(len(urls) for urls in self._cache.values())
            logger.info(f"Loaded {total} images from cache")
            return True

        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")
            return False

    def _save_cache(self):
        """Save current lookup to cache file."""
        try:
            LOOKUP_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            # Copy cache to avoid "dictionary changed size during iteration" error
            cache_copy = {k: dict(v) for k, v in self._cache.items()}
            with open(LOOKUP_CACHE_FILE, 'w') as f:
                json.dump(cache_copy, f)
            logger.debug(f"Saved image lookup cache to {LOOKUP_CACHE_FILE}")
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")

    def get_url(self, product_name: str, retailer: str) -> Optional[str]:
        """
        Get existing Supabase URL for a product image.

        Args:
            product_name: Product name
            retailer: Retailer name

        Returns:
            Supabase URL if image exists, None otherwise
        """
        if not self._loaded:
            # Try loading from cache first
            if not self.load_from_cache():
                logger.warning("Image lookup not loaded. Call load_from_supabase() or load_from_cache() first.")
                return None

        retailer_lower = retailer.lower()
        if retailer_lower not in self._cache:
            return None

        # Normalize the product name to match storage filename
        normalized = normalize_product_name(product_name).lower()

        return self._cache[retailer_lower].get(normalized)

    def add_url(self, product_name: str, retailer: str, url: str):
        """
        Add a new URL to the lookup (after uploading a new image).

        Args:
            product_name: Product name
            retailer: Retailer name
            url: Supabase storage URL
        """
        retailer_lower = retailer.lower()
        if retailer_lower not in self._cache:
            self._cache[retailer_lower] = {}

        normalized = normalize_product_name(product_name).lower()
        self._cache[retailer_lower][normalized] = url

        # Save cache periodically (every 100 new images)
        total = sum(len(urls) for urls in self._cache.values())
        if total % 100 == 0:
            self._save_cache()

    def exists(self, product_name: str, retailer: str) -> bool:
        """Check if an image exists for this product."""
        return self.get_url(product_name, retailer) is not None


# Global instance
image_lookup = ImageLookup()


def init_image_lookup(force_refresh: bool = False) -> int:
    """
    Initialize the image lookup.

    Args:
        force_refresh: If True, reload from Supabase even if cache exists

    Returns:
        Number of images in lookup
    """
    if not force_refresh and image_lookup.load_from_cache():
        return sum(len(urls) for urls in image_lookup._cache.values())

    return image_lookup.load_from_supabase()