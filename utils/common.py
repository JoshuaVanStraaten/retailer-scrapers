"""
Common Utilities for Savvy Grocery Scrapers

Shared functions for HTTP requests, data processing, logging,
image handling, and Supabase operations.
"""

import hashlib
import logging
import mimetypes
import os
import random
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import config, USER_AGENTS, PLACEHOLDER_IMAGES

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Cached Supabase client
_supabase_client = None


def get_supabase_client(force_new: bool = False):
    """Get a Supabase client with properly formatted URL."""
    global _supabase_client

    if _supabase_client is None or force_new:
        from supabase import create_client

        # Ensure URL has trailing slash to avoid SDK warning
        supabase_url = config.supabase.url
        if not supabase_url.endswith('/'):
            supabase_url = supabase_url + '/'

        _supabase_client = create_client(supabase_url, config.supabase.key)

    return _supabase_client


def reset_supabase_client():
    """Reset the cached Supabase client (use after connection errors)."""
    global _supabase_client
    _supabase_client = None


# =============================================================================
# HTTP Request Utilities
# =============================================================================

def get_random_user_agent() -> str:
    """Get a random user agent string."""
    return random.choice(USER_AGENTS)


def create_session_with_retries(
    retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: Tuple[int, ...] = (500, 502, 503, 504),
) -> requests.Session:
    """
    Create a requests Session with automatic retry logic.

    Args:
        retries: Number of retries for failed requests
        backoff_factor: Factor for exponential backoff between retries
        status_forcelist: HTTP status codes that trigger a retry

    Returns:
        Configured requests.Session
    """
    session = requests.Session()

    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "POST"],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Set default headers
    session.headers.update({
        "User-Agent": get_random_user_agent(),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    })

    return session


def make_request_with_retry(
    url: str,
    method: str = "GET",
    session: Optional[requests.Session] = None,
    max_retries: int = 3,
    retry_delay: float = 2.0,
    **kwargs,
) -> Optional[requests.Response]:
    """
    Make an HTTP request with manual retry logic and exponential backoff.

    Args:
        url: URL to request
        method: HTTP method (GET, POST, etc.)
        session: Optional requests.Session to use
        max_retries: Maximum number of retry attempts
        retry_delay: Base delay between retries (multiplied exponentially)
        **kwargs: Additional arguments passed to requests

    Returns:
        Response object if successful, None otherwise
    """
    session = session or requests.Session()

    for attempt in range(max_retries + 1):
        try:
            response = session.request(method, url, timeout=config.scraper.request_timeout, **kwargs)

            if response.ok:
                return response

            logger.warning(f"Request to {url} returned status {response.status_code}")

            # Don't retry client errors (4xx) except 429 (rate limit)
            if 400 <= response.status_code < 500 and response.status_code != 429:
                return response

        except requests.RequestException as e:
            logger.warning(f"Request to {url} failed: {e}")

        if attempt < max_retries:
            sleep_time = retry_delay * (2 ** attempt) + random.uniform(0, 1)
            logger.info(f"Retrying in {sleep_time:.1f}s (attempt {attempt + 1}/{max_retries})")
            time.sleep(sleep_time)

    logger.error(f"All {max_retries + 1} attempts failed for {url}")
    return None


# =============================================================================
# Data Processing Utilities
# =============================================================================

def normalize_product_name(name: str) -> str:
    """
    Normalize a product name for search/comparison.

    Converts to lowercase, removes special characters, normalizes whitespace.

    Args:
        name: Original product name

    Returns:
        Normalized product name
    """
    if not name:
        return ""

    # Normalize unicode characters
    normalized = unicodedata.normalize("NFKD", name)

    # Convert to lowercase
    normalized = normalized.lower()

    # Remove special characters but keep alphanumeric and spaces
    normalized = re.sub(r"[^\w\s]", " ", normalized)

    # Normalize whitespace
    normalized = " ".join(normalized.split())

    return normalized


def extract_size_info(name: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Extract size/weight information from a product name.

    Args:
        name: Product name containing size info (e.g., "Milk 2L", "Rice 500g")

    Returns:
        Tuple of (size_value, size_unit) or (None, None) if not found
    """
    if not name:
        return None, None

    # Common patterns for sizes
    patterns = [
        # Match "500g", "2kg", "1.5L", etc.
        r"(\d+(?:\.\d+)?)\s*(g|kg|ml|l|L|lt|mg|oz|lb)(?:\s|$|,)",
        # Match "500 g", "2 kg", etc. (with space)
        r"(\d+(?:\.\d+)?)\s+(g|kg|ml|l|L|lt|mg|oz|lb)(?:\s|$|,)",
        # Match "x6", "6 pack", "6-pack"
        r"[x×]?\s*(\d+)\s*(?:pack|pk|pcs?|pieces?)(?:\s|$|,)",
    ]

    for pattern in patterns:
        match = re.search(pattern, name, re.IGNORECASE)
        if match:
            try:
                value = float(match.group(1))
                unit = match.group(2).lower() if len(match.groups()) > 1 else "pack"

                # Normalize units
                unit_map = {
                    "l": "ml",  # Convert L to ml
                    "lt": "ml",
                    "kg": "g",  # Keep kg as-is, or convert to g
                }

                # Convert L to ml, kg to g for consistency
                if unit == "l":
                    value *= 1000
                    unit = "ml"
                elif unit == "kg":
                    value *= 1000
                    unit = "g"

                return value, unit
            except (ValueError, IndexError):
                continue

    return None, None


def parse_price(price_str: str) -> Optional[float]:
    """
    Parse a price string into a float value.

    Args:
        price_str: Price string like "R45.99", "45,99", etc.

    Returns:
        Float price value or None if parsing fails
    """
    if not price_str:
        return None

    try:
        # Remove currency symbols and whitespace
        cleaned = re.sub(r"[R$€£\s]", "", price_str)

        # Handle comma as decimal separator
        if "," in cleaned and "." not in cleaned:
            cleaned = cleaned.replace(",", ".")
        elif "," in cleaned and "." in cleaned:
            # Remove thousands separator (comma)
            cleaned = cleaned.replace(",", "")

        # Extract numeric part
        match = re.search(r"[\d.]+", cleaned)
        if match:
            return float(match.group())
    except (ValueError, AttributeError):
        pass

    return None


def sanitize_filename(name: str, max_length: int = 100) -> str:
    """
    Sanitize a string for use as a filename.

    Args:
        name: Original name
        max_length: Maximum length for the filename

    Returns:
        Sanitized filename-safe string
    """
    # Normalize unicode
    normalized = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")

    # Replace spaces with underscores
    sanitized = normalized.replace(" ", "_")

    # Remove any character that isn't alphanumeric, underscore, hyphen, or dot
    sanitized = re.sub(r"[^\w\-.]", "_", sanitized)

    # Remove multiple consecutive underscores
    sanitized = re.sub(r"_+", "_", sanitized)

    # Truncate if too long
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length]

    return sanitized


def generate_product_id(name: str, retailer: str, province: str) -> str:
    """
    Generate a unique product ID based on name, retailer, and province.

    Args:
        name: Product name
        retailer: Retailer name
        province: Province name

    Returns:
        Unique hash-based ID
    """
    # Create a consistent string for hashing
    key = f"{normalize_product_name(name)}|{retailer.lower()}|{province.lower()}"

    # Generate MD5 hash (fast, sufficient for uniqueness)
    hash_value = hashlib.md5(key.encode()).hexdigest()[:12]

    return f"{retailer[:3].lower()}_{hash_value}"


# =============================================================================
# Image Handling Utilities
# =============================================================================

def download_image(
    url: str,
    save_path: Path,
    timeout: int = 30,
) -> bool:
    """
    Download an image from a URL.

    Args:
        url: Image URL
        save_path: Local path to save the image
        timeout: Request timeout in seconds

    Returns:
        True if successful, False otherwise
    """
    try:
        response = requests.get(
            url,
            headers={"User-Agent": get_random_user_agent()},
            timeout=timeout,
        )
        response.raise_for_status()

        # Ensure parent directory exists
        save_path.parent.mkdir(parents=True, exist_ok=True)

        with open(save_path, "wb") as f:
            f.write(response.content)

        return True

    except Exception as e:
        logger.error(f"Failed to download image from {url}: {e}")
        return False


def upload_image_to_supabase(
    local_path: Path,
    remote_path: str,
    bucket_name: Optional[str] = None,
    max_retries: int = 3,
) -> Optional[str]:
    """
    Upload an image to Supabase Storage with retry logic.

    Args:
        local_path: Path to local image file
        remote_path: Path in Supabase storage
        bucket_name: Storage bucket name (uses config default if not provided)
        max_retries: Maximum number of retry attempts

    Returns:
        Public URL if successful, None otherwise
    """
    bucket_name = bucket_name or config.supabase.bucket_name

    for attempt in range(max_retries):
        try:
            supabase = get_supabase_client()

            with open(local_path, "rb") as f:
                # Check if file already exists (only on first attempt)
                if attempt == 0:
                    try:
                        files = supabase.storage.from_(bucket_name).list(
                            os.path.dirname(remote_path),
                            {"limit": 10000}
                        )
                        file_exists = any(
                            file["name"] == os.path.basename(remote_path)
                            for file in files
                        )

                        if file_exists:
                            logger.debug(f"File already exists: {remote_path}")
                            return supabase.storage.from_(bucket_name).get_public_url(remote_path)
                    except Exception:
                        pass  # If check fails, try uploading anyway

                # Upload file
                supabase.storage.from_(bucket_name).upload(remote_path, f)

            return supabase.storage.from_(bucket_name).get_public_url(remote_path)

        except Exception as e:
            error_str = str(e)

            # File already exists - not an error
            if "already exists" in error_str.lower():
                supabase = get_supabase_client()
                return supabase.storage.from_(bucket_name).get_public_url(remote_path)

            # Retry on connection errors
            if attempt < max_retries - 1 and ("disconnect" in error_str.lower() or "connection" in error_str.lower()):
                wait_time = (attempt + 1) * 2  # 2s, 4s, 6s
                logger.warning(f"Upload failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {e}")
                reset_supabase_client()  # Reset the cached client
                time.sleep(wait_time)
                continue

            logger.error(f"Failed to upload {local_path} to Supabase: {e}")
            return None

    return None


def process_product_image(
    image_url: str,
    product_name: str,
    retailer: str,
    existing_url: Optional[str] = None,
) -> str:
    """
    Process a product image: download, upload to Supabase, return URL.

    Checks multiple sources before downloading:
    1. Existing URL from CSV (if it's a Supabase URL)
    2. Image lookup cache (existing images in Supabase storage)
    3. If neither found, downloads and uploads new image

    Args:
        image_url: Original image URL from retailer
        product_name: Product name (used for filename)
        retailer: Retailer name (used for folder organization)
        existing_url: Existing Supabase URL if known

    Returns:
        Final image URL (Supabase or CDN)
    """
    from utils.image_lookup import image_lookup

    retailer_lower = retailer.lower().replace(" ", "_")
    placeholder = PLACEHOLDER_IMAGES.get(retailer_lower, PLACEHOLDER_IMAGES.get("checkers"))

    # 1. If we have an existing Supabase URL, use it
    if existing_url and existing_url != placeholder and "supabase" in existing_url:
        return existing_url

    # 2. Check image lookup cache for existing image in storage
    cached_url = image_lookup.get_url(product_name, retailer)
    if cached_url:
        logger.info(f"Cache hit: {product_name[:40]}...")
        return cached_url

    # 3. If configured to use CDN URLs directly, return the original
    if config.scraper.use_cdn_urls and image_url:
        return image_url

    if not image_url:
        return placeholder

    try:
        # Generate filename
        safe_name = sanitize_filename(product_name)
        filename = f"{retailer_lower}_image_{safe_name}.jpg"

        # Local and remote paths
        local_path = config.scraper.image_dir / retailer_lower / filename
        remote_path = f"{retailer_lower}/{filename}"

        # Download image (new - not in cache)
        logger.info(f"Downloading new image: {product_name[:40]}...")
        if not download_image(image_url, local_path):
            return placeholder

        # Upload to Supabase
        uploaded_url = upload_image_to_supabase(local_path, remote_path)

        if uploaded_url:
            # Add to lookup cache for future use
            image_lookup.add_url(product_name, retailer, uploaded_url)

            # Optionally delete local file
            if config.scraper.delete_local_images:
                try:
                    local_path.unlink()
                except Exception:
                    pass

            return uploaded_url

        return placeholder

    except Exception as e:
        logger.warning(f"Failed to process image for {product_name}: {e}")
        return placeholder


# =============================================================================
# Supabase Database Utilities
# =============================================================================

def upsert_to_supabase(
    data: List[Dict[str, Any]],
    table_name: str = "Products",
    batch_size: Optional[int] = None,
    on_conflict: str = "name,retailer,province",
) -> bool:
    """
    Upsert data to Supabase in batches.

    Args:
        data: List of dictionaries to upsert
        table_name: Target table name
        batch_size: Records per batch (uses config default if not provided)
        on_conflict: Comma-separated column names for conflict resolution

    Returns:
        True if successful, False otherwise
    """
    from utils.notifications import notifier

    if not data:
        logger.warning("No data to upsert")
        return True

    batch_size = batch_size or config.scraper.supabase_batch_size

    try:
        supabase = get_supabase_client()
        total_rows = len(data)
        failed_batches = 0
        successful_rows = 0

        logger.info(f"Upserting {total_rows} rows to {table_name}")

        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch = data[start:end]

            try:
                logger.debug(f"Upserting batch {start + 1} to {end}")
                # Use on_conflict to specify which columns determine uniqueness
                response = supabase.table(table_name).upsert(
                    batch,
                    on_conflict=on_conflict
                ).execute()

                if response.data:
                    successful_rows += len(response.data)
                else:
                    logger.warning(f"Batch {start}-{end} returned no data")

            except Exception as batch_error:
                failed_batches += 1
                logger.error(f"Batch {start}-{end} failed: {batch_error}")

                # Send Discord notification for batch failure
                notifier.send_embed(
                    title="⚠️ Supabase Batch Error",
                    description=str(batch_error)[:500],
                    color=0xFFA500,  # Orange
                    fields=[
                        {"name": "Batch", "value": f"{start}-{end}", "inline": True},
                        {"name": "Batch Size", "value": str(len(batch)), "inline": True},
                    ]
                )

        if failed_batches > 0:
            logger.warning(f"Completed with {failed_batches} failed batches")
            notifier.send_embed(
                title="⚠️ Supabase Upload Incomplete",
                description=f"{failed_batches} batches failed",
                color=0xFFA500,
                fields=[
                    {"name": "Total Rows", "value": str(total_rows), "inline": True},
                    {"name": "Successful", "value": str(successful_rows), "inline": True},
                    {"name": "Failed Batches", "value": str(failed_batches), "inline": True},
                ]
            )
            return False

        logger.info(f"Successfully upserted {successful_rows} rows")
        return True

    except Exception as e:
        logger.error(f"Failed to upsert to Supabase: {e}")

        # Send Discord notification for complete failure
        notifier.send_embed(
            title="❌ Supabase Upload Failed",
            description=str(e)[:500],
            color=0xFF0000,  # Red
            fields=[
                {"name": "Table", "value": table_name, "inline": True},
                {"name": "Total Rows", "value": str(len(data)), "inline": True},
            ]
        )
        return False


# =============================================================================
# CSV Utilities
# =============================================================================

def load_csv_as_dict(
    filepath: Path,
    key_column: str = "name",
) -> Dict[str, Dict[str, Any]]:
    """
    Load a CSV file as a dictionary keyed by a specific column.

    Args:
        filepath: Path to CSV file
        key_column: Column to use as dictionary key

    Returns:
        Dictionary mapping key_column values to row dictionaries
    """
    try:
        df = pd.read_csv(filepath, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(filepath, encoding="latin1")
    except FileNotFoundError:
        return {}

    # Fill NaN values - convert all columns to object type first to avoid FutureWarning
    for col in df.columns:
        if df[col].dtype == 'float64':
            df[col] = df[col].fillna("").astype(str)
        else:
            df[col] = df[col].fillna("")

    return {row[key_column]: row.to_dict() for _, row in df.iterrows()}


def save_to_csv(
    data: List[Dict[str, Any]],
    filepath: Path,
    append: bool = False,
) -> bool:
    """
    Save data to a CSV file.

    Args:
        data: List of dictionaries to save
        filepath: Output file path
        append: If True, append to existing file

    Returns:
        True if successful
    """
    if not data:
        return True

    try:
        df = pd.DataFrame(data)

        # Ensure parent directory exists
        filepath.parent.mkdir(parents=True, exist_ok=True)

        mode = "a" if append and filepath.exists() else "w"
        header = not (append and filepath.exists())

        df.to_csv(filepath, mode=mode, header=header, index=False, encoding="utf-8")
        return True

    except Exception as e:
        logger.error(f"Failed to save CSV to {filepath}: {e}")
        return False


def deduplicate_csv(
    filepath: Path,
    subset: List[str] = ["name", "price", "province"],
    priority_column: Optional[str] = "promotion_price",
    priority_value: str = "No promo",
) -> int:
    """
    Remove duplicate rows from a CSV file.

    Args:
        filepath: Path to CSV file
        subset: Columns to consider for duplicates
        priority_column: Column to use for priority (keep rows where value != priority_value)
        priority_value: Value indicating lower priority

    Returns:
        Number of duplicates removed
    """
    try:
        df = pd.read_csv(filepath, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(filepath, encoding="latin1")

    original_count = len(df)

    if priority_column and priority_column in df.columns:
        # Sort so rows with promotions come first (priority)
        df["_priority"] = df[priority_column].apply(lambda x: 0 if x != priority_value else 1)
        df = df.sort_values(by=subset + ["_priority"])
        df = df.drop_duplicates(subset=subset, keep="first")
        df = df.drop(columns=["_priority"])
    else:
        df = df.drop_duplicates(subset=subset, keep="first")

    df.to_csv(filepath, index=False, encoding="utf-8")

    removed = original_count - len(df)
    logger.info(f"Removed {removed} duplicates from {filepath}")

    return removed


# =============================================================================
# Logging Utilities
# =============================================================================

def setup_logger(
    name: str,
    log_dir: Optional[Path] = None,
    level: int = logging.INFO,
) -> logging.Logger:
    """
    Set up a logger with file and console handlers.

    Args:
        name: Logger name (typically retailer name)
        log_dir: Directory for log files (uses config default if not provided)
        level: Logging level

    Returns:
        Configured logger
    """
    log_dir = log_dir or config.scraper.log_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove existing handlers
    logger.handlers.clear()

    # File handler
    log_file = log_dir / f"{name}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_formatter = logging.Formatter("%(levelname)s: %(message)s")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger


# =============================================================================
# Timing Utilities
# =============================================================================

class Timer:
    """Simple context manager for timing operations."""

    def __init__(self, name: str = "Operation"):
        self.name = name
        self.start_time = None
        self.end_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()
        logger.info(f"{self.name} completed in {self.elapsed:.2f}s")

    @property
    def elapsed(self) -> float:
        if self.end_time:
            return self.end_time - self.start_time
        elif self.start_time:
            return time.time() - self.start_time
        return 0.0


def rate_limited(min_interval: float):
    """
    Decorator to rate-limit function calls.

    Args:
        min_interval: Minimum seconds between calls
    """
    last_called = [0.0]

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args, **kwargs) -> T:
            elapsed = time.time() - last_called[0]
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)

            result = func(*args, **kwargs)
            last_called[0] = time.time()
            return result

        return wrapper
    return decorator