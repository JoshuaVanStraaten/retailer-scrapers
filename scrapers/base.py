"""
Base Scraper Class for Savvy Grocery Scrapers

Provides common functionality for all retailer scrapers including:
- Session management
- Concurrent page scraping
- Progress tracking
- Error handling with retries
- Data storage
- Notifications
"""

import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config import config
from utils import (
    Timer,
    create_session_with_retries,
    deduplicate_csv,
    load_csv_as_dict,
    notify_completed,
    notify_error,
    notify_started,
    save_to_csv,
    setup_logger,
    upsert_to_supabase,
    init_image_lookup,
)


@dataclass
class ScrapeResult:
    """Result of scraping a single page."""
    page: int
    store_code: str
    province: str
    products: List[Dict[str, Any]] = field(default_factory=list)
    success: bool = True
    error: Optional[str] = None
    duration_seconds: float = 0.0


@dataclass
class ScraperStats:
    """Statistics for a scraping session."""
    retailer: str
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    total_products: int = 0
    total_pages: int = 0
    failed_pages: int = 0
    provinces_completed: int = 0
    errors: List[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        end = self.end_time or datetime.now()
        return (end - self.start_time).total_seconds()

    @property
    def success_rate(self) -> float:
        total = self.total_pages + self.failed_pages
        return self.total_pages / total if total > 0 else 0.0


class BaseScraper(ABC):
    """
    Abstract base class for all retailer scrapers.

    Subclasses must implement:
    - scrape_page(): Scrape a single page of products
    - get_total_pages(): Determine total pages to scrape (optional)

    The base class handles:
    - Concurrent scraping across pages and provinces
    - Session management
    - Progress tracking
    - Error handling and retries
    - Data persistence
    - Notifications
    """

    # Override in subclasses
    RETAILER_NAME: str = "Unknown"
    OUTPUT_FILENAME: str = "products.csv"

    def __init__(
        self,
        stores: Dict[str, str],
        max_threads: Optional[int] = None,
        page_delay: Optional[float] = None,
        start_index: int = 0,
    ):
        """
        Initialize the scraper.

        Args:
            stores: Dictionary mapping store_code to province name
            max_threads: Maximum concurrent threads (uses config default)
            page_delay: Delay between page requests (uses config default)
            start_index: Starting product index
        """
        self.stores = stores
        self.max_threads = max_threads or config.scraper.max_threads
        self.page_delay = page_delay or config.scraper.page_delay
        self.current_index = start_index

        # Set up logging
        self.logger = setup_logger(self.RETAILER_NAME.lower())

        # Output file path
        self.output_path = config.scraper.data_dir / self.OUTPUT_FILENAME

        # Statistics
        self.stats = ScraperStats(retailer=self.RETAILER_NAME)

        # Existing data cache (for image URL reuse)
        self.existing_data: Dict[str, Dict[str, Any]] = {}

        # Initialize image lookup cache (loads from local cache or Supabase)
        if not config.scraper.use_cdn_urls:
            image_count = init_image_lookup()
            if image_count > 0:
                self.logger.info(f"Image lookup cache: {image_count} existing images")

    def load_existing_data(self, filepath: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
        """
        Load existing product data for comparison/updates.

        Args:
            filepath: Path to existing data CSV

        Returns:
            Dictionary mapping product names to their data
        """
        filepath = filepath or self.output_path
        self.existing_data = load_csv_as_dict(filepath)
        self.logger.info(f"Loaded {len(self.existing_data)} existing products from {filepath}")
        return self.existing_data

    @abstractmethod
    def scrape_page(
        self,
        page: int,
        store_code: str,
        province: str,
        session: Any,
    ) -> ScrapeResult:
        """
        Scrape a single page of products.

        Must be implemented by subclasses.

        Args:
            page: Page number to scrape
            store_code: Store code for the request
            province: Province name
            session: HTTP session to use

        Returns:
            ScrapeResult with products and status
        """
        pass

    def get_total_pages(self, store_code: str, session: Any) -> int:
        """
        Determine total pages to scrape for a store.

        Override in subclasses if the site provides this info.

        Args:
            store_code: Store code
            session: HTTP session

        Returns:
            Total number of pages
        """
        return 999  # Default: scrape until no products returned

    def should_stop_scraping(self, result: ScrapeResult, page: int) -> bool:
        """
        Determine if scraping should stop based on result.

        Args:
            result: Result from scraping a page
            page: Page number

        Returns:
            True if scraping should stop
        """
        # Stop if no products returned
        if not result.products:
            self.logger.info(f"No products on page {page}, stopping")
            return True

        return False

    def create_session(self, store_code: str) -> Any:
        """
        Create an HTTP session for requests.

        Override in subclasses for custom session setup (e.g., Playwright).

        Args:
            store_code: Store code (for store-specific cookies)

        Returns:
            Session object (requests.Session or custom)
        """
        return create_session_with_retries()

    def scrape_store(
        self,
        store_code: str,
        province: str,
        start_page: int = 0,
        end_page: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Scrape all pages for a single store.

        Args:
            store_code: Store code
            province: Province name
            start_page: First page to scrape
            end_page: Last page to scrape (None = auto-detect)

        Returns:
            List of all scraped products
        """
        self.logger.info(f"Starting scrape for {province} (store {store_code})")

        session = self.create_session(store_code)

        # Determine total pages if not provided
        if end_page is None:
            end_page = self.get_total_pages(store_code, session)

        all_products = []
        page = start_page
        consecutive_failures = 0
        max_consecutive_failures = 5

        while page <= end_page:
            try:
                with Timer(f"Page {page}"):
                    result = self.scrape_page(page, store_code, province, session)

                if result.success:
                    consecutive_failures = 0
                    all_products.extend(result.products)
                    self.stats.total_pages += 1
                    self.stats.total_products += len(result.products)

                    self.logger.info(
                        f"Page {page}: {len(result.products)} products "
                        f"(total: {len(all_products)})"
                    )

                    # Save incrementally
                    if result.products:
                        save_to_csv(result.products, self.output_path, append=True)

                    # Check if we should stop
                    if self.should_stop_scraping(result, page):
                        break
                else:
                    consecutive_failures += 1
                    self.stats.failed_pages += 1
                    self.stats.errors.append(f"Page {page}: {result.error}")

                    self.logger.warning(f"Page {page} failed: {result.error}")
                    notify_error(self.RETAILER_NAME, result.error, page, province)

                    if consecutive_failures >= max_consecutive_failures:
                        self.logger.error(f"Too many consecutive failures, stopping")
                        break

                # Delay between pages
                time.sleep(self.page_delay)
                page += 1

            except Exception as e:
                self.logger.error(f"Unexpected error on page {page}: {e}")
                consecutive_failures += 1
                page += 1

                if consecutive_failures >= max_consecutive_failures:
                    break

        self.stats.provinces_completed += 1
        self.logger.info(f"Completed {province}: {len(all_products)} products")

        return all_products

    def scrape_store_concurrent(
        self,
        store_code: str,
        province: str,
        start_page: int = 0,
        end_page: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Scrape a store using concurrent threads for pages.

        Args:
            store_code: Store code
            province: Province name
            start_page: First page
            end_page: Last page

        Returns:
            List of all products
        """
        self.logger.info(
            f"Starting concurrent scrape for {province} "
            f"(pages {start_page}-{end_page}, {self.max_threads} threads)"
        )

        all_products = []

        # Pre-create session before spawning threads (avoid race condition)
        session = self.create_session(store_code)

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = {
                executor.submit(
                    self.scrape_page, page, store_code, province, session
                ): page
                for page in range(start_page, end_page + 1)
            }

            for future in as_completed(futures):
                page = futures[future]
                try:
                    result = future.result()

                    if result.success:
                        all_products.extend(result.products)
                        self.stats.total_pages += 1
                        self.stats.total_products += len(result.products)

                        # Save incrementally
                        if result.products:
                            save_to_csv(result.products, self.output_path, append=True)
                    else:
                        self.stats.failed_pages += 1
                        self.stats.errors.append(f"Page {page}: {result.error}")

                except Exception as e:
                    self.logger.error(f"Error processing page {page}: {e}")
                    self.stats.failed_pages += 1

        self.stats.provinces_completed += 1
        return all_products

    def run(
        self,
        concurrent: bool = True,
        start_page: int = 0,
        end_page: int = 100,
        provinces: Optional[List[str]] = None,
    ) -> ScraperStats:
        """
        Run the full scraping process for all stores.

        Args:
            concurrent: Use concurrent scraping for pages
            start_page: First page to scrape
            end_page: Last page to scrape
            provinces: List of provinces to scrape (None = all)

        Returns:
            ScraperStats with results
        """
        self.logger.info(f"Starting {self.RETAILER_NAME} scraper")
        self.stats = ScraperStats(retailer=self.RETAILER_NAME)

        # Filter stores by province if specified
        stores_to_scrape = {
            code: prov for code, prov in self.stores.items()
            if provinces is None or prov in provinces
        }

        # Send start notification
        notify_started(
            self.RETAILER_NAME,
            list(stores_to_scrape.values()),
            (end_page - start_page + 1) * len(stores_to_scrape)
        )

        # Load existing data for image URL reuse
        self.load_existing_data()

        all_products = []
        province_index = 0

        for store_code, province in stores_to_scrape.items():
            # Calculate starting index for this province
            self.current_index = config.get_index_range(
                self.RETAILER_NAME.lower().replace(" ", "_"),
                province_index
            )

            try:
                if concurrent:
                    products = self.scrape_store_concurrent(
                        store_code, province, start_page, end_page
                    )
                else:
                    products = self.scrape_store(
                        store_code, province, start_page, end_page
                    )

                all_products.extend(products)

            except Exception as e:
                self.logger.error(f"Failed to scrape {province}: {e}")
                self.stats.errors.append(f"{province}: {str(e)}")
                notify_error(self.RETAILER_NAME, str(e), province=province)

            province_index += 1

        # Finalize
        self.stats.end_time = datetime.now()

        # Deduplicate CSV
        if self.output_path.exists():
            deduplicate_csv(self.output_path)

        # Send completion notification
        notify_completed(
            self.RETAILER_NAME,
            self.stats.total_products,
            self.stats.duration_seconds,
            self.stats.provinces_completed,
        )

        self.logger.info(
            f"Scraping complete: {self.stats.total_products} products in "
            f"{self.stats.duration_seconds:.1f}s"
        )

        return self.stats

    def upload_to_supabase(self) -> bool:
        """
        Upload scraped data to Supabase.

        Returns:
            True if successful
        """
        if not self.output_path.exists():
            self.logger.warning("No output file to upload")
            return False

        data = load_csv_as_dict(self.output_path)

        # Filter to only this retailer's products and remove 'index' field
        # Index is the PK but changes daily, so we let the DB handle conflicts
        # via the (name, retailer, province) unique constraint
        products = []
        for details in data.values():
            if details.get("retailer") == self.RETAILER_NAME:
                # Remove index - it's the PK and causes conflicts
                product = {k: v for k, v in details.items() if k != "index"}
                products.append(product)

        self.logger.info(f"Uploading {len(products)} products to Supabase")

        return upsert_to_supabase(products)