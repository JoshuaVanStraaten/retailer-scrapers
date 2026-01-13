"""
Checkers Scraper for Savvy Grocery

Scrapes products from products.checkers.co.za using:
- Playwright for session/cookie management
- requests for fast concurrent page scraping
- BeautifulSoup for HTML parsing
"""

import html
import json
import math
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from config import CHECKERS_STORES, PLACEHOLDER_IMAGES, config
from scrapers.base import BaseScraper, ScrapeResult
from utils import (
    PlaywrightSessionManager,
    SessionData,
    get_random_user_agent,
    normalize_product_name,
    process_product_image,
    setup_logger,
)


class CheckersScraper(BaseScraper):
    """
    Scraper for Checkers (products.checkers.co.za).

    Uses Playwright to establish sessions with proper cookies,
    then switches to fast concurrent requests for actual scraping.
    """

    RETAILER_NAME = "Checkers"
    OUTPUT_FILENAME = "products_checkers.csv"

    BASE_URL = "https://products.checkers.co.za"
    PRODUCT_URL = "/c-2413/All-Departments/Food"
    HEAVY_ATTRIBUTES_URL = "/populateProductsWithHeavyAttributes"

    def __init__(
        self,
        stores: Optional[Dict[str, str]] = None,
        max_threads: Optional[int] = None,
        page_delay: float = 2.0,
        start_index: int = 0,
        use_playwright: bool = True,
    ):
        """
        Initialize Checkers scraper.

        Args:
            stores: Store codes mapping (uses default if None)
            max_threads: Max concurrent threads
            page_delay: Delay between page requests
            start_index: Starting product index
            use_playwright: Whether to use Playwright for session management
        """
        super().__init__(
            stores=stores or CHECKERS_STORES,
            max_threads=max_threads,
            page_delay=page_delay,
            start_index=start_index,
        )

        self.use_playwright = use_playwright
        self.session_manager = PlaywrightSessionManager(
            headless=config.playwright.headless,
            timeout=config.playwright.timeout,
        ) if use_playwright else None

        # Cache sessions per store
        self._sessions: Dict[str, SessionData] = {}
        self._http_sessions: Dict[str, requests.Session] = {}

    def create_session(self, store_code: str) -> requests.Session:
        """
        Create a requests session with proper Checkers cookies.

        Uses Playwright to establish the session if enabled,
        otherwise falls back to manual cookie handling.

        IMPORTANT: Playwright is required for the Heavy Attributes API
        because it needs the CSRF token.
        """
        # Return cached session if available
        if store_code in self._http_sessions:
            return self._http_sessions[store_code]

        session = requests.Session()
        session.headers.update({
            "User-Agent": get_random_user_agent(),
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "x-requested-with": "XMLHttpRequest",
        })

        if self.use_playwright and self.session_manager:
            try:
                # Get or create Playwright session - this is required for CSRF token!
                if store_code not in self._sessions:
                    self.logger.info(f"Creating Playwright session for store {store_code}")
                    self._sessions[store_code] = self.session_manager.create_session(
                        "checkers", store_code
                    )

                playwright_session = self._sessions[store_code]

                # Verify we got the CSRF token
                if not playwright_session.csrf_token:
                    self.logger.warning("Playwright session has no CSRF token - promos may not work")
                else:
                    self.logger.info(f"Got CSRF token: {playwright_session.csrf_token[:20]}...")

                # Apply cookies from Playwright session
                session.cookies.update(playwright_session.get_requests_cookies())
                session.headers.update(playwright_session.headers)

                self.logger.info(f"Applied Playwright session cookies for store {store_code}")

            except Exception as e:
                self.logger.warning(f"Playwright session failed: {e}")
                self.logger.warning("Falling back to manual session - promotion prices will NOT be available")
                self._setup_manual_session(session, store_code)
        else:
            self.logger.warning("Playwright disabled - promotion prices will NOT be available")
            self._setup_manual_session(session, store_code)

        self._http_sessions[store_code] = session
        return session

    def _setup_manual_session(self, session: requests.Session, store_code: str):
        """Set up session using manual store selection."""
        try:
            # First, visit the main page to get initial cookies
            self.logger.info(f"Setting up manual session for store {store_code}")

            main_response = session.get(
                self.BASE_URL,
                timeout=config.scraper.request_timeout
            )

            if main_response.ok:
                self.logger.debug("Got initial cookies from main page")

            # Set preferred store via API
            store_url = f"{self.BASE_URL}/store-finder/setPreferredStore"
            response = session.get(
                store_url,
                params={"preferredStoreName": store_code},
                timeout=config.scraper.request_timeout
            )

            if response.ok:
                self.logger.info(f"Manual store selection successful for {store_code}")
            else:
                self.logger.warning(f"Store selection returned {response.status_code}")

            # Update headers for subsequent requests
            session.headers.update({
                "referer": f"{self.BASE_URL}/",
                "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
            })

        except Exception as e:
            self.logger.error(f"Manual session setup failed: {e}")

    def scrape_page(
        self,
        page: int,
        store_code: str,
        province: str,
        session: Any,
        retry_count: int = 0,
    ) -> ScrapeResult:
        """
        Scrape a single page of Checkers products.

        Args:
            page: Page number (0-indexed)
            store_code: Store code
            province: Province name
            session: requests.Session to use
            retry_count: Number of retries attempted (for session refresh)

        Returns:
            ScrapeResult with products
        """
        start_time = time.time()
        result = ScrapeResult(
            page=page,
            store_code=store_code,
            province=province,
        )

        self.logger.info(f"Scraping page {page} for {province}")

        try:
            # Build URL
            url = f"{self.BASE_URL}{self.PRODUCT_URL}"
            params = {
                "q": ":relevance",
                "page": str(page),
            }

            # Make request
            response = session.get(url, params=params, timeout=config.scraper.request_timeout)

            # Handle 403 - session may be stale, retry with fresh session
            if response.status_code == 403 and retry_count < 2:
                self.logger.warning(f"Page {page} got 403, refreshing session (attempt {retry_count + 1})")

                # Clear cached sessions for this store
                if store_code in self._http_sessions:
                    del self._http_sessions[store_code]
                if store_code in self._sessions:
                    del self._sessions[store_code]

                # Clear session file cache
                cache_file = Path(f"data/sessions/checkers_{store_code}.json")
                if cache_file.exists():
                    try:
                        cache_file.unlink()
                        self.logger.debug(f"Deleted stale session cache: {cache_file}")
                    except Exception:
                        pass

                # Create fresh session and retry
                fresh_session = self.create_session(store_code)
                return self.scrape_page(page, store_code, province, fresh_session, retry_count + 1)

            if not response.ok:
                result.success = False
                result.error = f"HTTP {response.status_code}"
                return result

            # Parse HTML
            soup = BeautifulSoup(response.text, "html.parser")

            # Get product list JSON for heavy attributes API
            json_data_elem = soup.select_one(".productListJSON")
            json_data = json_data_elem.text if json_data_elem else "[]"

            # Find product elements
            product_elements = soup.select(".item-product")

            if not product_elements:
                self.logger.debug(f"No products found on page {page}")
                result.success = True
                return result

            # Parse basic product info
            products = []
            for item in product_elements:
                product = self._parse_product_element(item, province)
                if product:
                    products.append(product)

            # Get additional info from heavy attributes API
            products = self._enrich_with_heavy_attributes(
                products, json_data, session, store_code, page
            )

            # Assign indexes
            for product in products:
                self.current_index += 1
                product["index"] = str(self.current_index)

            result.products = products
            result.success = True
            result.duration_seconds = time.time() - start_time

            return result

        except Exception as e:
            self.logger.error(f"Error scraping page {page}: {e}")
            result.success = False
            result.error = str(e)
            result.duration_seconds = time.time() - start_time
            return result

    def _parse_product_element(
        self,
        element: BeautifulSoup,
        province: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Parse a single product element from the HTML.

        Args:
            element: BeautifulSoup element for the product
            province: Province name

        Returns:
            Product dictionary or None if parsing fails
        """
        try:
            # Get product name
            name_elem = element.select_one(".item-product__name")
            if not name_elem:
                return None
            name = name_elem.get_text(strip=True)

            # Get prices
            price_old = element.select_one(".before")
            price_old = price_old.get_text(strip=True) if price_old else None

            price_current = element.select_one(".now")
            price_current = price_current.get_text(strip=True) if price_current else None

            # Determine price and promotion
            if price_old:
                price = price_old
                promotion_price = price_current or "No promo"
            elif price_current:
                price = price_current
                promotion_price = "No promo"
            else:
                price = "Price not available"
                promotion_price = "No promo"

            # Get image URL
            image_url = self._get_product_image(element, name)

            return {
                "name": name,
                "price": price,
                "promotion_price": promotion_price,
                "retailer": self.RETAILER_NAME,
                "image_url": image_url,
                "promotion_valid": " ",
                "province": province,
                "normalized_name": normalize_product_name(name),
            }

        except Exception as e:
            self.logger.debug(f"Failed to parse product element: {e}")
            return None

    def _get_product_image(
        self,
        element: BeautifulSoup,
        product_name: str,
    ) -> str:
        """
        Extract and process product image.

        Args:
            element: Product HTML element
            product_name: Product name

        Returns:
            Image URL (Supabase or placeholder)
        """
        placeholder = PLACEHOLDER_IMAGES["checkers"]

        # Check if we have an existing image
        existing = self.existing_data.get(product_name, {})
        existing_url = existing.get("image_url")
        if existing_url and existing_url != placeholder:
            return existing_url

        # Skip image processing for now if configured or if network issues
        # This speeds up scraping significantly
        if config.scraper.use_cdn_urls:
            try:
                images = element.select("img")
                for img in images:
                    src = img.get("data-original-src") or img.get("src")
                    if src and "discovery-vitality" not in src:
                        if not src.startswith("http"):
                            src = f"https://www.checkers.co.za{src}"
                        return src
            except Exception:
                pass
            return placeholder

        try:
            # Find image element
            images = element.select("img")
            for img in images:
                src = img.get("data-original-src") or img.get("src")
                if src and "discovery-vitality" not in src:
                    # Ensure full URL - use products.checkers.co.za for images
                    if not src.startswith("http"):
                        src = f"https://products.checkers.co.za{src}"

                    # Process image (download, upload to Supabase)
                    return process_product_image(
                        src, product_name, self.RETAILER_NAME, existing_url
                    )

        except Exception as e:
            self.logger.debug(f"Image extraction failed for {product_name}: {e}")

        return placeholder

    def _enrich_with_heavy_attributes(
        self,
        products: List[Dict[str, Any]],
        json_data: str,
        session: requests.Session,
        store_code: str,
        page: int,
    ) -> List[Dict[str, Any]]:
        """
        Enrich products with data from the heavy attributes API.

        This API provides additional promotion details including
        sale prices, bonus buys, and validity dates.

        Args:
            products: List of basic product dictionaries
            json_data: JSON string for the API request body
            session: requests.Session
            store_code: Store code
            page: Page number (for referer header)

        Returns:
            Enriched products list
        """
        if not products or not json_data:
            return products

        try:
            # Get session data for CSRF token
            playwright_session = self._sessions.get(store_code)

            # Build headers - CSRF token is REQUIRED
            headers = {
                "accept": "text/plain, */*; q=0.01",
                "content-type": "application/json",
                "origin": self.BASE_URL,
                "referer": f"{self.BASE_URL}{self.PRODUCT_URL}?q=%3Arelevance&page={page}",
                "x-requested-with": "XMLHttpRequest",
            }

            # Add CSRF token - this is required for the API to work!
            if playwright_session and playwright_session.csrf_token:
                headers["csrftoken"] = playwright_session.csrf_token
                self.logger.debug(f"Using CSRF token: {playwright_session.csrf_token[:20]}...")
            else:
                self.logger.warning("No CSRF token available - API will likely fail")

            response = session.post(
                f"{self.BASE_URL}{self.HEAVY_ATTRIBUTES_URL}",
                headers=headers,
                data=json_data,
                timeout=config.scraper.request_timeout,
            )

            if not response.ok:
                self.logger.warning(f"Heavy attributes API returned {response.status_code}")
                if response.status_code == 400:
                    self.logger.warning("  Hint: CSRF token may be missing or invalid")
                return products

            api_data = response.json()

            # Match API data to products
            for product, api_item in zip(products, api_data):
                info = api_item.get("information", [{}])[0]

                # Get sale price
                sale_price = info.get("salePrice")
                if sale_price and sale_price != "" and not (isinstance(sale_price, float) and math.isnan(sale_price)):
                    product["promotion_price"] = f"R{sale_price}"

                # Check for bonus buys
                bonus_buys = info.get("includedInBonusBuys", [])
                if bonus_buys and product.get("promotion_price") == "No promo":
                    bundle_name = bonus_buys[0].get("name")
                    if bundle_name:
                        product["promotion_price"] = str(bundle_name)

                # Get validity date
                html_bbs = info.get("htmlBBs", "")
                if html_bbs:
                    html_unescaped = html.unescape(html_bbs)
                    soup = BeautifulSoup(html_unescaped, "html.parser")
                    valid_tag = soup.find("span", class_="item-product__valid")
                    if valid_tag:
                        product["promotion_valid"] = valid_tag.get_text(strip=True).replace("\xa0", " ")

            self.logger.debug(f"Enriched {len(products)} products with promo data")

        except Exception as e:
            self.logger.warning(f"Failed to enrich with heavy attributes: {e}")

        return products

    def get_total_pages(self, store_code: str, session: Any) -> int:
        """
        Get total number of pages for a store.

        Checkers typically has ~375 pages of food products.
        """
        return 375  # Known approximate value

    def refresh_sessions(self, force: bool = False):
        """
        Refresh all Playwright sessions.

        Args:
            force: Force refresh even if sessions aren't expired
        """
        if not self.session_manager:
            return

        self._sessions.clear()
        self._http_sessions.clear()

        if force:
            self.session_manager.clear_cache("checkers")

        self.logger.info("Sessions cleared, will be recreated on next request")


def run_checkers_scraper(
    provinces: Optional[List[str]] = None,
    start_page: int = 0,
    end_page: int = 375,
    upload_to_supabase: bool = True,
    concurrent: bool = True,
):
    """
    Convenience function to run the Checkers scraper.

    Args:
        provinces: List of provinces to scrape (None = all)
        start_page: First page to scrape
        end_page: Last page to scrape
        upload_to_supabase: Whether to upload results to Supabase
        concurrent: Use concurrent page scraping

    Returns:
        ScraperStats with results
    """
    scraper = CheckersScraper()

    stats = scraper.run(
        concurrent=concurrent,
        start_page=start_page,
        end_page=end_page,
        provinces=provinces,
    )

    if upload_to_supabase:
        scraper.upload_to_supabase()

    return stats


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape Checkers products")
    parser.add_argument("--provinces", nargs="*", help="Provinces to scrape")
    parser.add_argument("--start-page", type=int, default=0, help="Starting page")
    parser.add_argument("--end-page", type=int, default=375, help="Ending page")
    parser.add_argument("--no-upload", action="store_true", help="Skip Supabase upload")
    parser.add_argument("--sequential", action="store_true", help="Disable concurrent scraping")

    args = parser.parse_args()

    stats = run_checkers_scraper(
        provinces=args.provinces,
        start_page=args.start_page,
        end_page=args.end_page,
        upload_to_supabase=not args.no_upload,
        concurrent=not args.sequential,
    )

    print(f"\n{'='*50}")
    print(f"Scraping complete!")
    print(f"  Products: {stats.total_products:,}")
    print(f"  Pages: {stats.total_pages}")
    print(f"  Duration: {stats.duration_seconds:.1f}s")
    print(f"  Provinces: {stats.provinces_completed}")
    if stats.errors:
        print(f"  Errors: {len(stats.errors)}")