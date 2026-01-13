"""
Woolworths Scraper for Savvy Grocery

Scrapes products from www.woolworths.co.za using:
- JSON API (no HTML parsing needed)
- Place confirmation for location setting
- Category-based scraping

Woolworths requires setting a delivery location first, then iterates
through food categories to get all products.
"""

import html
import json
import re
import time
from typing import Any, Dict, List, Optional

import requests

from config import WOOLWORTHS_STORES, WOOLWORTHS_CATEGORIES, PLACEHOLDER_IMAGES, config
from scrapers.base import BaseScraper, ScrapeResult
from utils import get_random_user_agent, normalize_product_name, save_to_csv


class WoolworthsScraper(BaseScraper):
    """Scraper for Woolworths products."""

    RETAILER_NAME = "Woolworths"
    OUTPUT_FILENAME = "products_woolworths.csv"
    BASE_URL = "https://www.woolworths.co.za"
    SEARCH_URL = "https://www.woolworths.co.za/server/searchCategory"
    CONFIRM_PLACE_URL = "https://www.woolworths.co.za/server/confirmPlace"

    PRODUCTS_PER_PAGE = 24

    def __init__(self, categories: Optional[Dict[str, str]] = None):
        """
        Initialize Woolworths scraper.

        Args:
            categories: Dict of category_name -> category_code to scrape
                       (None = all categories)
        """
        super().__init__(stores=WOOLWORTHS_STORES)
        self.categories = categories or WOOLWORTHS_CATEGORIES
        self._sessions: Dict[str, requests.Session] = {}
        self._offer_valid_cache: Dict[str, str] = {}
        self.products: List[Dict] = []

    def get_store_code(self, province: str) -> Optional[str]:
        """Get store info (nickname, place_id) for a province."""
        # Woolworths uses province name as key
        return province if province in self.stores else None

    def create_session(self, province: str) -> Optional[requests.Session]:
        """
        Create a session with location set for Woolworths.

        Woolworths requires a confirmPlace call to set the delivery location.
        """
        if province in self._sessions:
            return self._sessions[province]

        store_info = self.stores.get(province)
        if not store_info:
            self.logger.warning(f"No store info for province: {province}")
            return None

        nickname, place_id = store_info

        session = requests.Session()
        session.headers.update({
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "x-requested-by": "Woolworths Online",
            "user-agent": get_random_user_agent(),
            "referer": f"{self.BASE_URL}/",
        })

        # Confirm place to set location
        confirm_data = {
            "deliveryType": "Standard",
            "address": {
                "placeId": place_id,
                "nickname": nickname,
            },
            "addSuburbToOrder": True,
        }

        try:
            response = session.post(
                self.CONFIRM_PLACE_URL,
                data=json.dumps(confirm_data),
                timeout=30,
            )

            if response.ok:
                self.logger.debug(f"Set location for {province}: {nickname}")
                self._sessions[province] = session
                return session
            else:
                self.logger.warning(f"Failed to confirm place for {province}: {response.status_code}")
                return None

        except Exception as e:
            self.logger.warning(f"Error confirming place for {province}: {e}")
            return None

    def get_offer_valid_date(self, session: requests.Session, province: str) -> str:
        """
        Get the current offer valid date from DailyDifference page.

        Woolworths shows promotion validity dates on their specials page.
        """
        if province in self._offer_valid_cache:
            return self._offer_valid_cache[province]

        try:
            params = {
                "pageURL": "/cat/DailyDifference/_/N-1z13sk5ZakhueZxtznwk",
                "No": "0",
                "Nrpp": "24",
            }

            response = session.get(
                self.SEARCH_URL,
                params=params,
                timeout=30,
            )

            if response.ok:
                data = response.json()
                offer_dates = self._extract_offer_dates(data)
                if offer_dates:
                    self.logger.info(f"Found offer date: {offer_dates[0]}")
                    self._offer_valid_cache[province] = offer_dates[0]
                    return offer_dates[0]
                else:
                    self.logger.warning(f"No offer dates found in DailyDifference response for {province}")

        except Exception as e:
            self.logger.warning(f"Could not get offer date: {e}")

        self._offer_valid_cache[province] = ""
        return ""

    def _extract_offer_dates(self, obj: Any) -> List[str]:
        """Recursively search for 'Offer valid ...' text in response."""
        results = []

        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "content" and isinstance(value, str):
                    clean_text = html.unescape(value)

                    # Try multiple patterns - Woolworths changes format sometimes
                    patterns = [
                        # "Offer valid  5-25 January" (double space, no year) - CURRENT FORMAT
                        r'Offer valid\s+\d{1,2}\s*-\s*\d{1,2}\s+\w+',
                        # "Offer valid 6 - 19 January 2026" (with year)
                        r'Offer valid\s+\d{1,2}\s*-\s*\d{1,2}\s+\w+\s+\d{4}',
                        # "Valid 6 - 19 January 2026"
                        r'Valid\s+\d{1,2}\s*-\s*\d{1,2}\s+\w+\s+\d{4}',
                        # "Valid 5-25 January" (no year)
                        r'Valid\s+\d{1,2}\s*-\s*\d{1,2}\s+\w+',
                        # "Offer valid until 19 January 2026"
                        r'Offer valid until\s+\d{1,2}\s+\w+\s+\d{4}',
                        # "Valid until 19 January"
                        r'Valid until\s+\d{1,2}\s+\w+',
                    ]

                    for pattern in patterns:
                        match = re.search(pattern, clean_text, re.IGNORECASE)
                        if match:
                            results.append(match.group().strip())
                            break
                else:
                    results.extend(self._extract_offer_dates(value))
        elif isinstance(obj, list):
            for item in obj:
                results.extend(self._extract_offer_dates(item))

        return results

    def scrape_page(
        self,
        page: int,
        store_code: str,  # This is actually category_code for Woolworths
        province: str,
        session: Any,
        retry_count: int = 0,
        category_name: str = "",
    ) -> ScrapeResult:
        """
        Scrape a single page of Woolworths products.

        For Woolworths, store_code is actually the category code.
        """
        start_time = time.time()
        result = ScrapeResult(
            page=page,
            store_code=store_code,
            province=province,
        )

        try:
            params = {
                "pageURL": f"/cat/Food/{category_name}/_/N-{store_code}",
                "No": str(page * self.PRODUCTS_PER_PAGE),
                "Nrpp": str(self.PRODUCTS_PER_PAGE),
            }

            response = session.get(
                self.SEARCH_URL,
                params=params,
                headers={
                    "referer": f"{self.BASE_URL}/cat/Food/{category_name}/_/N-{store_code}",
                },
                timeout=config.scraper.request_timeout,
            )

            # Handle rate limiting
            if response.status_code in (403, 429) and retry_count < 2:
                self.logger.warning(f"Page {page} got {response.status_code}, retrying")
                time.sleep(5 * (retry_count + 1))
                return self.scrape_page(page, store_code, province, session, retry_count + 1, category_name)

            if not response.ok:
                result.success = False
                result.error = f"HTTP {response.status_code}"
                return result

            data = response.json()

            # Navigate to products in response
            try:
                contents = data.get("contents", [{}])[0]
                main_content = contents.get("mainContent", [{}])[0]
                records_container = main_content.get("contents", [{}])[0]
                products_data = records_container.get("records", [])
            except (IndexError, AttributeError):
                products_data = []

            if not products_data:
                result.success = True
                return result

            # Get offer valid date
            offer_valid = self._offer_valid_cache.get(province, "")

            # Parse products
            products = []
            for item in products_data:
                product = self._parse_product(item, province, offer_valid)
                if product:
                    products.append(product)

            # Get total pages from response
            try:
                secondary = contents.get("secondaryContent", [{}])[0]
                category_dims = secondary.get("categoryDimensions", [{}])[0]
                total_count = category_dims.get("count", 0)
                result.total_pages = (total_count + self.PRODUCTS_PER_PAGE - 1) // self.PRODUCTS_PER_PAGE
            except (IndexError, KeyError):
                pass

            result.products = products
            result.success = True
            result.duration = time.time() - start_time

            return result

        except json.JSONDecodeError as e:
            result.success = False
            result.error = f"JSON parse error: {e}"
            return result
        except Exception as e:
            result.success = False
            result.error = str(e)
            return result

    def _parse_product(self, item: Dict, province: str, offer_valid: str) -> Optional[Dict]:
        """Parse a product from Woolworths API response."""
        try:
            attrs = item.get("attributes", {})
            name = attrs.get("p_displayName")

            if not name or name == "FFF_Water_Content_Card_Wk43":
                return None

            # Get price
            starting_price = item.get("startingPrice", {})
            price_value = starting_price.get("p_pl10")
            price = f"R{price_value}" if price_value else "Price not available"

            # Get promotion
            promo_price = attrs.get("PROMOTION", "No promo")

            # Get image URL (CDN URL works for Woolworths)
            image_url = attrs.get("p_externalImageReference")
            if not image_url:
                image_url = PLACEHOLDER_IMAGES.get("woolworths")

            product = {
                "name": name,
                "price": price,
                "promotion_price": promo_price,
                "retailer": self.RETAILER_NAME,
                "image_url": image_url,
                "promotion_valid": offer_valid,
                "province": province,
                "normalized_name": normalize_product_name(name),
            }

            return product

        except Exception as e:
            self.logger.warning(f"Failed to parse product: {e}")
            return None

    def run(
        self,
        provinces: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        concurrent: bool = False,
        start_page: int = 0,
        end_page: Optional[int] = None,
    ):
        """
        Run the Woolworths scraper.

        Args:
            provinces: List of provinces to scrape (None = all)
            categories: List of category names to scrape (None = all)
            concurrent: Use concurrent scraping (not implemented yet)
            start_page: Starting page number
            end_page: Ending page number (None = all pages)
        """
        self.logger.info("Starting Woolworths scraper")

        # Load existing data
        self.load_existing_data()

        # Determine provinces
        if provinces:
            provinces_to_scrape = [p for p in provinces if p in self.stores]
        else:
            provinces_to_scrape = list(self.stores.keys())

        # Determine categories
        if categories:
            categories_to_scrape = {k: v for k, v in self.categories.items() if k in categories}
        else:
            categories_to_scrape = self.categories

        total_products = 0
        start_time = time.time()

        for province in provinces_to_scrape:
            self.logger.info(f"Starting scrape for {province}")

            session = self.create_session(province)
            if not session:
                self.logger.error(f"Could not create session for {province}")
                self.stats.errors.append(f"Failed to create session for {province}")
                continue

            # Get offer valid date
            offer_valid = self.get_offer_valid_date(session, province)
            if offer_valid:
                self.logger.info(f"Offer valid: {offer_valid}")

            province_products = 0

            # Iterate through categories
            for category_name, category_code in categories_to_scrape.items():
                self.logger.info(f"  Category: {category_name}")

                page = start_page
                category_products = 0
                max_page = end_page

                while True:
                    result = self.scrape_page(
                        page=page,
                        store_code=category_code,
                        province=province,
                        session=session,
                        category_name=category_name,
                    )

                    if result.success and result.products:
                        for product in result.products:
                            product["index"] = self.current_index
                            self.products.append(product)
                            self.current_index += 1
                            category_products += 1

                        self.logger.debug(f"    Page {page}: {len(result.products)} products")

                        # Update max_page from response if not set
                        if max_page is None and result.total_pages:
                            max_page = result.total_pages - 1

                    elif not result.success:
                        self.logger.warning(f"    Page {page} failed: {result.error}")
                        break
                    else:
                        # Empty page
                        break

                    page += 1

                    # Check if we've reached end
                    if max_page is not None and page > max_page:
                        break

                    # Small delay
                    time.sleep(config.scraper.page_delay)

                self.logger.info(f"    {category_name}: {category_products} products")
                province_products += category_products

            self.logger.info(f"Completed {province}: {province_products} products")
            total_products += province_products

        # Update stats
        self.stats.total_products = total_products
        self.stats.duration = time.time() - start_time

        # Save to CSV
        save_to_csv(self.products, self.output_path)

        self.logger.info(f"Scraping complete: {total_products} products in {self.stats.duration:.1f}s")

        return self.stats


def run_woolworths_scraper(
    provinces: Optional[List[str]] = None,
    categories: Optional[List[str]] = None,
    upload_to_supabase: bool = True,
    concurrent: bool = False,
    start_page: int = 0,
    end_page: Optional[int] = None,
):
    """
    Convenience function to run the Woolworths scraper.

    Args:
        provinces: List of provinces to scrape
        categories: List of categories to scrape
        upload_to_supabase: Whether to upload results
        concurrent: Use concurrent scraping
        start_page: Starting page
        end_page: Ending page

    Returns:
        ScrapeStats with results
    """
    scraper = WoolworthsScraper()
    stats = scraper.run(
        provinces=provinces,
        categories=categories,
        concurrent=concurrent,
        start_page=start_page,
        end_page=end_page,
    )

    if upload_to_supabase:
        scraper.upload_to_supabase()

    return stats