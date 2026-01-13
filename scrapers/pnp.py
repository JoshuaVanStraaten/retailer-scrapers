"""
Pick n Pay Scraper for Savvy Grocery

Scrapes products from www.pnp.co.za using:
- JSON API (no HTML parsing needed)
- Direct requests (no Playwright required)

PnP uses a simple JSON API that returns product data directly.
"""

import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from config import PNP_STORES, PLACEHOLDER_IMAGES, config
from scrapers.base import BaseScraper, ScrapeResult
from utils import get_random_user_agent, normalize_product_name, save_to_csv


class PnPScraper(BaseScraper):
    """Scraper for Pick n Pay products."""

    RETAILER_NAME = "Pick n Pay"
    OUTPUT_FILENAME = "products_pnp.csv"
    BASE_URL = "https://www.pnp.co.za"
    API_URL = "https://www.pnp.co.za/pnphybris/v2/pnp-spa/products/search"

    # Product fields to request from API
    PRODUCT_FIELDS = [
        "code", "name", "brandSellerId", "averageWeight", "summary",
        "price(FULL)", "images(DEFAULT)", "stock(FULL)", "averageRating",
        "numberOfReviews", "variantOptions", "maxOrderQuantity",
        "productDisplayBadges(DEFAULT)", "allowedQuantities(DEFAULT)",
        "available", "defaultQuantityOfUom", "inStockIndicator",
        "defaultUnitOfMeasure", "potentialPromotions(FULL)", "categoryNames"
    ]

    OTHER_FIELDS = [
        "facets", "breadcrumbs", "pagination(DEFAULT)", "sorts(DEFAULT)",
        "freeTextSearch", "currentQuery", "keywordRedirectUrl"
    ]

    PRODUCTS_PER_PAGE = 72

    def __init__(self):
        super().__init__(stores=PNP_STORES)
        self._sessions: Dict[str, requests.Session] = {}
        self.products: List[Dict] = []

    def get_store_code(self, province: str) -> Optional[str]:
        """Get store code for a province."""
        for code, prov in self.stores.items():
            if prov.lower() == province.lower():
                return code
        return None

    def create_session(self, store_code: str) -> requests.Session:
        """Create a requests session for PnP API."""
        if store_code in self._sessions:
            return self._sessions[store_code]

        session = requests.Session()
        session.headers.update({
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json",
            "origin": self.BASE_URL,
            "referer": f"{self.BASE_URL}/c/pnpbase",
            "user-agent": get_random_user_agent(),
            "x-anonymous-consents": "%5B%5D",
        })

        self._sessions[store_code] = session
        return session

    def get_total_pages(self, store_code: str) -> int:
        """Get total number of pages for a store."""
        session = self.create_session(store_code)

        fields = f"products({','.join(self.PRODUCT_FIELDS)}),{','.join(self.OTHER_FIELDS)}"

        params = {
            "fields": fields,
            "query": ":relevance:allCategories:pnpbase",
            "pageSize": "1",  # Just need pagination info
            "currentPage": "0",
            "storeCode": store_code,
            "lang": "en",
            "curr": "ZAR",
        }

        try:
            response = session.post(self.API_URL, params=params, timeout=30)
            if response.ok:
                data = response.json()
                pagination = data.get("pagination", {})
                total_results = pagination.get("totalResults", 0)
                page_size = pagination.get("pageSize", self.PRODUCTS_PER_PAGE)
                total_pages = (total_results + page_size - 1) // page_size
                self.logger.info(f"Store {store_code}: {total_results} products, {total_pages} pages")
                return total_pages
        except Exception as e:
            self.logger.warning(f"Failed to get total pages: {e}")

        return 138  # Default fallback

    def scrape_page(
        self,
        page: int,
        store_code: str,
        province: str,
        session: Any,
        retry_count: int = 0,
    ) -> ScrapeResult:
        """Scrape a single page of PnP products."""
        start_time = time.time()
        result = ScrapeResult(
            page=page,
            store_code=store_code,
            province=province,
        )

        try:
            fields = f"products({','.join(self.PRODUCT_FIELDS)}),{','.join(self.OTHER_FIELDS)}"

            params = {
                "fields": fields,
                "query": ":relevance:allCategories:pnpbase",
                "pageSize": str(self.PRODUCTS_PER_PAGE),
                "currentPage": str(page),
                "storeCode": store_code,
                "lang": "en",
                "curr": "ZAR",
            }

            response = session.post(
                self.API_URL,
                params=params,
                timeout=config.scraper.request_timeout,
            )

            # Handle 403/429 with retry
            if response.status_code in (403, 429) and retry_count < 2:
                self.logger.warning(f"Page {page} got {response.status_code}, retrying (attempt {retry_count + 1})")
                time.sleep(5 * (retry_count + 1))  # Exponential backoff
                return self.scrape_page(page, store_code, province, session, retry_count + 1)

            if not response.ok:
                result.success = False
                result.error = f"HTTP {response.status_code}"
                return result

            data = response.json()
            products_data = data.get("products", [])

            if not products_data:
                result.success = True
                return result

            # Parse products
            products = []
            for item in products_data:
                product = self._parse_product(item, province)
                if product:
                    products.append(product)

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

    def _parse_product(self, item: Dict, province: str) -> Optional[Dict]:
        """Parse a product from PnP API response."""
        try:
            name = item.get("name")
            if not name:
                return None

            # Get price
            price_data = item.get("price", {})
            price = price_data.get("formattedValue", "Price not available")

            # Get promotion
            promo_price, promo_valid = self._get_promotion(item.get("potentialPromotions", []))

            # Get image URL (CDN URL works for PnP)
            image_url = None
            images = item.get("images", [])
            for img in images:
                if img.get("format") == "carousel":
                    image_url = img.get("url")
                    break

            if not image_url and images:
                # Fallback to first image
                image_url = images[0].get("url")

            if not image_url:
                image_url = PLACEHOLDER_IMAGES.get("pnp")

            # Build product dict
            product = {
                "name": name,
                "price": price,
                "promotion_price": promo_price,
                "retailer": self.RETAILER_NAME,
                "image_url": image_url,
                "promotion_valid": promo_valid,
                "province": province,
                "normalized_name": normalize_product_name(name),
            }

            return product

        except Exception as e:
            self.logger.warning(f"Failed to parse product: {e}")
            return None

    def _get_promotion(self, promotions: List) -> tuple:
        """Extract promotion info from potentialPromotions."""
        if not promotions:
            return "No promo", ""

        promo = promotions[0] if isinstance(promotions, list) else promotions
        if not promo:
            return "No promo", ""

        message = promo.get("promotionTextMessage", "").strip()
        end_date_str = promo.get("endDate")

        # Format end date
        formatted_date = ""
        if end_date_str:
            try:
                end_date = datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%S%z")
                formatted_date = f"Valid until {end_date.strftime('%d %B %Y')}"
            except Exception:
                pass

        return message or "No promo", formatted_date

    def run(
        self,
        provinces: Optional[List[str]] = None,
        concurrent: bool = False,  # PnP is fast, sequential is fine
        start_page: int = 0,
        end_page: Optional[int] = None,
    ):
        """
        Run the PnP scraper.

        Args:
            provinces: List of provinces to scrape (None = all)
            concurrent: Use concurrent scraping (not needed for PnP)
            start_page: Starting page number
            end_page: Ending page number (None = all pages)
        """
        self.logger.info("Starting PnP scraper")

        # Load existing data
        self.load_existing_data()

        # Determine provinces to scrape
        if provinces:
            provinces_to_scrape = [(code, prov) for code, prov in self.stores.items()
                                   if prov in provinces]
        else:
            provinces_to_scrape = list(self.stores.items())

        total_products = 0
        start_time = time.time()

        for store_code, province in provinces_to_scrape:
            self.logger.info(f"Starting scrape for {province} (store {store_code})")

            session = self.create_session(store_code)

            # Get total pages if not specified
            max_page = end_page if end_page is not None else self.get_total_pages(store_code)

            province_products = 0
            page = start_page

            while page <= max_page:
                result = self.scrape_page(page, store_code, province, session)

                if result.success and result.products:
                    # Assign indices and add to collection
                    for product in result.products:
                        product["index"] = self.current_index
                        self.products.append(product)
                        self.current_index += 1
                        province_products += 1

                    self.logger.info(f"Page {page}: {len(result.products)} products (total: {province_products})")
                elif not result.success:
                    self.logger.warning(f"Page {page} failed: {result.error}")
                    self.stats.errors.append(f"{province} page {page}: {result.error}")
                else:
                    # Empty page - might be end of results
                    self.logger.info(f"Page {page}: No products, stopping")
                    break

                page += 1

                # Small delay between pages
                time.sleep(config.scraper.page_delay)

            self.logger.info(f"Completed {province}: {province_products} products")
            total_products += province_products

        # Update stats
        self.stats.total_products = total_products
        self.stats.duration = time.time() - start_time

        # Save to CSV
        save_to_csv(self.products, self.output_path)

        self.logger.info(f"Scraping complete: {total_products} products in {self.stats.duration:.1f}s")

        return self.stats


def run_pnp_scraper(
    provinces: Optional[List[str]] = None,
    upload_to_supabase: bool = True,
    concurrent: bool = False,
    start_page: int = 0,
    end_page: Optional[int] = None,
):
    """
    Convenience function to run the PnP scraper.

    Args:
        provinces: List of provinces to scrape
        upload_to_supabase: Whether to upload results
        concurrent: Use concurrent scraping
        start_page: Starting page
        end_page: Ending page

    Returns:
        ScrapeStats with results
    """
    scraper = PnPScraper()
    stats = scraper.run(
        provinces=provinces,
        concurrent=concurrent,
        start_page=start_page,
        end_page=end_page,
    )

    if upload_to_supabase:
        scraper.upload_to_supabase()

    return stats