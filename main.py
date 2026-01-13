"""
Savvy Grocery Scraper - Main Entry Point

Run all scrapers or specific retailers with proper configuration,
logging, and notifications.
"""

import argparse
import sys
import time
from datetime import datetime
from typing import List, Optional

from config import config
from scrapers import (
    run_checkers_scraper,
    run_shoprite_scraper,
    run_pnp_scraper,
    run_woolworths_scraper,
)
from utils import notify_summary, setup_logger, Timer


def run_all_scrapers(
    retailers: Optional[List[str]] = None,
    provinces: Optional[List[str]] = None,
    upload: bool = True,
    concurrent: bool = True,
):
    """
    Run scrapers for specified retailers.

    Args:
        retailers: List of retailers to scrape (None = all)
        provinces: List of provinces to scrape (None = all)
        upload: Whether to upload to Supabase
        concurrent: Use concurrent scraping

    Returns:
        List of results for each retailer
    """
    logger = setup_logger("main")

    all_retailers = ["checkers", "shoprite", "pnp", "woolworths"]
    retailers_to_run = retailers or all_retailers

    results = []
    total_start = time.time()

    logger.info(f"Starting scraping run for: {', '.join(retailers_to_run)}")
    logger.info(f"Provinces: {provinces or 'All'}")

    for retailer in retailers_to_run:
        retailer_lower = retailer.lower()
        result = {
            "retailer": retailer.title(),
            "products": 0,
            "success": False,
            "error": None,
            "duration": 0,
        }

        start = time.time()

        try:
            if retailer_lower == "checkers":
                stats = run_checkers_scraper(
                    provinces=provinces,
                    upload_to_supabase=upload,
                    concurrent=concurrent,
                )
                result["products"] = stats.total_products
                result["success"] = len(stats.errors) == 0

            elif retailer_lower == "shoprite":
                stats = run_shoprite_scraper(
                    provinces=provinces,
                    upload_to_supabase=upload,
                    concurrent=concurrent,
                )
                result["products"] = stats.total_products
                result["success"] = len(stats.errors) == 0

            elif retailer_lower == "pnp":
                stats = run_pnp_scraper(
                    provinces=provinces,
                    upload_to_supabase=upload,
                    concurrent=concurrent,
                )
                result["products"] = stats.total_products
                result["success"] = len(stats.errors) == 0

            elif retailer_lower == "woolworths":
                stats = run_woolworths_scraper(
                    provinces=provinces,
                    upload_to_supabase=upload,
                    concurrent=concurrent,
                )
                result["products"] = stats.total_products
                result["success"] = len(stats.errors) == 0

            else:
                logger.warning(f"Unknown retailer: {retailer}")
                result["error"] = "Unknown retailer"

        except Exception as e:
            logger.error(f"Failed to scrape {retailer}: {e}")
            result["error"] = str(e)

        result["duration"] = time.time() - start
        results.append(result)

        logger.info(
            f"{retailer} complete: {result['products']} products in "
            f"{result['duration']:.1f}s"
        )

    total_duration = time.time() - total_start

    # Send summary notification
    notify_summary(results, total_duration)

    # Print summary
    print("\n" + "=" * 60)
    print("SCRAPING SUMMARY")
    print("=" * 60)

    total_products = 0
    for result in results:
        status = "✅" if result["success"] else "❌"
        print(f"{status} {result['retailer']}: {result['products']:,} products")
        if result["error"]:
            print(f"   Error: {result['error']}")
        total_products += result["products"]

    print("-" * 60)
    print(f"Total: {total_products:,} products in {total_duration:.1f}s")
    print("=" * 60)

    return results


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="Savvy Grocery Scraper - Scrape product data from SA retailers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                          # Scrape all retailers, all provinces
  python main.py --retailers checkers     # Scrape only Checkers
  python main.py --provinces Gauteng      # Scrape only Gauteng stores
  python main.py --no-upload              # Scrape without uploading to Supabase
  python main.py --sequential             # Disable concurrent scraping
        """
    )

    parser.add_argument(
        "--retailers",
        nargs="+",
        choices=["checkers", "shoprite", "pnp", "woolworths"],
        help="Retailers to scrape (default: all)",
    )

    parser.add_argument(
        "--provinces",
        nargs="+",
        help="Provinces to scrape (default: all)",
    )

    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Skip uploading to Supabase",
    )

    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Disable concurrent page scraping",
    )

    parser.add_argument(
        "--validate-config",
        action="store_true",
        help="Validate configuration and exit",
    )

    args = parser.parse_args()

    # Validate configuration
    try:
        config.validate()
        if args.validate_config:
            print("✅ Configuration is valid")
            print(f"   Supabase URL: {config.supabase.url[:30]}...")
            print(f"   Discord notifications: {'Enabled' if config.discord.is_configured() else 'Disabled'}")
            print(f"   Max threads: {config.scraper.max_threads}")
            return 0
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        return 1

    # Run scrapers
    results = run_all_scrapers(
        retailers=args.retailers,
        provinces=args.provinces,
        upload=not args.no_upload,
        concurrent=not args.sequential,
    )

    # Return exit code based on results
    all_success = all(r["success"] for r in results)
    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())