"""
Simple test script to verify Checkers scraping works.
Run this to debug basic connectivity and parsing.
"""

import requests
from bs4 import BeautifulSoup

def test_checkers_scrape():
    """Test basic scraping without the full framework."""

    print("=" * 60)
    print("CHECKERS SCRAPER TEST")
    print("=" * 60)

    # Create session
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    })

    # Step 1: Test main page
    print("\n[1/4] Testing main page connection...")
    try:
        resp = session.get("https://products.checkers.co.za", timeout=30)
        print(f"  ✓ Main page: {resp.status_code}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False

    # Step 2: Set store
    print("\n[2/4] Setting store to Gauteng (7303)...")
    try:
        store_url = "https://products.checkers.co.za/store-finder/setPreferredStore"
        resp = session.get(store_url, params={"preferredStoreName": "7303"}, timeout=30)
        print(f"  ✓ Store set: {resp.status_code}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False

    # Step 3: Get products page
    print("\n[3/4] Fetching products page 0...")
    try:
        url = "https://products.checkers.co.za/c-2413/All-Departments/Food"
        params = {"q": ":relevance", "page": "0"}
        resp = session.get(url, params=params, timeout=30)
        print(f"  ✓ Products page: {resp.status_code}")
        print(f"  ✓ Response size: {len(resp.text)} bytes")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False

    # Step 4: Parse products
    print("\n[4/4] Parsing products...")
    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        products = soup.select(".item-product")
        print(f"  ✓ Found {len(products)} product elements")

        if products:
            print("\n  First 3 products:")
            for i, item in enumerate(products[:3]):
                name_elem = item.select_one(".item-product__name")
                price_elem = item.select_one(".now") or item.select_one(".before")

                name = name_elem.get_text(strip=True) if name_elem else "N/A"
                price = price_elem.get_text(strip=True) if price_elem else "N/A"

                print(f"    {i+1}. {name[:50]}... - {price}")

    except Exception as e:
        print(f"  ✗ Parse error: {e}")
        return False

    print("\n" + "=" * 60)
    print("TEST PASSED! Basic scraping is working.")
    print("=" * 60)

    return True


if __name__ == "__main__":
    test_checkers_scrape()
