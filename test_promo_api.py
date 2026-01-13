"""
Test script for Playwright session and Heavy Attributes API.
This tests if we can get promotion prices properly.
"""

import json
import asyncio
from playwright.async_api import async_playwright


async def test_checkers_session():
    """Test creating a session and calling the Heavy Attributes API."""

    print("=" * 60)
    print("CHECKERS SESSION & PROMO API TEST")
    print("=" * 60)

    store_code = "7303"  # Gauteng
    base_url = "https://products.checkers.co.za"

    async with async_playwright() as p:
        print("\n[1/6] Launching browser...")
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        page.set_default_timeout(60000)

        try:
            # Step 1: Go to main page
            print("\n[2/6] Loading main page...")
            await page.goto(base_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)
            print("  ✓ Main page loaded")

            # Step 2: Set store
            print(f"\n[3/6] Setting store to {store_code}...")
            store_url = f"{base_url}/store-finder/setPreferredStore?preferredStoreName={store_code}"
            await page.goto(store_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)
            print("  ✓ Store set")

            # Step 3: Load products page
            print("\n[4/6] Loading products page...")
            product_url = f"{base_url}/c-2413/All-Departments/Food?q=%3Arelevance&page=0"
            await page.goto(product_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)
            print("  ✓ Products page loaded")

            # Step 4: Extract cookies
            print("\n[5/6] Extracting session data...")
            cookies = await context.cookies()

            cookie_dict = {}
            jsessionid = None
            csrf_token = None

            for cookie in cookies:
                cookie_dict[cookie['name']] = cookie['value']
                if cookie['name'] == 'JSESSIONID':
                    jsessionid = cookie['value']

            print(f"  ✓ Got {len(cookies)} cookies")
            print(f"  ✓ JSESSIONID: {jsessionid[:30] if jsessionid else 'NOT FOUND'}...")

            # Try to find CSRF token
            csrf_token = await page.evaluate("""
                () => {
                    const scripts = document.querySelectorAll('script');
                    for (const script of scripts) {
                        const match = script.textContent.match(/csrf[Tt]oken["']?\\s*[:=]\\s*["']([^"']+)["']/);
                        if (match) return match[1];
                    }
                    // Look for it in the HTML
                    const html = document.documentElement.innerHTML;
                    const csrfMatch = html.match(/CSRFToken["']?\\s*[:=]\\s*["']([a-f0-9-]+)["']/i);
                    if (csrfMatch) return csrfMatch[1];
                    return null;
                }
            """)
            print(f"  ✓ CSRF Token: {csrf_token[:30] if csrf_token else 'NOT FOUND'}...")

            # Step 5: Get product JSON and test Heavy Attributes API
            print("\n[6/6] Testing Heavy Attributes API...")

            # Extract productListJSON from page
            product_json = await page.evaluate("""
                () => {
                    const elem = document.querySelector('.productListJSON');
                    return elem ? elem.textContent : null;
                }
            """)

            if not product_json:
                print("  ✗ Could not find productListJSON on page")
                return False

            print(f"  ✓ Got product JSON ({len(product_json)} chars)")

            # Make API request using page.evaluate to use browser's session
            api_result = await page.evaluate("""
                async (jsonData) => {
                    try {
                        const response = await fetch('/populateProductsWithHeavyAttributes', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'X-Requested-With': 'XMLHttpRequest',
                                'Accept': 'text/plain, */*; q=0.01'
                            },
                            body: jsonData
                        });

                        if (!response.ok) {
                            return { error: `HTTP ${response.status}`, status: response.status };
                        }

                        const data = await response.json();
                        return { success: true, data: data.slice(0, 3) };  // Return first 3 items
                    } catch (e) {
                        return { error: e.message };
                    }
                }
            """, product_json)

            if api_result.get('error'):
                print(f"  ✗ API Error: {api_result['error']}")

                # Try with CSRF header
                print("\n  Retrying with explicit CSRF header...")
                api_result = await page.evaluate("""
                    async ([jsonData, csrfToken]) => {
                        try {
                            const headers = {
                                'Content-Type': 'application/json',
                                'X-Requested-With': 'XMLHttpRequest',
                                'Accept': 'text/plain, */*; q=0.01'
                            };
                            if (csrfToken) {
                                headers['csrftoken'] = csrfToken;
                            }

                            const response = await fetch('/populateProductsWithHeavyAttributes', {
                                method: 'POST',
                                headers: headers,
                                body: jsonData
                            });

                            if (!response.ok) {
                                return { error: `HTTP ${response.status}`, status: response.status };
                            }

                            const data = await response.json();
                            return { success: true, data: data.slice(0, 3) };
                        } catch (e) {
                            return { error: e.message };
                        }
                    }
                """, [product_json, csrf_token])

            if api_result.get('success'):
                print("  ✓ Heavy Attributes API SUCCESS!")
                print("\n  Sample promo data:")
                for i, item in enumerate(api_result['data'][:3]):
                    info = item.get('information', [{}])[0]
                    sale_price = info.get('salePrice', 'N/A')
                    bonus_buys = info.get('includedInBonusBuys', [])
                    print(f"    {i+1}. Sale Price: R{sale_price}, Bonus Buys: {len(bonus_buys)}")
            else:
                print(f"  ✗ API failed: {api_result.get('error')}")

                # Print helpful debug info
                print("\n  Debug info:")
                print(f"    Cookies: {list(cookie_dict.keys())}")

        finally:
            await browser.close()

    print("\n" + "=" * 60)
    print("TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(test_checkers_session())
