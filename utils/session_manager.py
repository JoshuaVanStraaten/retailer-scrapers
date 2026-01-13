"""
Playwright Session Manager for Shoprite Group Sites

This module handles browser-based session creation for Checkers and Shoprite.
It uses Playwright to:
1. Load the site in a real browser
2. Set the preferred store (which triggers necessary cookies)
3. Extract session cookies and CSRF tokens
4. Return them for use with the requests library

This hybrid approach provides the reliability of browser automation
with the speed of direct HTTP requests.
"""

import json
import logging
import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class SessionData:
    """Holds extracted session information."""
    cookies: Dict[str, str] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)
    csrf_token: Optional[str] = None
    jsessionid: Optional[str] = None
    aws_cookies: Dict[str, str] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    store_code: Optional[str] = None

    def is_expired(self, max_age_minutes: int = 30) -> bool:
        """Check if session is older than max_age_minutes."""
        return datetime.now() - self.created_at > timedelta(minutes=max_age_minutes)

    def get_cookie_header(self) -> str:
        """Format cookies as a header string."""
        all_cookies = {**self.cookies, **self.aws_cookies}
        return "; ".join(f"{k}={v}" for k, v in all_cookies.items())

    def get_requests_cookies(self) -> Dict[str, str]:
        """Get cookies in format suitable for requests library."""
        return {**self.cookies, **self.aws_cookies}

    def to_dict(self) -> Dict:
        """Serialize session data to dictionary."""
        return {
            "cookies": self.cookies,
            "headers": self.headers,
            "csrf_token": self.csrf_token,
            "jsessionid": self.jsessionid,
            "aws_cookies": self.aws_cookies,
            "created_at": self.created_at.isoformat(),
            "store_code": self.store_code,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "SessionData":
        """Deserialize session data from dictionary."""
        session = cls()
        session.cookies = data.get("cookies", {})
        session.headers = data.get("headers", {})
        session.csrf_token = data.get("csrf_token")
        session.jsessionid = data.get("jsessionid")
        session.aws_cookies = data.get("aws_cookies", {})
        session.created_at = datetime.fromisoformat(data.get("created_at", datetime.now().isoformat()))
        session.store_code = data.get("store_code")
        return session


class PlaywrightSessionManager:
    """
    Manages browser sessions for Checkers and Shoprite.

    Uses Playwright to create authenticated sessions, then extracts
    cookies for use with the faster requests library.
    """

    SITE_CONFIGS = {
        "checkers": {
            "base_url": "https://products.checkers.co.za",
            "store_endpoint": "/store-finder/setPreferredStore",
            "product_page": "/c-2413/All-Departments/Food?q=%3Arelevance",
            "cookie_prefix": "checkersZA",
        },
        "shoprite": {
            "base_url": "https://www.shoprite.co.za",
            "store_endpoint": "/store-finder/setPreferredStore",
            "product_page": "/c-2256/All-Departments?q=%3Arelevance",
            "cookie_prefix": "shopriteZA",
        }
    }

    def __init__(
        self,
        headless: bool = True,
        slow_mo: int = 0,
        timeout: int = 60000,  # Increased to 60 seconds
        cache_dir: Optional[Path] = None,
    ):
        """
        Initialize the session manager.

        Args:
            headless: Run browser in headless mode
            slow_mo: Slow down operations by X milliseconds (for debugging)
            timeout: Default timeout for page operations (default 60s)
            cache_dir: Directory to cache session data
        """
        self.headless = headless
        self.slow_mo = slow_mo
        self.timeout = timeout
        self.cache_dir = cache_dir or Path("./data/sessions")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Cache sessions in memory
        self._session_cache: Dict[str, SessionData] = {}

    def _get_cache_path(self, site: str, store_code: str) -> Path:
        """Get the cache file path for a site/store combination."""
        return self.cache_dir / f"{site}_{store_code}_session.json"

    def _load_cached_session(self, site: str, store_code: str) -> Optional[SessionData]:
        """Load a cached session if it exists and is not expired."""
        cache_key = f"{site}_{store_code}"

        # Check memory cache first
        if cache_key in self._session_cache:
            session = self._session_cache[cache_key]
            if not session.is_expired():
                logger.info(f"Using memory-cached session for {site} store {store_code}")
                return session

        # Check file cache
        cache_path = self._get_cache_path(site, store_code)
        if cache_path.exists():
            try:
                with open(cache_path, "r") as f:
                    data = json.load(f)
                session = SessionData.from_dict(data)

                if not session.is_expired():
                    self._session_cache[cache_key] = session
                    logger.info(f"Using file-cached session for {site} store {store_code}")
                    return session
                else:
                    logger.info(f"Cached session expired for {site} store {store_code}")
            except Exception as e:
                logger.warning(f"Failed to load cached session: {e}")

        return None

    def _save_session(self, site: str, session: SessionData):
        """Save session to memory and file cache."""
        cache_key = f"{site}_{session.store_code}"
        self._session_cache[cache_key] = session

        cache_path = self._get_cache_path(site, session.store_code)
        try:
            with open(cache_path, "w") as f:
                json.dump(session.to_dict(), f)
            logger.info(f"Saved session to cache for {site} store {session.store_code}")
        except Exception as e:
            logger.warning(f"Failed to save session to cache: {e}")

    async def create_session_async(
        self,
        site: str,
        store_code: str,
        force_refresh: bool = False,
    ) -> SessionData:
        """
        Create a new session for the specified site and store.

        Args:
            site: "checkers" or "shoprite"
            store_code: The store code to set as preferred
            force_refresh: If True, ignore cached session

        Returns:
            SessionData with cookies and headers ready for use
        """
        if site not in self.SITE_CONFIGS:
            raise ValueError(f"Unknown site: {site}. Must be 'checkers' or 'shoprite'")

        # Check cache unless force refresh
        if not force_refresh:
            cached = self._load_cached_session(site, store_code)
            if cached:
                return cached

        config = self.SITE_CONFIGS[site]
        logger.info(f"Creating new session for {site} store {store_code}")

        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise ImportError(
                "Playwright is not installed. Install it with:\n"
                "  pip install playwright\n"
                "  playwright install chromium"
            )

        session = SessionData(store_code=store_code)

        async with async_playwright() as p:
            # Launch browser
            browser = await p.chromium.launch(
                headless=self.headless,
                slow_mo=self.slow_mo,
            )

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
            )

            page = await context.new_page()
            page.set_default_timeout(self.timeout)

            # Set up request interception to capture CSRF token from actual requests
            captured_csrf = [None]  # Use list to allow modification in closure

            async def capture_csrf(request):
                # Look for CSRF token in request headers
                headers = request.headers
                if 'csrftoken' in headers:
                    captured_csrf[0] = headers['csrftoken']
                    logger.debug(f"Captured CSRF from request header: {captured_csrf[0][:20]}...")

            page.on("request", capture_csrf)

            try:
                # Step 1: Navigate to the main page - use domcontentloaded instead of networkidle
                logger.info(f"Navigating to {config['base_url']}")
                await page.goto(config["base_url"], wait_until="domcontentloaded")

                # Wait for page to be interactive
                await page.wait_for_load_state("domcontentloaded")
                await page.wait_for_timeout(3000)  # Give JS time to initialize

                # Step 2: Set the preferred store
                store_url = f"{config['base_url']}{config['store_endpoint']}?preferredStoreName={store_code}"
                logger.info(f"Setting preferred store: {store_code}")

                # Make the store selection request
                response = await page.goto(store_url, wait_until="domcontentloaded")
                if response and response.ok:
                    logger.info(f"Store set successfully: {response.status}")
                else:
                    logger.warning(f"Store selection returned status: {response.status if response else 'No response'}")

                await page.wait_for_timeout(2000)

                # Step 3: Navigate to product page to trigger all necessary cookies
                product_url = f"{config['base_url']}{config['product_page']}"
                logger.info(f"Loading product page to complete session setup")
                await page.goto(product_url, wait_until="domcontentloaded")
                await page.wait_for_timeout(3000)

                # Step 4: Extract cookies
                cookies = await context.cookies()
                logger.info(f"Extracted {len(cookies)} cookies")

                for cookie in cookies:
                    name = cookie["name"]
                    value = cookie["value"]

                    if name == "JSESSIONID":
                        session.jsessionid = value
                        session.cookies[name] = value
                    elif name.startswith("AWS"):
                        session.aws_cookies[name] = value
                    elif name == f"{config['cookie_prefix']}-preferredStore":
                        session.cookies[name] = value
                    else:
                        session.cookies[name] = value

                # Step 5: Extract CSRF token from page content or meta tags
                try:
                    # Try to find CSRF token in various places - use raw string to avoid escape warnings
                    csrf_token = await page.evaluate(r"""
                        () => {
                            // Try meta tag
                            const meta = document.querySelector('meta[name="csrf-token"]');
                            if (meta) return meta.content;

                            // Try input field
                            const input = document.querySelector('input[name="_csrf"]');
                            if (input) return input.value;

                            // Try window object
                            if (window.CSRFToken) return window.CSRFToken;
                            if (window.csrfToken) return window.csrfToken;

                            // Try to find in any script tag - this is the key one for Checkers
                            const scripts = document.querySelectorAll('script');
                            for (const script of scripts) {
                                const text = script.textContent || '';
                                // Look for csrfToken or CSRFToken patterns
                                let match = text.match(/csrf[Tt]oken["']?\s*[:=]\s*["']([^"']+)["']/);
                                if (match) return match[1];
                                // Also try ACC.config.CSRFToken pattern used by Hybris
                                match = text.match(/CSRFToken\s*[:=]\s*["']([a-f0-9-]+)["']/i);
                                if (match) return match[1];
                            }

                            // Try data attribute
                            const csrfElem = document.querySelector('[data-csrf]');
                            if (csrfElem) return csrfElem.dataset.csrf;

                            // Search the entire HTML as last resort
                            const html = document.documentElement.innerHTML;
                            const htmlMatch = html.match(/CSRFToken["']?\s*[:=]\s*["']([a-f0-9-]+)["']/i);
                            if (htmlMatch) return htmlMatch[1];

                            return null;
                        }
                    """)
                    if csrf_token:
                        session.csrf_token = csrf_token
                        logger.info(f"Extracted CSRF token: {csrf_token[:20]}...")
                    else:
                        logger.warning("No CSRF token found in page")
                except Exception as e:
                    logger.debug(f"Could not extract CSRF token: {e}")

                # Step 6: Make a test API call from browser to discover working CSRF token
                if not session.csrf_token:
                    logger.info("Attempting to discover CSRF token via test API call...")
                    try:
                        # Get product JSON from page
                        product_json = await page.evaluate("""
                            () => {
                                const elem = document.querySelector('.productListJSON');
                                return elem ? elem.textContent : null;
                            }
                        """)

                        if product_json:
                            # Try API call without CSRF first to see if it works
                            api_test = await page.evaluate(r"""
                                async (jsonData) => {
                                    // First try without CSRF
                                    let response = await fetch('/populateProductsWithHeavyAttributes', {
                                        method: 'POST',
                                        headers: {
                                            'Content-Type': 'application/json',
                                            'X-Requested-With': 'XMLHttpRequest'
                                        },
                                        body: jsonData
                                    });

                                    if (response.ok) {
                                        return { success: true, csrf: null };
                                    }

                                    // Try to find CSRF in page and retry
                                    const html = document.documentElement.innerHTML;
                                    const match = html.match(/CSRFToken["']?\s*[:=]\s*["']([a-f0-9-]+)["']/i);
                                    const csrf = match ? match[1] : null;

                                    if (csrf) {
                                        response = await fetch('/populateProductsWithHeavyAttributes', {
                                            method: 'POST',
                                            headers: {
                                                'Content-Type': 'application/json',
                                                'X-Requested-With': 'XMLHttpRequest',
                                                'csrftoken': csrf
                                            },
                                            body: jsonData
                                        });

                                        if (response.ok) {
                                            return { success: true, csrf: csrf };
                                        }
                                    }

                                    return { success: false, csrf: csrf };
                                }
                            """, product_json)

                            if api_test.get('success') and api_test.get('csrf'):
                                session.csrf_token = api_test['csrf']
                                logger.info(f"Discovered working CSRF token: {session.csrf_token[:20]}...")
                            elif api_test.get('csrf'):
                                session.csrf_token = api_test['csrf']
                                logger.warning(f"Found CSRF token but API test failed: {session.csrf_token[:20]}...")
                    except Exception as e:
                        logger.debug(f"CSRF discovery via API failed: {e}")

                # Step 7: Use captured CSRF from network if still not found
                if not session.csrf_token and captured_csrf[0]:
                    session.csrf_token = captured_csrf[0]
                    logger.info(f"Using CSRF token captured from network: {session.csrf_token[:20]}...")

                # Step 8: Try to get CSRF from cookies as fallback
                if not session.csrf_token:
                    # Make a dummy request to see if we can capture the token
                    try:
                        # Look for csrftoken in cookies
                        for cookie in cookies:
                            if 'csrf' in cookie['name'].lower():
                                session.csrf_token = cookie['value']
                                logger.info(f"Found CSRF in cookie: {cookie['name']}")
                                break
                    except Exception:
                        pass

                # Step 7: Set up headers
                session.headers = {
                    "accept": "text/plain, */*; q=0.01",
                    "accept-language": "en-US,en;q=0.9",
                    "content-type": "application/json",
                    "origin": config['base_url'],
                    "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                    "x-requested-with": "XMLHttpRequest",
                    "referer": product_url,
                }

                if session.csrf_token:
                    session.headers["csrftoken"] = session.csrf_token

                logger.info(f"Session created successfully for {site} store {store_code}")
                logger.info(f"  JSESSIONID: {session.jsessionid[:20] if session.jsessionid else 'None'}...")
                logger.info(f"  CSRF Token: {session.csrf_token[:20] if session.csrf_token else 'None'}...")
                logger.info(f"  AWS Cookies: {len(session.aws_cookies)}")

            finally:
                await browser.close()

        # Cache the session
        self._save_session(site, session)

        return session

    def create_session(
        self,
        site: str,
        store_code: str,
        force_refresh: bool = False,
    ) -> SessionData:
        """
        Synchronous wrapper for create_session_async.

        Args:
            site: "checkers" or "shoprite"
            store_code: The store code to set as preferred
            force_refresh: If True, ignore cached session

        Returns:
            SessionData with cookies and headers ready for use
        """
        return asyncio.run(self.create_session_async(site, store_code, force_refresh))

    async def create_sessions_for_all_stores_async(
        self,
        site: str,
        store_codes: List[str],
        max_concurrent: int = 2,
    ) -> Dict[str, SessionData]:
        """
        Create sessions for multiple stores concurrently.

        Args:
            site: "checkers" or "shoprite"
            store_codes: List of store codes
            max_concurrent: Maximum concurrent browser sessions

        Returns:
            Dictionary mapping store_code to SessionData
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        results = {}

        async def create_with_semaphore(store_code: str):
            async with semaphore:
                try:
                    session = await self.create_session_async(site, store_code)
                    results[store_code] = session
                except Exception as e:
                    logger.error(f"Failed to create session for store {store_code}: {e}")
                    results[store_code] = None

        await asyncio.gather(*[create_with_semaphore(code) for code in store_codes])
        return results

    def create_sessions_for_all_stores(
        self,
        site: str,
        store_codes: List[str],
        max_concurrent: int = 2,
    ) -> Dict[str, SessionData]:
        """
        Synchronous wrapper for create_sessions_for_all_stores_async.
        """
        return asyncio.run(self.create_sessions_for_all_stores_async(site, store_codes, max_concurrent))

    def clear_cache(self, site: Optional[str] = None, store_code: Optional[str] = None):
        """
        Clear cached sessions.

        Args:
            site: If provided, only clear sessions for this site
            store_code: If provided with site, only clear this specific session
        """
        if site and store_code:
            # Clear specific session
            cache_key = f"{site}_{store_code}"
            self._session_cache.pop(cache_key, None)
            cache_path = self._get_cache_path(site, store_code)
            if cache_path.exists():
                cache_path.unlink()
        elif site:
            # Clear all sessions for a site
            keys_to_remove = [k for k in self._session_cache if k.startswith(f"{site}_")]
            for key in keys_to_remove:
                del self._session_cache[key]
            for path in self.cache_dir.glob(f"{site}_*_session.json"):
                path.unlink()
        else:
            # Clear all sessions
            self._session_cache.clear()
            for path in self.cache_dir.glob("*_session.json"):
                path.unlink()


# Convenience function for quick session creation
def get_session(site: str, store_code: str, **kwargs) -> SessionData:
    """
    Quick function to get a session for a site/store.

    Args:
        site: "checkers" or "shoprite"
        store_code: The store code
        **kwargs: Additional arguments for PlaywrightSessionManager

    Returns:
        SessionData ready for use with requests
    """
    manager = PlaywrightSessionManager(**kwargs)
    return manager.create_session(site, store_code)


# Example usage and testing
if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)

    # Test with Checkers
    site = sys.argv[1] if len(sys.argv) > 1 else "checkers"
    store = sys.argv[2] if len(sys.argv) > 2 else "7303"  # Gauteng store

    print(f"\nCreating session for {site} store {store}...")

    manager = PlaywrightSessionManager(headless=True)
    session = manager.create_session(site, store)

    print(f"\n✅ Session created successfully!")
    print(f"   JSESSIONID: {session.jsessionid[:20]}..." if session.jsessionid else "   No JSESSIONID")
    print(f"   CSRF Token: {session.csrf_token[:20]}..." if session.csrf_token else "   No CSRF token")
    print(f"   Cookies: {len(session.cookies)} standard, {len(session.aws_cookies)} AWS")
    print(f"\n   Cookie Header:\n   {session.get_cookie_header()[:100]}...")
