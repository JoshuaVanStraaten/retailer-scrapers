"""
Discord Webhook Notifications for Savvy Grocery Scraper

Sends formatted messages to Discord for scraper status updates,
errors, and completion notifications.
"""

import json
import logging
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List
import requests

from config import config

logger = logging.getLogger(__name__)


class NotificationType(Enum):
    """Types of notifications with associated colors."""
    SUCCESS = 0x00FF00  # Green
    WARNING = 0xFFFF00  # Yellow
    ERROR = 0xFF0000    # Red
    INFO = 0x0099FF     # Blue
    STARTED = 0x9B59B6  # Purple


class DiscordNotifier:
    """Send notifications to Discord via webhooks."""

    def __init__(self, webhook_url: Optional[str] = None):
        """
        Initialize the Discord notifier.

        Args:
            webhook_url: Discord webhook URL. If not provided, uses config.
        """
        self.webhook_url = webhook_url or config.discord.webhook_url

        # Check if webhook URL is actually configured (not placeholder)
        is_placeholder = (
            not self.webhook_url or
            "your-webhook" in self.webhook_url.lower() or
            len(self.webhook_url) < 50  # Real Discord webhooks are long
        )

        self.enabled = config.discord.enabled and not is_placeholder

        if not self.enabled and config.discord.enabled:
            logger.debug("Discord notifications disabled - webhook not configured")

    def _send_webhook(self, payload: Dict[str, Any]) -> bool:
        """
        Send a payload to the Discord webhook.

        Args:
            payload: The JSON payload to send

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled:
            return False

        try:
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10
            )

            if response.status_code == 204:
                return True
            else:
                logger.error(f"Discord webhook failed with status {response.status_code}: {response.text}")
                return False

        except requests.RequestException as e:
            logger.error(f"Discord webhook request failed: {e}")
            return False

    def send_embed(
        self,
        title: str,
        description: str,
        notification_type: NotificationType = NotificationType.INFO,
        fields: Optional[List[Dict[str, Any]]] = None,
        footer: Optional[str] = None,
    ) -> bool:
        """
        Send a rich embed message to Discord.

        Args:
            title: Embed title
            description: Main message content
            notification_type: Type of notification (affects color)
            fields: Optional list of field dicts with 'name', 'value', 'inline' keys
            footer: Optional footer text

        Returns:
            True if sent successfully
        """
        embed = {
            "title": title,
            "description": description,
            "color": notification_type.value,
            "timestamp": datetime.utcnow().isoformat(),
        }

        if fields:
            embed["fields"] = fields

        if footer:
            embed["footer"] = {"text": footer}

        payload = {"embeds": [embed]}
        return self._send_webhook(payload)

    def scraper_started(self, retailer: str, provinces: List[str], total_pages: Optional[int] = None) -> bool:
        """
        Notify that a scraper has started.

        Args:
            retailer: Name of the retailer being scraped
            provinces: List of provinces to scrape
            total_pages: Estimated total pages to scrape
        """
        fields = [
            {"name": "🏪 Retailer", "value": retailer, "inline": True},
            {"name": "🗺️ Provinces", "value": str(len(provinces)), "inline": True},
        ]

        if total_pages:
            fields.append({"name": "📄 Est. Pages", "value": str(total_pages), "inline": True})

        return self.send_embed(
            title="🚀 Scraper Started",
            description=f"Started scraping **{retailer}** across {len(provinces)} provinces.",
            notification_type=NotificationType.STARTED,
            fields=fields,
            footer="Savvy Grocery Scraper"
        )

    def scraper_completed(
        self,
        retailer: str,
        products_scraped: int,
        duration_seconds: float,
        provinces_completed: int,
    ) -> bool:
        """
        Notify that a scraper has completed successfully.

        Args:
            retailer: Name of the retailer
            products_scraped: Total number of products scraped
            duration_seconds: Time taken in seconds
            provinces_completed: Number of provinces completed
        """
        duration_str = self._format_duration(duration_seconds)

        fields = [
            {"name": "🏪 Retailer", "value": retailer, "inline": True},
            {"name": "📦 Products", "value": f"{products_scraped:,}", "inline": True},
            {"name": "⏱️ Duration", "value": duration_str, "inline": True},
            {"name": "🗺️ Provinces", "value": str(provinces_completed), "inline": True},
            {"name": "📊 Rate", "value": f"{products_scraped / max(duration_seconds, 1):.1f}/sec", "inline": True},
        ]

        return self.send_embed(
            title="✅ Scraper Completed",
            description=f"Successfully scraped **{retailer}**!",
            notification_type=NotificationType.SUCCESS,
            fields=fields,
            footer="Savvy Grocery Scraper"
        )

    def scraper_error(
        self,
        retailer: str,
        error_message: str,
        page: Optional[int] = None,
        province: Optional[str] = None,
        recoverable: bool = True,
    ) -> bool:
        """
        Notify of a scraper error.

        Args:
            retailer: Name of the retailer
            error_message: Description of the error
            page: Page number where error occurred
            province: Province being scraped when error occurred
            recoverable: Whether the scraper can continue
        """
        fields = [
            {"name": "🏪 Retailer", "value": retailer, "inline": True},
        ]

        if province:
            fields.append({"name": "🗺️ Province", "value": province, "inline": True})
        if page is not None:
            fields.append({"name": "📄 Page", "value": str(page), "inline": True})

        fields.append({
            "name": "🔄 Status",
            "value": "Continuing..." if recoverable else "**STOPPED**",
            "inline": True
        })

        # Truncate error message if too long
        if len(error_message) > 1000:
            error_message = error_message[:997] + "..."

        fields.append({"name": "❌ Error", "value": f"```{error_message}```", "inline": False})

        return self.send_embed(
            title="⚠️ Scraper Error" if recoverable else "🛑 Scraper Failed",
            description=f"Error occurred while scraping **{retailer}**",
            notification_type=NotificationType.WARNING if recoverable else NotificationType.ERROR,
            fields=fields,
            footer="Savvy Grocery Scraper"
        )

    def session_refresh_needed(self, retailer: str, details: Optional[str] = None) -> bool:
        """
        Notify that session/cookies need to be refreshed.

        Args:
            retailer: Name of the retailer
            details: Additional details about the issue
        """
        description = f"Session expired for **{retailer}**. Attempting automatic refresh..."

        if details:
            description += f"\n\n{details}"

        return self.send_embed(
            title="🔑 Session Refresh Required",
            description=description,
            notification_type=NotificationType.WARNING,
            footer="Savvy Grocery Scraper"
        )

    def daily_summary(
        self,
        results: List[Dict[str, Any]],
        total_duration_seconds: float,
    ) -> bool:
        """
        Send a daily summary of all scraping operations.

        Args:
            results: List of dicts with 'retailer', 'products', 'success', 'error' keys
            total_duration_seconds: Total time for all scrapers
        """
        total_products = sum(r.get("products", 0) for r in results)
        successful = sum(1 for r in results if r.get("success", False))
        failed = len(results) - successful

        description_lines = [
            f"**Total Products:** {total_products:,}",
            f"**Duration:** {self._format_duration(total_duration_seconds)}",
            f"**Successful:** {successful}/{len(results)} retailers",
        ]

        if failed > 0:
            description_lines.append(f"**Failed:** {failed} retailers")

        fields = []
        for result in results:
            status = "✅" if result.get("success") else "❌"
            value = f"{result.get('products', 0):,} products"
            if result.get("error"):
                value += f"\n_{result['error'][:50]}..._" if len(result.get("error", "")) > 50 else f"\n_{result.get('error')}_"

            fields.append({
                "name": f"{status} {result['retailer']}",
                "value": value,
                "inline": True
            })

        notification_type = NotificationType.SUCCESS if failed == 0 else NotificationType.WARNING

        return self.send_embed(
            title="📊 Daily Scraping Summary",
            description="\n".join(description_lines),
            notification_type=notification_type,
            fields=fields,
            footer=f"Savvy Grocery Scraper • {datetime.now().strftime('%Y-%m-%d')}"
        )

    def _format_duration(self, seconds: float) -> str:
        """Format duration in human-readable format."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"


# Global notifier instance
notifier = DiscordNotifier()


# Convenience functions for quick notifications
def notify_started(retailer: str, provinces: List[str], total_pages: Optional[int] = None) -> bool:
    """Quick function to notify scraper started."""
    return notifier.scraper_started(retailer, provinces, total_pages)


def notify_completed(retailer: str, products: int, duration: float, provinces: int) -> bool:
    """Quick function to notify scraper completed."""
    return notifier.scraper_completed(retailer, products, duration, provinces)


def notify_error(retailer: str, error: str, page: Optional[int] = None, province: Optional[str] = None) -> bool:
    """Quick function to notify of an error."""
    return notifier.scraper_error(retailer, error, page, province)


def notify_summary(results: List[Dict[str, Any]], duration: float) -> bool:
    """Quick function to send daily summary."""
    return notifier.daily_summary(results, duration)
