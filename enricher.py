"""
Data enrichment service for firmographic and contact data.
"""
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class Enricher:
    """Handles data enrichment for firms via the mock API."""

    def __init__(self, api_client, base_url: str):
        self.api = api_client
        self.base_url = base_url

    def fetch_firmographic(self, firm_id: str) -> Optional[Dict[str, Any]]:
        """Fetch firmographic data, normalizing lawyer_count -> num_lawyers."""
        try:
            response = self.api.get(f"{self.base_url}/firms/{firm_id}/firmographic")
            if response is None or response.status_code != 200:
                logger.warning("Firmographic fetch failed for %s (HTTP %s)",
                               firm_id, getattr(response, "status_code", "N/A"))
                return None
            data = response.json()
            if "lawyer_count" in data and "num_lawyers" not in data:
                data["num_lawyers"] = data.pop("lawyer_count")
            return data
        except Exception as e:
            logger.error("Error fetching firmographic for %s: %s", firm_id, e)
            return None

    def fetch_contact(self, firm_id: str) -> Optional[Dict[str, Any]]:
        """Fetch primary contact info for a firm."""
        try:
            response = self.api.get(f"{self.base_url}/firms/{firm_id}/contact")
            if response is None or response.status_code != 200:
                logger.warning("Contact fetch failed for %s (HTTP %s)",
                               firm_id, getattr(response, "status_code", "N/A"))
                return None
            return response.json()
        except Exception as e:
            logger.error("Error fetching contact for %s: %s", firm_id, e)
            return None
