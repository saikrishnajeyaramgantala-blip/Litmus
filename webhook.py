"""
Webhook client for firing events to downstream systems.
"""
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class WebhookClient:
    """Handles webhook delivery to CRM and email platform endpoints."""

    def __init__(self, config: Dict[str, Any], api_client):
        self.crm_endpoint = config.get("crm_endpoint", "")
        self.email_endpoint = config.get("email_endpoint", "")
        self.api = api_client

    def fire(self, payload: Dict[str, Any]) -> bool:
        """Fire webhook to both CRM and email endpoints."""
        crm_ok = self.fire_crm(payload)
        email_ok = self.fire_email(payload)
        return crm_ok and email_ok

    def fire_crm(self, payload: Dict[str, Any]) -> bool:
        """Deliver lead data to CRM system."""
        return self._post(self.crm_endpoint, payload, "CRM")

    def fire_email(self, payload: Dict[str, Any]) -> bool:
        """Deliver campaign data to email platform."""
        return self._post(self.email_endpoint, payload, "email")

    def _post(self, url: str, payload: Dict[str, Any], label: str) -> bool:
        try:
            response = self.api.post(url, json=payload)
            if response is not None and response.status_code == 200:
                logger.debug("Webhook %s delivered successfully", label)
                return True
            logger.warning("Webhook %s failed: HTTP %s", label,
                           getattr(response, "status_code", "N/A"))
            return False
        except Exception as e:
            logger.error("Webhook %s error: %s", label, e)
            return False
