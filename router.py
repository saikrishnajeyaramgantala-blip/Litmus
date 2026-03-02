"""
Lead routing system for qualified prospects.
"""
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class LeadRouter:
    """Routes leads into priority tiers based on ICP score."""

    def __init__(self, config: Dict[str, Any]):
        self.high_threshold = config.get("high_priority_threshold", 0.70)
        self.nurture_threshold = config.get("nurture_threshold", 0.40)

    def route(self, firm: Dict[str, Any], score: float) -> str:
        """
        Route a lead based on score and firm data.

        Returns:
            "high_priority", "nurture", or "disqualified"
        """
        if score >= self.high_threshold:
            return "high_priority"
        if score >= self.nurture_threshold:
            return "nurture"
        return "disqualified"
