"""
ICP scoring system for evaluating firm fit.
"""
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class ICPScorer:
    """Scores firms against ideal customer profile criteria."""

    def __init__(self, config: Dict[str, Any]):
        self.size_cfg = config.get("firm_size", {})
        self.practice_cfg = config.get("practice_areas", {})
        self.geo_cfg = config.get("geography", {})

        self.size_weight = self.size_cfg.get("weight", 0.40)
        self.practice_weight = self.practice_cfg.get("weight", 0.35)
        self.geo_weight = self.geo_cfg.get("weight", 0.25)

    def score(self, firm: Dict[str, Any]) -> float:
        """Weighted ICP score across size, practice areas, and geography. Returns 0.0-1.0."""
        size_score = self._score_firm_size(firm)
        practice_score = self._score_practice_areas(firm)
        geo_score = self._score_geography(firm)

        total = (
            self.size_weight * size_score
            + self.practice_weight * practice_score
            + self.geo_weight * geo_score
        )
        return round(min(max(total, 0.0), 1.0), 4)

    def _score_firm_size(self, firm: Dict[str, Any]) -> float:
        num_lawyers = firm.get("num_lawyers")
        if num_lawyers is None:
            return 0.0

        min_size = self.size_cfg.get("min_lawyers", 50)
        max_size = self.size_cfg.get("max_lawyers", 500)

        if min_size <= num_lawyers <= max_size:
            return 1.0
        if num_lawyers < min_size:
            return max(num_lawyers / min_size, 0.0)
        return max(max_size / num_lawyers, 0.0)

    def _score_practice_areas(self, firm: Dict[str, Any]) -> float:
        """Fraction of a firm's practice areas that are in the preferred set."""
        firm_areas = firm.get("practice_areas", [])
        preferred = set(self.practice_cfg.get("preferred", []))
        if not firm_areas or not preferred:
            return 0.0
        matches = len(set(firm_areas) & preferred)
        return matches / len(firm_areas)

    def _score_geography(self, firm: Dict[str, Any]) -> float:
        country = firm.get("country", "")
        preferred = self.geo_cfg.get("preferred_regions", [])
        if not preferred:
            return 0.0
        return 1.0 if country in preferred else 0.0
