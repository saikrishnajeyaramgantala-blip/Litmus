"""
Experiment assignment system for A/B testing.
"""
import hashlib
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class ExperimentAssigner:
    """Assigns leads to experiment variants using deterministic hashing."""

    def __init__(self, config: Dict[str, Any]):
        self.variants = list(config.get("email_variants", {}).keys())
        if not self.variants:
            self.variants = ["variant_a", "variant_b"]

    def assign_variant(self, lead_id: str) -> str:
        """Hash-based variant assignment -- same lead_id always gets the same variant."""
        hash_val = int(hashlib.sha256(lead_id.encode()).hexdigest(), 16)
        return self.variants[hash_val % len(self.variants)]
