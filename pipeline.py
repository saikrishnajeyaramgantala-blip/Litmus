"""
Main pipeline orchestrator for GTM data processing.
"""
import json
import logging
import time
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional

import httpx
import yaml

from enricher import Enricher
from scorer import ICPScorer
from router import LeadRouter
from experiment import ExperimentAssigner
from webhook import WebhookClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rate-limited HTTP client with retry and exponential backoff
# ---------------------------------------------------------------------------

class APIClient:
    """
    HTTP client that self-throttles to stay below the server's rate limit
    and retries on transient failures (429, 5xx, timeouts).
    """

    def __init__(self, timeout: int = 30, max_retries: int = 3, rate_limit: int = 18):
        self.client = httpx.Client(timeout=timeout)
        self.max_retries = max_retries
        self._timestamps: list[float] = []
        self._rate_limit = rate_limit
        self._window = 60

    def _wait_for_rate_limit(self):
        """Sleep if the sliding-window request count is at the limit."""
        now = time.time()
        self._timestamps = [t for t in self._timestamps if t > now - self._window]
        if len(self._timestamps) >= self._rate_limit:
            sleep_until = self._timestamps[0] + self._window
            sleep_time = sleep_until - now + 0.5
            if sleep_time > 0:
                logger.info("Rate-limit throttle: sleeping %.1fs", sleep_time)
                time.sleep(sleep_time)
                self._timestamps = [
                    t for t in self._timestamps if t > time.time() - self._window
                ]

    def request(self, method: str, url: str, **kwargs) -> Optional[httpx.Response]:
        last_response = None
        for attempt in range(self.max_retries + 1):
            self._wait_for_rate_limit()
            try:
                self._timestamps.append(time.time())
                response = self.client.request(method, url, **kwargs)
                last_response = response

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 10))
                    logger.info("429 on %s, waiting %ds", url, retry_after)
                    time.sleep(retry_after + 1)
                    continue

                if response.status_code >= 500 and attempt < self.max_retries:
                    wait = min(2 ** attempt, 30)
                    logger.info(
                        "%d on %s, retry %d/%d in %ds",
                        response.status_code, url, attempt + 1, self.max_retries, wait,
                    )
                    time.sleep(wait)
                    continue

                return response

            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                if attempt < self.max_retries:
                    wait = min(2 ** attempt, 30)
                    logger.warning(
                        "Connection error on %s: %s, retry %d/%d in %ds",
                        url, exc, attempt + 1, self.max_retries, wait,
                    )
                    time.sleep(wait)
                    continue
                raise

        return last_response

    def get(self, url: str, **kwargs) -> Optional[httpx.Response]:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> Optional[httpx.Response]:
        return self.request("POST", url, **kwargs)

    def close(self):
        self.client.close()


# ---------------------------------------------------------------------------
# Firm ingestion helpers
# ---------------------------------------------------------------------------

def fetch_all_firms(api: APIClient, base_url: str) -> List[Dict[str, Any]]:
    """Paginate through /firms and collect every firm record."""
    firms: list[Dict[str, Any]] = []
    page = 1
    while True:
        response = api.get(f"{base_url}/firms", params={"page": page, "per_page": 50})
        if response is None or response.status_code != 200:
            logger.error("Failed to fetch firms page %d (HTTP %s)",
                         page, getattr(response, "status_code", "N/A"))
            break
        data = response.json()
        firms.extend(data["items"])
        logger.info("Fetched page %d/%d (%d firms)", page, data["total_pages"], len(data["items"]))
        if page >= data["total_pages"]:
            break
        page += 1
    return firms


def deduplicate_firms(firms: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove duplicate firms.

    Primary key: domain (exact match).
    Secondary: name similarity > 0.85 via SequenceMatcher, as a safety net.
    """
    seen_domains: Dict[str, str] = {}
    unique: list[Dict[str, Any]] = []
    removed = 0

    for firm in firms:
        domain = firm["domain"].lower().strip()

        if domain in seen_domains:
            logger.info("Dedup (domain): '%s' duplicate of '%s'",
                        firm["name"], seen_domains[domain])
            removed += 1
            continue

        is_dup = False
        for existing in unique:
            ratio = SequenceMatcher(None, firm["name"].lower(), existing["name"].lower()).ratio()
            if ratio > 0.85:
                logger.info("Dedup (name %.2f): '%s' duplicate of '%s'",
                            ratio, firm["name"], existing["name"])
                is_dup = True
                removed += 1
                break

        if not is_dup:
            seen_domains[domain] = firm["name"]
            unique.append(firm)

    logger.info("Deduplication: %d -> %d firms (%d removed)", len(firms), len(unique), removed)
    return unique


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(config_path: str) -> Any:
    """
    Run the complete GTM data pipeline: fetch firms, deduplicate,
    enrich, score, route, assign experiments, and fire webhooks.
    """
    with open(config_path) as f:
        config = yaml.safe_load(f)

    api_cfg = config["apis"]["enrichment"]
    wh_cfg = config["apis"]["webhooks"]
    base_url = api_cfg["base_url"]

    api = APIClient(
        timeout=api_cfg["timeout"],
        max_retries=api_cfg["max_retries"],
        rate_limit=api_cfg["rate_limit"] - 2,
    )

    enricher = Enricher(api, base_url)
    scorer = ICPScorer(config["icp_criteria"])
    router = LeadRouter(config.get("routing", {}))
    assigner = ExperimentAssigner(config["experiments"])
    webhook = WebhookClient(wh_cfg, api)

    results: Dict[str, Any] = {
        "total_fetched": 0,
        "unique_firms": 0,
        "enriched": 0,
        "routes": {"high_priority": 0, "nurture": 0, "disqualified": 0},
        "experiments": {"variant_a": 0, "variant_b": 0},
        "webhooks": {"crm_sent": 0, "crm_failed": 0, "email_sent": 0, "email_failed": 0},
        "leads": [],
    }

    try:
        logger.info("=" * 60)
        logger.info("Step 1: Fetching firms")
        logger.info("=" * 60)
        firms = fetch_all_firms(api, base_url)
        results["total_fetched"] = len(firms)

        logger.info("=" * 60)
        logger.info("Step 2: Deduplicating")
        logger.info("=" * 60)
        unique_firms = deduplicate_firms(firms)
        results["unique_firms"] = len(unique_firms)

        logger.info("=" * 60)
        logger.info("Step 3: Enrich, score, route, experiment, webhook")
        logger.info("=" * 60)

        for idx, firm in enumerate(unique_firms, 1):
            firm_id = firm["id"]
            logger.info("[%d/%d] %s (%s)", idx, len(unique_firms), firm["name"], firm_id)

            # Enrich
            firmographic = enricher.fetch_firmographic(firm_id)
            contact = enricher.fetch_contact(firm_id)

            enriched = {**firm}
            if firmographic:
                enriched.update(firmographic)
                results["enriched"] += 1
            else:
                logger.warning("  No firmographic data, scoring with limited info")

            if contact:
                enriched["contact"] = contact

            # Score
            score = scorer.score(enriched)
            enriched["icp_score"] = score

            # Route
            route = router.route(enriched, score)
            enriched["route"] = route
            results["routes"][route] += 1
            logger.info("  score=%.4f  route=%s", score, route)

            lead_record = {
                "firm_id": firm_id,
                "name": enriched.get("name"),
                "domain": enriched.get("domain"),
                "score": score,
                "route": route,
                "variant": None,
            }

            # Experiment + webhooks for non-disqualified leads
            if route != "disqualified":
                variant = assigner.assign_variant(firm_id)
                enriched["experiment_variant"] = variant
                lead_record["variant"] = variant
                results["experiments"][variant] += 1

                variant_cfg = config["experiments"]["email_variants"].get(variant, {})

                crm_payload = {
                    "firm_id": firm_id,
                    "firm_name": enriched.get("name"),
                    "domain": enriched.get("domain"),
                    "icp_score": score,
                    "route": route,
                    "contact": enriched.get("contact"),
                    "experiment_variant": variant,
                }
                if webhook.fire_crm(crm_payload):
                    results["webhooks"]["crm_sent"] += 1
                else:
                    results["webhooks"]["crm_failed"] += 1

                email_payload = {
                    "firm_id": firm_id,
                    "firm_name": enriched.get("name"),
                    "contact": enriched.get("contact"),
                    "variant": variant,
                    "subject": variant_cfg.get("subject", ""),
                }
                if webhook.fire_email(email_payload):
                    results["webhooks"]["email_sent"] += 1
                else:
                    results["webhooks"]["email_failed"] += 1

            results["leads"].append(lead_record)

        logger.info("=" * 60)
        logger.info("Pipeline complete")
        logger.info("=" * 60)
        logger.info("Fetched:      %d firms", results["total_fetched"])
        logger.info("Unique:       %d firms", results["unique_firms"])
        logger.info("Enriched:     %d firms", results["enriched"])
        logger.info("Routes:       %s", results["routes"])
        logger.info("Experiments:  %s", results["experiments"])
        logger.info("Webhooks:     %s", results["webhooks"])

        with open("pipeline_results.json", "w") as out:
            json.dump(results, out, indent=2)
        logger.info("Results written to pipeline_results.json")

    finally:
        api.close()

    return results


if __name__ == "__main__":
    run_pipeline("config.yaml")
