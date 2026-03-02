# WRITEUP

## Approach

I went with a straightforward sequential pipeline -- each firm goes through ingest, dedup, enrich, score/route, and then webhook delivery. I split each stage into its own module so they're easy to reason about individually.

The biggest challenge was the rate limit. The server caps at 20 requests/minute across all endpoints, and since every firm needs at least 4 API calls (firmographic, contact, CRM webhook, email webhook), I had to be careful about pacing.

### Rate Limiting & Retry

I built an `APIClient` class in `pipeline.py` that handles all HTTP requests. Instead of just reacting to 429s, it tracks its own request timestamps in a sliding window and sleeps proactively when it's about to hit the limit. I set the threshold at `rate_limit - 2` to leave a small buffer for timing edge cases.

When a 429 does come through (timing drift, etc.), it reads the `Retry-After` header and backs off. For 500 errors I used exponential backoff (`2^attempt`, capped at 30s). All of this lives in `APIClient.request()` so the enricher and webhook client don't need to worry about retries themselves.

### Deduplication

Looking at the data, I noticed five near-duplicates -- same domain but slightly different names (e.g. "Baker & Sterling LLP" vs "Baker Sterling LLP"). I dedup primarily on domain since that's the strongest signal. I also added a secondary check using `SequenceMatcher` (threshold 0.85) as a safety net in case duplicates show up under different domains. First one in wins, later dupes get logged and dropped. This took us from 55 firms down to 50.

### Enrichment

The enricher in `enricher.py` makes two calls per firm -- firmographic and contact. The main thing I had to handle was the schema inconsistency where ~25% of responses return `lawyer_count` instead of `num_lawyers`. I normalize that in the enricher so the scorer doesn't have to care about it.

For missing fields (~20% of firmographic responses drop things like `practice_areas` or `region`), the pipeline just keeps going with whatever data came back. The scorer gives 0.0 for anything it can't measure.

### ICP Scoring

The scorer in `scorer.py` uses three weighted criteria, all configurable in `config.yaml`:

| Criterion | Weight | How it works |
|-----------|--------|--------------|
| Firm size | 0.40 | 1.0 if lawyers in [50, 500], linear decay outside that range |
| Practice areas | 0.35 | What fraction of the firm's areas overlap with our preferred set |
| Geography | 0.25 | Binary -- 1.0 if they're in a target country, 0.0 otherwise |

I used linear decay for size instead of a hard cutoff because I didn't want a firm with 48 lawyers to score 0 when they're basically in range. For practice areas I went with `matches / len(firm_areas)` which rewards firms that are focused on our target areas rather than ones that do a bit of everything.

### Routing

Pretty simple -- three buckets based on configurable thresholds in `config.yaml`:
- **High priority** (>= 0.70): sales should reach out
- **Nurture** (>= 0.40): drip campaign
- **Disqualified** (< 0.40): skip

### Experiment Assignment

I used SHA-256 hashing on the `firm_id` so assignment is deterministic -- same firm always gets the same variant, even if you rerun the pipeline. This is important for A/B test validity. The hash mod 2 gives a pretty natural ~50/50 split (ended up 24/26 on 50 firms).

### Webhooks

The webhook client in `webhook.py` fires payloads to CRM and email endpoints. It goes through the same `APIClient`, so it gets retry and rate-limiting for free. Only non-disqualified leads get webhooks. CRM gets the full lead context (score, route, contact, variant), email gets the campaign subject line from the experiment config.

## Trade-offs

1. **Sequential over concurrent.** I considered async but with a 20 req/min rate limit, parallelism doesn't actually help -- you'd just hit the cap faster and wait longer. Sequential is simpler to debug and the ~12 minute runtime is fine for a batch pipeline.

2. **Conservative rate-limit buffer.** The `rate_limit - 2` buffer wastes a couple slots per window, but it avoids edge cases where network latency or clock skew triggers unexpected 429s. In production I'd monitor the actual 429 rate and tune it.

3. **Stateless pipeline.** Everything gets refetched on every run. In production I'd add a database to track what's already been processed so we're not re-enriching firms we already have data for.

4. **Practice area scoring favors specialists.** A firm doing only Corporate Law and Litigation (both preferred) scores 1.0, while a full-service firm with the same two preferred areas plus three others scores 0.4. This is intentional for targeting midsize focused firms, but the weights are easy to adjust if the GTM strategy changes.

## What I'd do with more time

- Switch to `httpx.AsyncClient` with a semaphore to pipeline requests more efficiently within the rate limit
- Add SQLite or Redis for dedup state across runs so we don't re-enrich known firms
- Build a proper webhook delivery queue with dead-letter handling
- Make scoring functions pluggable via config (step function, linear decay, etc.)
- Add structured JSON logging and metrics for observability
- Write integration tests that spin up the mock server and validate end-to-end outputs
