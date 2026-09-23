# Ranking and auction

Not a reproduction of any proprietary ad exchange.

## Pipeline

1. Normalize and tokenize the query (`running shoes` → `running`, `shoes`).
2. Retrieve candidates from the inverted index with TF-IDF cosine similarity (relevance in `[0, 1]`).
3. Drop ineligible campaigns: not `active`, daily budget exhausted, geo mismatch.
4. Privacy: if a `cohort_id` is present, require `member_count >= k` (default 50). Below `k`, ads are still eligible as **contextual** (query keywords only); no individual-level targeting.
5. Score: `score = relevance * bid_micros`.
6. Second-price on score: winner is `argmax(score)`. Price in bid space is `ceil(second_score / winner_relevance)`. Single-candidate auctions charge `0.8 * bid`.

## Example (rupees stored as micros)

| Campaign | Relevance | Bid | Score |
|---|---|---|---|
| A | 0.91 | ₹4 | 3.64e6 |
| B | 0.84 | ₹6 | 5.04e6 |
| C | 0.77 | ₹5 | 3.85e6 |

Winner is **B**. Charge is `ceil(3.85e6 / 0.84)` micros, which is below B's bid.

Unit test: `services/serving/src/auction.rs`.
