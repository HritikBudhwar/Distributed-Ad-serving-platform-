from __future__ import annotations

import math
from collections import defaultdict

from adpulse_common import tokenize


class InvertedIndex:
    """In-memory inverted index with TF-IDF scoring and event-id idempotency."""

    def __init__(self) -> None:
        self.postings: dict[str, dict[str, int]] = defaultdict(dict)
        self.docs: dict[str, dict] = {}
        self.doc_len: dict[str, int] = {}
        self.seen_events: set[str] = set()
        self.version = 0

    def apply_event(self, event: dict) -> bool:
        event_id = event.get("event_id")
        if not event_id:
            return False
        if event_id in self.seen_events:
            return False
        self.seen_events.add(event_id)
        if len(self.seen_events) > 200_000:
            # bound memory; oldest ids are not tracked after trim
            self.seen_events = set(list(self.seen_events)[-100_000:])
        campaign = event.get("campaign") or {}
        cid = campaign.get("id")
        if not cid:
            return False
        if campaign.get("status") != "active":
            self.remove(cid)
        else:
            self.upsert(campaign)
        self.version += 1
        return True

    def remove(self, campaign_id: str) -> None:
        old = self.docs.pop(campaign_id, None)
        if not old:
            return
        tokens = tokenize(" ".join(old.get("keywords", []) + [old.get("headline", ""), old.get("name", "")]))
        for t in set(tokens):
            self.postings[t].pop(campaign_id, None)
            if not self.postings[t]:
                del self.postings[t]
        self.doc_len.pop(campaign_id, None)

    def upsert(self, campaign: dict) -> None:
        cid = campaign["id"]
        self.remove(cid)
        tokens = tokenize(
            " ".join(campaign.get("keywords", []) + [campaign.get("headline", ""), campaign.get("name", "")])
        )
        tf: dict[str, int] = defaultdict(int)
        for t in tokens:
            tf[t] += 1
        for t, c in tf.items():
            self.postings[t][cid] = c
        self.doc_len[cid] = max(len(tokens), 1)
        self.docs[cid] = campaign

    def search(self, query: str, k: int = 50) -> list[tuple[str, float]]:
        q_tokens = tokenize(query)
        if not q_tokens:
            return []
        n = max(len(self.docs), 1)
        scores: dict[str, float] = defaultdict(float)
        q_tf: dict[str, int] = defaultdict(int)
        for t in q_tokens:
            q_tf[t] += 1
        q_norm = 0.0
        for t, qf in q_tf.items():
            df = len(self.postings.get(t, {}))
            idf = math.log((n + 1) / (df + 1)) + 1.0
            q_w = (qf / len(q_tokens)) * idf
            q_norm += q_w * q_w
            for cid, tf in self.postings.get(t, {}).items():
                d_w = (tf / self.doc_len[cid]) * idf
                scores[cid] += q_w * d_w
        q_norm = math.sqrt(q_norm) or 1.0
        ranked = []
        for cid, dot in scores.items():
            d_norm = 0.0
            campaign = self.docs[cid]
            d_tokens = tokenize(
                " ".join(campaign.get("keywords", []) + [campaign.get("headline", ""), campaign.get("name", "")])
            )
            d_tf: dict[str, int] = defaultdict(int)
            for t in d_tokens:
                d_tf[t] += 1
            for t, tf in d_tf.items():
                df = len(self.postings.get(t, {}))
                idf = math.log((n + 1) / (df + 1)) + 1.0
                w = (tf / self.doc_len[cid]) * idf
                d_norm += w * w
            d_norm = math.sqrt(d_norm) or 1.0
            ranked.append((cid, dot / (q_norm * d_norm)))
        ranked.sort(key=lambda x: x[1], reverse=True)
        return ranked[:k]
