//! Relevance-weighted second-price auction.
//!
//! score_i = relevance_i * bid_i
//! winner = argmax(score)
//! charged_bid = second_score / winner_relevance   (falls back to 80% of bid)
//!
//! This is a teaching model, not a reproduction of any proprietary ad auction.

#[derive(Clone, Debug)]
pub struct AuctionCandidate {
    pub campaign_id: String,
    pub headline: String,
    pub landing_url: String,
    pub relevance: f64,
    pub bid_micros: i64,
}

#[derive(Clone, Debug)]
pub struct AuctionResult {
    pub winner: AuctionCandidate,
    pub charged_micros: i64,
    pub score: f64,
    pub second_score: Option<f64>,
}

pub fn score(relevance: f64, bid_micros: i64) -> f64 {
    relevance.max(0.0) * bid_micros.max(0) as f64
}

pub fn run_auction(mut cands: Vec<AuctionCandidate>) -> Option<AuctionResult> {
    cands.retain(|c| c.relevance > 0.0 && c.bid_micros > 0);
    if cands.is_empty() {
        return None;
    }
    cands.sort_by(|a, b| {
        score(b.relevance, b.bid_micros)
            .partial_cmp(&score(a.relevance, a.bid_micros))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let winner = cands[0].clone();
    let win_score = score(winner.relevance, winner.bid_micros);
    let second_score = cands.get(1).map(|c| score(c.relevance, c.bid_micros));
    let charged = match second_score {
        Some(s) if winner.relevance > 0.0 => (s / winner.relevance).ceil() as i64,
        _ => ((winner.bid_micros as f64) * 0.8).ceil() as i64,
    };
    Some(AuctionResult {
        winner,
        charged_micros: charged.max(1),
        score: win_score,
        second_score,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(id: &str, rel: f64, bid: i64) -> AuctionCandidate {
        AuctionCandidate {
            campaign_id: id.into(),
            headline: id.into(),
            landing_url: "https://example.com".into(),
            relevance: rel,
            bid_micros: bid,
        }
    }

    #[test]
    fn running_shoes_example() {
        // A 0.91 x 4, B 0.84 x 6, C 0.77 x 5  (bids in rupees * 1e6)
        let result = run_auction(vec![
            c("A", 0.91, 4_000_000),
            c("B", 0.84, 6_000_000),
            c("C", 0.77, 5_000_000),
        ])
        .unwrap();
        // B has highest score: 0.84 * 6e6 = 5.04e6 vs A 3.64e6 vs C 3.85e6
        assert_eq!(result.winner.campaign_id, "B");
        let expected_second = score(0.77, 5_000_000);
        let expected_charge = (expected_second / 0.84).ceil() as i64;
        assert_eq!(result.charged_micros, expected_charge);
        assert!(result.charged_micros <= 6_000_000);
    }

    #[test]
    fn empty_none() {
        assert!(run_auction(vec![]).is_none());
    }

    #[test]
    fn single_pays_reserve_fraction() {
        let r = run_auction(vec![c("A", 0.9, 1_000_000)]).unwrap();
        assert_eq!(r.charged_micros, 800_000);
    }
}
