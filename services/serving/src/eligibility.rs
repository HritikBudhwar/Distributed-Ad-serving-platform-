pub fn eligible(
    status: &str,
    remaining_budget_micros: i64,
    geo_targets: &[String],
    request_geo: Option<&str>,
) -> Result<(), &'static str> {
    if status != "active" {
        return Err("inactive");
    }
    if remaining_budget_micros <= 0 {
        return Err("budget_exhausted");
    }
    if let Some(geo) = request_geo {
        if !geo_targets.is_empty() && !geo_targets.iter().any(|g| g == geo) {
            return Err("geo_mismatch");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geo_and_budget() {
        assert!(eligible("active", 10, &["IN".into()], Some("IN")).is_ok());
        assert_eq!(
            eligible("paused", 10, &["IN".into()], Some("IN")).unwrap_err(),
            "inactive"
        );
        assert_eq!(
            eligible("active", 0, &["IN".into()], Some("IN")).unwrap_err(),
            "budget_exhausted"
        );
        assert_eq!(
            eligible("active", 10, &["US".into()], Some("IN")).unwrap_err(),
            "geo_mismatch"
        );
    }
}
