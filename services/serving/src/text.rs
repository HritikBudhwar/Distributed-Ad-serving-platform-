use regex::Regex;
use once_cell::sync::Lazy;

static TOKEN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[a-z0-9]+").unwrap());

const STOP: &[&str] = &[
    "a", "an", "the", "for", "and", "or", "of", "to", "in", "on", "with",
];

pub fn normalize(text: &str) -> String {
    text.to_lowercase().trim().to_string()
}

pub fn tokenize(text: &str) -> Vec<String> {
    TOKEN_RE
        .find_iter(&normalize(text))
        .map(|m| m.as_str().to_string())
        .filter(|t| t.len() > 1 && !STOP.contains(&t.as_str()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drops_stopwords() {
        assert_eq!(tokenize("The running shoes"), vec!["running", "shoes"]);
    }
}
