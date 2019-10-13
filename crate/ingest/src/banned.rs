use common::*;
use serde::Deserialize;
use url::Url;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub enum Banned {
    TLD(String),
    Host(String),
    NoScheme(String),
    Full(String),
}

impl Banned {
    pub fn matches(&self, url: &Url) -> bool {
        use Banned::*;
        match self {
            TLD(tld) => get_tld(url) == *tld,
            Host(host) => url
                .host_str()
                .map(|host_str| host_str == *host)
                .unwrap_or(false),
            NoScheme(no_scheme) => {
                url.as_str()
                    .trim_start_matches(url.scheme())
                    .trim_start_matches("://")
                    == *no_scheme
            }
            Full(link) => url.as_str() == *link,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn banned() {
        assert!(Banned::NoScheme("imgur.com/trtbLIL".to_string())
            .matches(&Url::parse("https://imgur.com/trtbLIL").unwrap()));
    }
}
