use common::*;
use serde::Deserialize;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub enum Banned {
    HostEnd(String),
    Host(String),
    NoScheme(String),
    Full(String),
}

impl Banned {
    pub fn matches(&self, url: &str) -> bool {
        use Banned::*;
        match self {
            HostEnd(end) => host_ends_with(url, end),
            Host(host) => get_host(url)
                .map(|host_str| host_str == *host)
                .unwrap_or(false),
            NoScheme(no_scheme) => {
                url.split("://").nth(1).map(|u| u == *no_scheme).unwrap_or(false)
            }
            Full(link) => url == *link,
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
