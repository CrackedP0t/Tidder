use super::*;
use serde::Deserialize;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub enum Banned {
    HostEnd(String),
    Host(String),
    AnyScheme(String),
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
            AnyScheme(no_scheme) => url
                .find("://")
                .map(|loc| url.split_at(loc + 3).1 == *no_scheme)
                .unwrap_or(false),
            Full(link) => url == *link,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn any_scheme() {
        assert!(
            Banned::AnyScheme("imgur.com/trtbLIL".to_string()).matches("https://imgur.com/trtbLIL")
        );

        assert!(Banned::AnyScheme("site.com/x=http://asdf.com".to_string())
            .matches("http://site.com/x=http://asdf.com"));
    }

    #[test]
    fn host() {
        assert!(Banned::Host("bad.com".to_string()).matches("https://bad.com/asdf"));
    }

    #[test]
    fn host_end() {
        assert!(Banned::HostEnd("sub.bad.com".to_string()).matches("https://a.sub.bad.com/asdf"));
    }
}
