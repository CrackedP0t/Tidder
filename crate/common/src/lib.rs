use cache_control::CacheControl;
use chrono::{DateTime, NaiveDateTime};
use deadpool_postgres::Pool;
pub use failure::{self, format_err, Error};
use futures::prelude::*;
use log::LevelFilter;
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::header::{self, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::string::ToString;
use std::time::Duration;

// Get around https://github.com/rust-lang/rust/issues/64960
#[macro_export]
macro_rules! format {
    ($($arg:tt)*) => {{
        #[allow(clippy::let_and_return)]
        let res = std::fmt::format(format_args!($($arg)*));
        res
    }}
}

mod banned;
pub use banned::*;

mod getter;
pub use getter::*;

mod hash;
pub use hash::*;

mod submission;
pub use submission::*;

pub use tracing::{error, info, warn};

pub const USER_AGENT: &str = concat!("Tidder ", env!("CARGO_PKG_VERSION"));

pub static EXT_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\W(?:png|jpe?g|gif|webp|p[bgpn]m|tiff?|bmp|ico|hdr)\b").unwrap());
pub static URL_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^(?i)https?://(?:[a-z0-9.-]+|\[[0-9a-f:]+\])(?:$|[:/?#])").unwrap());
pub static PG_POOL: Lazy<Pool> =
    Lazy::new(|| SECRETS.postgres.create_pool(tokio_postgres::NoTls).unwrap());
pub static COMMON_HEADERS: Lazy<HeaderMap<HeaderValue>> = Lazy::new(|| {
    let mut headers = HeaderMap::new();
    headers.insert(header::USER_AGENT, HeaderValue::from_static(USER_AGENT));
    headers
});
pub static REQW_CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .default_headers(COMMON_HEADERS.clone())
        .build()
        .unwrap()
});

pub mod user_error {
    use failure::Error;
    use reqwest::StatusCode;
    use serde::Serialize;
    use std::borrow::Cow;
    use std::fmt::{self, Display, Formatter};

    #[derive(Debug, Serialize)]
    pub enum Source {
        Internal,
        External,
        User,
    }

    #[derive(Debug, Serialize)]
    pub struct UserError {
        pub user_msg: Cow<'static, str>,
        pub source: Source,
        #[serde(skip)]
        pub error: Error,
        #[serde(skip)]
        pub file: Option<&'static str>,
        #[serde(skip)]
        pub line: Option<u32>,
        #[serde(skip)]
        pub save_error: Option<Cow<'static, str>>,
    }

    impl UserError {
        pub fn new<M: Into<Cow<'static, str>> + Sync + Send, E: Into<Error>>(
            user_msg: M,
            error: E,
        ) -> Self {
            Self {
                source: Source::External,
                user_msg: user_msg.into(),
                error: error.into(),
                file: None,
                line: None,
                save_error: None,
            }
        }
        pub fn new_source<M: Into<Cow<'static, str>> + Sync + Send, E: Into<Error>>(
            user_msg: M,
            source: Source,
            error: E,
        ) -> Self {
            Self {
                source,
                user_msg: user_msg.into(),
                error: error.into(),
                file: None,
                line: None,
                save_error: None,
            }
        }
        pub fn new_msg<M: Into<Cow<'static, str>> + Sync + Send>(user_msg: M) -> Self {
            let user_msg = user_msg.into();
            let error = failure::err_msg(user_msg.clone());
            Self {
                source: Source::External,
                user_msg,
                error,
                file: None,
                line: None,
                save_error: None,
            }
        }
        pub fn new_msg_source<M: Into<Cow<'static, str>> + Sync + Send>(
            user_msg: M,
            source: Source,
        ) -> Self {
            let user_msg = user_msg.into();
            let error = failure::err_msg(user_msg.clone());
            Self {
                source,
                user_msg,
                error,
                file: None,
                line: None,
                save_error: None,
            }
        }
        pub fn from_std<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
            Self {
                source: Source::Internal,
                user_msg: Cow::Borrowed("internal error"),
                error: error.into(),
                file: None,
                line: None,
                save_error: None,
            }
        }

        pub fn status_code(&self) -> StatusCode {
            match self.source {
                Source::Internal => StatusCode::INTERNAL_SERVER_ERROR,
                Source::External => StatusCode::OK,
                Source::User => StatusCode::BAD_REQUEST,
            }
        }
    }

    impl<E> From<E> for UserError
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        fn from(error: E) -> Self {
            Self::from_std(error)
        }
    }

    impl Display for UserError {
        fn fmt(&self, f: &mut Formatter) -> fmt::Result {
            Display::fmt(&self.error, f)
        }
    }

    pub fn error_for_status_ue(e: reqwest::Error) -> UserError {
        let msg = match e.status() {
            None => Cow::Borrowed("request failed"),
            Some(sc) => Cow::Owned(format!("recieved error status from host: {}", sc)),
        };

        UserError::new(msg, e)
    }

    #[macro_export]
    macro_rules! ue {
        ($msg:expr) => {
            UserError {
                file: Some(file!()),
                line: Some(line!()),
                ..UserError::new_msg($msg)
            }
        };
        ($msg:expr, $source:expr) => {
            UserError {
                file: Some(file!()),
                line: Some(line!()),
                ..UserError::new_msg_source($msg, $source)
            }
        };
    }

    #[macro_export]
    macro_rules! ue_save {
        ($msg:expr, $save_error:expr) => {
            UserError {
                file: Some(file!()),
                line: Some(line!()),
                save_error: Some($save_error.into()),
                ..UserError::new_msg($msg)
            }
        };
        ($msg:expr, $save_error:expr, $source:expr) => {
            UserError {
                file: Some(file!()),
                line: Some(line!()),
                save_error: Some($save_error.into()),
                ..UserError::new_msg_source($msg, $source)
            }
        };
    }

    #[macro_export]
    macro_rules! map_ue {
        () => {
            |e| UserError {
                file: Some(file!()),
                line: Some(line!()),
                ..UserError::from_std(e)
            }
        };
        ($msg:expr) => {
            |e| UserError {
                file: Some(file!()),
                line: Some(line!()),
                ..UserError::new($msg, failure::Error::from(e))
            }
        };
        ($msg:expr, $source:expr) => {
            |e| UserError {
                file: Some(file!()),
                line: Some(line!()),
                ..UserError::new_source($msg, $source, failure::Error::from(e))
            }
        };
    }

    #[macro_export]
    macro_rules! map_ue_save {
        ($save_error:expr) => {
            |e| UserError {
                file: Some(file!()),
                line: Some(line!()),
                save_error: Some($save_error.into()),
                ..UserError::from_std(e)
            }
        };
        ($msg:expr, $save_error:expr) => {
            |e| UserError {
                file: Some(file!()),
                line: Some(line!()),
                save_error: Some($save_error.into()),
                ..UserError::new($msg, failure::Error::from(e))
            }
        };
        ($msg:expr, $save_error:expr, $source:expr) => {
            |e| UserError {
                file: Some(file!()),
                line: Some(line!()),
                save_error: Some($save_error.into()),
                ..UserError::new_source($msg, $source, failure::Error::from(e))
            }
        };
    }
}

pub use user_error::*;

pub const DEFAULT_DISTANCE: i64 = 1;

pub const IMAGE_MIMES: [&str; 12] = [
    "image/png",
    "image/jpeg",
    "image/jpg",
    "image/gif",
    "image/webp",
    "image/x-portable-anymap",
    "image/tiff",
    "image/x-targa",
    "image/x-tga",
    "image/bmp",
    "image/vnd.microsoft.icon",
    "image/vnd.radiance",
];

pub const IMAGE_MIMES_NO_WEBP: [&str; 11] = [
    "image/png",
    "image/jpeg",
    "image/jpg",
    "image/gif",
    "image/x-portable-anymap",
    "image/tiff",
    "image/x-targa",
    "image/x-tga",
    "image/bmp",
    "image/vnd.microsoft.icon",
    "image/vnd.radiance",
];

#[derive(Deserialize, Serialize)]
pub struct CommonImages {
    pub as_of: chrono::DateTime<chrono::Utc>,
    pub common_images: Vec<CommonImage>,
}

#[derive(Deserialize, Serialize)]
pub struct CommonImage {
    pub num: u64,
    pub link: String,
}

#[derive(Copy, Debug, Clone, Eq, PartialEq)]
pub enum HashDest {
    Images,
    ImageCache,
}

impl HashDest {
    pub fn table_name(self) -> &'static str {
        match self {
            HashDest::Images => "images",
            HashDest::ImageCache => "image_cache",
        }
    }
}

async fn get_existing(link: &str) -> Result<Option<(Hash, HashDest, i64)>, UserError> {
    let client = PG_POOL.get().await?;

    let stmt = client
        .prepare(
            "SELECT hash, id, 'images' as table_name \
             FROM images WHERE link = $1 \
             UNION \
             SELECT hash, id, 'image_cache' as table_name \
             FROM image_cache WHERE link = $1",
        )
        .await?;
    let rows = client.query(&stmt, &[&link]).await?;

    Ok(rows.first().map(|row| {
        (
            Hash(row.get::<_, i64>("hash") as u64),
            match row.get("table_name") {
                "images" => HashDest::Images,
                "image_cache" => HashDest::ImageCache,
                _ => unreachable!(),
            },
            row.get("id"),
        )
    }))
}

pub fn setup_logging(name: &str) {
    fern::Dispatch::new()
        .format(|out, message, record| {
            let level = record.level();
            out.finish(format_args!(
                "[{}]{}[{}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                if level != LevelFilter::Info && level != LevelFilter::Warn {
                    match record.file() {
                        Some(file) => Cow::Owned(format!(
                            "[{}{}]",
                            file,
                            match record.line() {
                                Some(line) => Cow::Owned(format!("#{}", line)),
                                None => Cow::Borrowed(""),
                            }
                        )),
                        None => Cow::Borrowed(""),
                    }
                } else {
                    Cow::Borrowed("")
                },
                record.level(),
                message
            ))
        })
        .level(LevelFilter::Warn)
        .level_for("gotham", LevelFilter::Info)
        .level_for("site", LevelFilter::Info)
        .level_for("op", LevelFilter::Info)
        .level_for("ingest", LevelFilter::Info)
        .level_for("common", LevelFilter::Info)
        .chain(std::io::stderr())
        .chain(
            fern::log_file(format!(
                "/var/log/tidder/{}_{}.log",
                name,
                chrono::Local::now().format("%Y-%m-%d_%H:%M:%S")
            ))
            .unwrap(),
        )
        .apply()
        .unwrap();
}

#[macro_export]
macro_rules! setup_logging {
    () => {
        common::setup_logging(env!("CARGO_PKG_NAME"))
    };
}

pub mod secrets {
    use failure::Error;
    use serde::Deserialize;
    use std::io::Read;

    #[derive(Debug, Deserialize)]
    pub struct Imgur {
        pub client_id: String,
        pub client_secret: String,
        pub rapidapi_key: String,
    }
    #[derive(Debug, Deserialize)]
    pub struct Reddit {
        pub client_id: String,
        pub client_secret: String,
        pub username: String,
        pub password: String,
    }
    #[derive(Debug, Deserialize)]
    pub struct Secrets {
        pub imgur: Imgur,
        pub postgres: deadpool_postgres::Config,
        pub reddit: Reddit,
    }

    pub fn load() -> Result<Secrets, Error> {
        let mut s = String::new();
        std::fs::File::open(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../secrets/secrets.toml"
        ))?
        .read_to_string(&mut s)?;
        toml::from_str::<Secrets>(&s).map_err(Error::from)
    }
}

pub mod config {
    use failure::Error;
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct Config {
        pub banned: Vec<super::Banned>,
        pub custom_limits: std::collections::HashMap<String, Option<u32>>,
        pub enable_imgur_api: bool,
        pub in_flight_limit: u32,
        pub no_blacklist: Vec<String>,
        pub worker_count: usize,
    }

    pub fn load() -> Result<Config, Error> {
        ron::de::from_reader(std::fs::File::open(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../tidder.ron"
        ))?)
        .map_err(Error::from)
    }
}

pub static SECRETS: Lazy<secrets::Secrets> = Lazy::new(|| secrets::load().unwrap());
pub static CONFIG: Lazy<config::Config> = Lazy::new(|| config::load().unwrap());
