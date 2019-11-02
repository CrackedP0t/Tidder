use cache_control::CacheControl;
use chrono::{DateTime, NaiveDateTime};
pub use failure::{self, format_err, Error};
use futures::prelude::*;
use lazy_static::lazy_static;
use log::LevelFilter;
use regex::Regex;
use reqwest::header::{self, HeaderMap, HeaderValue};
use serde::Deserialize;
use std::borrow::Cow;
use std::string::ToString;
use std::time::Duration;
use url::{
    percent_encoding::{percent_decode, utf8_percent_encode, QUERY_ENCODE_SET},
    Url,
};

mod getter;
pub use getter::*;

mod pool;
pub use pool::*;

mod hash;
pub use hash::*;

pub use log::{error, info, warn};

pub const USER_AGENT: &str = concat!("Tidder ", env!("CARGO_PKG_VERSION"));

lazy_static! {
    pub static ref EXT_RE: Regex =
        Regex::new(r"(?i)\W(?:png|jpe?g|gif|webp|p[bgpn]m|tiff?|bmp|ico|hdr)\b").unwrap();
    pub static ref URL_RE: Regex =
        Regex::new(r"^(?i)https?://(?:[a-z0-9.-]+|\[[0-9a-f:]+\])(?:$|[:/?#])").unwrap();
    pub static ref PG_POOL: PgPool = PgPool::new(&SECRETS.postgres.connect);
    pub static ref COMMON_HEADERS: HeaderMap<HeaderValue> = {
        let mut headers = HeaderMap::new();
        headers.insert(header::USER_AGENT, HeaderValue::from_static(USER_AGENT));
        headers
    };
    pub static ref REQW_CLIENT: reqwest::Client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .default_headers(COMMON_HEADERS.clone())
        .build()
        .unwrap();
}

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

#[derive(Deserialize, Debug)]
pub struct Submission {
    #[serde(default)]
    pub id_int: i64,
    pub id: String,
    pub author: String,
    #[serde(deserialize_with = "de_sub::created_utc")]
    pub created_utc: NaiveDateTime,
    #[serde(default, deserialize_with = "de_sub::crosspost_parent")]
    pub crosspost_parent: Option<i64>,
    pub is_self: bool,
    pub over_18: bool,
    pub permalink: String,
    pub promoted: Option<bool>,
    pub score: i64,
    pub spoiler: Option<bool>,
    pub subreddit: String,
    pub title: String,
    pub thumbnail: Option<String>,
    pub thumbnail_width: Option<i32>,
    pub thumbnail_height: Option<i32>,
    #[serde(default)]
    pub updated: Option<NaiveDateTime>,
    pub url: String,
}

impl Submission {
    pub fn finalize(mut self) -> Result<Self, UserError> {
        self.id_int = i64::from_str_radix(&self.id, 36).map_err(|e| {
            UserError::new_source(
                format!("Couldn't parse number from ID '{}'", self.id),
                Source::Internal,
                e,
            )
        })?;

        Ok(self)
    }
}

mod de_sub {
    use super::*;
    use serde::de::{self, Deserializer, Unexpected, Visitor};
    use std::fmt::{self, Formatter};

    pub fn created_utc<'de, D>(des: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CreatedUTC;
        impl<'de> Visitor<'de> for CreatedUTC {
            type Value = NaiveDateTime;

            fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
                write!(formatter, "a number, possibly inside a string")
            }

            fn visit_u64<E>(self, secs: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_i64(secs as i64)
            }

            fn visit_i64<E>(self, secs: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(NaiveDateTime::from_timestamp(secs, 0))
            }

            fn visit_str<E>(self, secs: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let secs = secs
                    .parse()
                    .map_err(|_e| E::invalid_value(Unexpected::Str(secs), &self))?;
                self.visit_i64(secs)
            }

            fn visit_f64<E>(self, secs: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_i64(secs as i64)
            }
        }

        des.deserialize_any(CreatedUTC)
    }

    pub fn crosspost_parent<'de, D>(des: D) -> Result<Option<i64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CrosspostParent;
        impl<'de> Visitor<'de> for CrosspostParent {
            type Value = Option<i64>;

            fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
                write!(formatter, "t3_<id>")
            }

            fn visit_some<D>(self, des: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                des.deserialize_str(self)
            }

            fn visit_str<E>(self, name: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                lazy_static! {
                    static ref T3_RE: Regex = Regex::new("^t3_([[:alnum:]]+)$").unwrap();
                }

                T3_RE
                    .captures(name)
                    .and_then(|cs| cs.get(1))
                    .and_then(|id| i64::from_str_radix(id.as_str(), 36).ok())
                    .ok_or_else(|| E::invalid_value(Unexpected::Str(name), &self))
                    .map(Some)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(None)
            }
        }

        des.deserialize_option(CrosspostParent)
    }
}

pub async fn save_post(
    post: &Submission,
    image_id: Result<i64, Option<Cow<'static, str>>>,
) -> Result<bool, UserError> {
    lazy_static! {
        static ref ID_RE: Regex = Regex::new(r"/comments/([^/]+)/").unwrap();
    }

    let reddit_id = String::from(
        ID_RE
            .captures(&post.permalink)
            .and_then(|cap| cap.get(1))
            .ok_or_else(|| ue!("Couldn't find ID in permalink"))?
            .as_str(),
    );

    let mut client = PG_POOL.take().await?;
    let trans = client.transaction().await?;

    let modified = match image_id {
        Ok(image_id) => {
            trans
                .execute(
                    "INSERT INTO posts \
                     (reddit_id, link, permalink, author, \
                     created_utc, score, subreddit, title, nsfw, \
                     spoiler, image_id, reddit_id_int, \
                     thumbnail, thumbnail_width, thumbnail_height, \
                     crosspost_parent) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, \
                     $8, $9, $10, $11, $12, $13, $14, $15, $16) \
                     ON CONFLICT DO NOTHING",
                    &[
                        &reddit_id,
                        &post.url,
                        &post.permalink,
                        &post.author,
                        &post.created_utc,
                        &post.score,
                        &post.subreddit,
                        &post.title,
                        &post.over_18,
                        &post.spoiler.unwrap_or(false),
                        &image_id,
                        &i64::from_str_radix(&reddit_id, 36).unwrap(),
                        &post.thumbnail,
                        &post.thumbnail_width,
                        &post.thumbnail_height,
                        &post.crosspost_parent,
                    ],
                )
                .await?
        }
        Err(save_error) => {
            trans
                .execute(
                    "INSERT INTO posts \
                     (reddit_id, link, permalink, author, \
                     created_utc, score, subreddit, title, nsfw, \
                     spoiler, reddit_id_int, thumbnail, \
                     thumbnail_width, thumbnail_height, save_error, \
                     crosspost_parent) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, \
                     $8, $9, $10, $11, $12, $13, $14, $15, $16) \
                     ON CONFLICT DO NOTHING",
                    &[
                        &reddit_id,
                        &post.url,
                        &post.permalink,
                        &post.author,
                        &post.created_utc,
                        &post.score,
                        &post.subreddit,
                        &post.title,
                        &post.over_18,
                        &post.spoiler.unwrap_or(false),
                        &i64::from_str_radix(&reddit_id, 36).unwrap(),
                        &post.thumbnail,
                        &post.thumbnail_width,
                        &post.thumbnail_height,
                        &save_error,
                        &post.crosspost_parent,
                    ],
                )
                .await?
        }
    };

    trans.commit().await?;

    Ok(modified > 0)
}

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
    let client = PG_POOL.take().await?;

    let stmt = client
        .prepare(
            "SELECT hash, id, 'images' as table_name \
             FROM images WHERE link = $1 \
             UNION \
             SELECT hash, id, 'image_cache' as table_name \
             FROM image_cache WHERE link = $1",
        )
        .await
        .map_err(map_ue!())?;
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
    pub struct Postgres {
        pub connect: String,
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
        pub postgres: Postgres,
        pub reddit: Reddit,
    }

    pub fn load() -> Result<Secrets, Error> {
        let mut s = String::new();
        std::fs::File::open("../secrets/secrets.toml")
            .map_err(Error::from)?
            .read_to_string(&mut s)
            .map_err(Error::from)?;
        toml::from_str::<Secrets>(&s).map_err(Error::from)
    }
}

lazy_static! {
    pub static ref SECRETS: secrets::Secrets = secrets::load().unwrap();
}
