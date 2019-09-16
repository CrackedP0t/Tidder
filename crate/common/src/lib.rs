use cache_control::CacheControl;
use chrono::{DateTime, NaiveDateTime};
pub use failure::{self, format_err, Error};
use futures::future::{err, Either, Future};
use futures::stream::Stream;
use image::{imageops, load_from_memory, DynamicImage};
use lazy_static::lazy_static;
use log::LevelFilter;
pub use log::{error, info, warn};
use regex::Regex;
use reqwest::header::{self, HeaderMap};
use serde::Deserialize;
use std::borrow::Cow;
use std::fmt::{self, Display};
use std::string::ToString;
use tokio_postgres::{to_sql_checked, types};
use url::{
    percent_encoding::{percent_decode, utf8_percent_encode, QUERY_ENCODE_SET},
    Url,
};

mod getter;
pub use getter::*;

mod pool;
pub use pool::*;

#[macro_export]
macro_rules! fut_try {
    ($res:expr) => {
        match $res {
            Ok(r) => r,
            Err(e) => return Either::B(err(e)),
        }
    };
    ($res:expr, ) => {
        match $res {
            Ok(r) => r,
            Err(e) => return err(e),
        }
    };
    ($res:expr, $wrap:path) => {
        match $res {
            Ok(r) => r,
            Err(e) => return $wrap(err(e)),
        }
    };
}

lazy_static! {
    pub static ref EXT_RE: Regex =
        Regex::new(r"(?i)\W(?:png|jpe?g|gif|webp|p[bgpn]m|tiff?|bmp|ico|hdr)\b").unwrap();
    pub static ref URL_RE: Regex =
        Regex::new(r"^(?i)https?://(?:[a-z0-9.-]+|\[[0-9a-f:]+\])(?:$|[:/?#])").unwrap();
    pub static ref PG_POOL: PgPool = PgPool::new(&SECRETS.postgres.connect);
}

// Log Error, returning empty
#[macro_export]
macro_rules! le {
    () => {
        |e| error!("{}", e)
    };
}

// Log Error as Info, returning empty
#[macro_export]
macro_rules! lei {
    () => {
        |e| info!("{}", e)
    };
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
        // #[serde(serialize_with = "UserError::serialize_status_code")]
        pub source: Source,
        #[serde(skip)]
        pub error: Error,
        #[serde(skip)]
        pub file: Option<&'static str>,
        #[serde(skip)]
        pub line: Option<u32>,
    }

    impl UserError {
        // #[allow(clippy::trivially_copy_pass_by_ref)]
        // pub fn serialize_status_code<S>(sc: &SC, ser: S) -> Result<S::Ok, S::Error>
        // where
        //     S: Serializer,
        // {
        //     ser.serialize_u16(sc.as_u16())
        // }
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
            }
        }
        pub fn from_std<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
            Self {
                source: Source::Internal,
                user_msg: Cow::Borrowed("internal error"),
                error: error.into(),
                file: None,
                line: None,
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

    // impl From<Error> for UserError {
    //     fn from(error: Error) -> Self {
    //         Self {
    //             source: Source::Internal,
    //             user_msg: Cow::Borrowed("internal error"),
    //             error,
    //             file: None,
    //             line: None,
    //         }
    //     }
    // }

    // impl<E> From<E> for UserError
    // where
    //     E: std::error::Error + Send + Sync + 'static,
    // {
    //     fn from(error: E) -> Self {
    //         Self::from_std(error)
    //     }
    // }

    impl From<tokio_postgres::error::Error> for UserError {
        fn from(error: tokio_postgres::error::Error) -> Self {
            Self::from_std(error)
        }
    }

    impl std::error::Error for UserError {}

    impl Display for UserError {
        fn fmt(&self, f: &mut Formatter) -> fmt::Result {
            Display::fmt(&self.error, f)
        }
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
                ..UserError::new($msg, Error::from(e))
            }
        };
        ($msg:expr, $source:expr) => {
            |e| UserError {
                file: Some(file!()),
                line: Some(line!()),
                ..UserError::new_source($msg, $source, Error::from(e))
            }
        };
    }
}

pub use user_error::*;

pub const DEFAULT_DISTANCE: i64 = 1;

#[derive(Deserialize, Debug)]
pub struct Submission {
    pub id_int: i64,
    pub id: String,
    pub author: Option<String>,
    pub created_utc: NaiveDateTime,
    pub is_self: bool,
    pub over_18: bool,
    pub permalink: String,
    pub score: i64,
    pub spoiler: Option<bool>,
    pub subreddit: String,
    pub title: String,
    pub thumbnail: Option<String>,
    pub thumbnail_width: Option<i32>,
    pub thumbnail_height: Option<i32>,
    pub updated: Option<NaiveDateTime>,
    pub url: String,
}

#[derive(Deserialize, Debug)]
pub struct Hit {
    #[serde(rename = "_source")]
    pub source: Submission,
}

#[derive(Deserialize, Debug)]
pub struct Hits {
    pub hits: Vec<Hit>,
}

#[derive(Deserialize, Debug)]
pub struct PushShiftSearch {
    pub hits: Hits,
}

pub fn save_post(
    post: Submission,
    image_id: Option<i64>,
) -> impl Future<Item = bool, Error = UserError> {
    lazy_static! {
        static ref ID_RE: Regex = Regex::new(r"/comments/([^/]+)/").unwrap();
    }

    let reddit_id = String::from(
        fut_try!(ID_RE
            .captures(&post.permalink)
            .and_then(|cap| cap.get(1))
            .ok_or_else(|| ue!("Couldn't find ID in permalink")))
        .as_str(),
    );

    Either::A(PG_POOL.take().and_then(move |mut client| {
        client
            .build_transaction()
            .build(
                client
                    .prepare(
                        "INSERT INTO posts \
                         (reddit_id, link, permalink, author, \
                         created_utc, score, subreddit, title, nsfw, \
                         spoiler, image_id, reddit_id_int, \
                         thumbnail, thumbnail_width, thumbnail_height) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, \
                         $8, $9, $10, $11, $12, $13, $14, $15) \
                         ON CONFLICT DO NOTHING",
                    )
                    .and_then(move |stmt| {
                        client.execute(
                            &stmt,
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
                                &post.thumbnail_height
                            ],
                        )
                    })
                    .map(|modified| modified > 0),
            )
            .map_err(map_ue!())
    }))
}

#[derive(Debug, Copy, Clone)]
pub struct Hash(u64);

impl Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl types::ToSql for Hash {
    fn to_sql(
        &self,
        t: &types::Type,
        w: &mut Vec<u8>,
    ) -> Result<types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        (self.0 as i64).to_sql(t, w)
    }

    fn accepts(t: &types::Type) -> bool {
        i64::accepts(t)
    }

    to_sql_checked!();
}

pub fn dhash(img: DynamicImage) -> Hash {
    let small_img = imageops::thumbnail(&img.to_luma(), 9, 8);

    let mut hash: u64 = 0;

    for y in 0..8 {
        for x in 0..8 {
            let bit = ((small_img.get_pixel(x, y)[0] > small_img.get_pixel(x + 1, y)[0]) as u64)
                << (x + y * 8);
            hash |= bit;
        }
    }

    Hash(hash)
}

pub fn distance(a: Hash, b: Hash) -> u32 {
    (a.0 ^ b.0).count_ones()
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

pub fn hash_from_memory(image: &[u8]) -> Result<Hash, UserError> {
    Ok(dhash(
        // match format {
        //     Some(format) => load_from_memory_with_format(&file, format),
        // None =>
        load_from_memory(&image)
            // }
            .map_err(map_ue!("invalid image"))?,
    ))
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

pub enum GetKind {
    Cache(HashDest, i64),
    Request(HeaderMap),
}

fn get_existing(
    link: String,
) -> impl Future<Item = Option<(Hash, HashDest, i64)>, Error = UserError> {
    PG_POOL.take().and_then(move |mut client| {
        client
            .build_transaction()
            .build(
                client
                    .prepare(
                        "SELECT hash, id, 'images' as table_name \
                         FROM images WHERE link = $1 \
                         UNION \
                         SELECT hash, id, 'image_cache' as table_name \
                         FROM image_cache WHERE link = $1",
                    )
                    .and_then(move |stmt| {
                        client
                            .query(&stmt, &[&link])
                            .into_future()
                            .map_err(|(e, _)| e)
                            .map(|(row, _)| {
                                row.map(|row| {
                                    (
                                        Hash(row.get::<_, i64>("hash") as u64),
                                        match row.get("table_name") {
                                            "images" => HashDest::Images,
                                            "image_cache" => HashDest::ImageCache,
                                            _ => unreachable!(),
                                        },
                                        row.get("id"),
                                    )
                                })
                            })
                    }),
            )
            .map_err(map_ue!())
    })
}

fn error_for_status_ue(e: reqwest::Error) -> UserError {
    let msg = match e.status() {
        None => Cow::Borrowed("couldn't download image"),
        Some(sc) => Cow::Owned(format!("recieved error status from image host: {}", sc)),
    };

    UserError::new(msg, e)
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
                                None => Cow::Borrowed("")
                            }
                        )),
                        None => Cow::Borrowed("")
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
        .level_for("watcher", LevelFilter::Info)
        .level_for("hasher", LevelFilter::Info)
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
    }
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
        pub password: String
    }
    #[derive(Debug, Deserialize)]
    pub struct Secrets {
        pub imgur: Imgur,
        pub postgres: Postgres,
        pub reddit: Reddit
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
