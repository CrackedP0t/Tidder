use cache_control::CacheControl;
use chrono::{DateTime, NaiveDateTime};
use image::{imageops, load_from_memory, DynamicImage};
use lazy_static::lazy_static;
use log::LevelFilter;
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use regex::Regex;
use reqwest::{
    header::{self, HeaderMap},
    Response, StatusCode as SC,
};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize, Serializer};
use std::borrow::Cow;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::{BufReader, Read};
use std::string::ToString;
use tokio_postgres::{to_sql_checked, types, NoTls};
use url::{
    percent_encoding::{utf8_percent_encode, QUERY_ENCODE_SET},
    Url,
};

pub use failure::{self, format_err, Error};
pub use log::{error, warn};

lazy_static! {
    pub static ref EXT_RE: Regex =
        Regex::new(r"\W(?:png|jpe?g|gif|webp|p[bgpn]m|tiff?|bmp|ico|hdr)\b").unwrap();
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

#[derive(Debug, Serialize)]
pub struct UserError {
    pub user_msg: String,
    #[serde(serialize_with = "UserError::serialize_status_code")]
    pub status_code: SC,
    #[serde(skip)]
    pub error: Error,
}

impl UserError {
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize_status_code<S>(sc: &SC, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ser.serialize_u16(sc.as_u16())
    }
    pub fn new<M: ToString, E: Into<Error>>(user_msg: M, error: E) -> Self {
        Self {
            status_code: SC::OK,
            user_msg: user_msg.to_string(),
            error: error.into(),
        }
    }
    pub fn new_sc<M: ToString, E: Into<Error>>(user_msg: M, status_code: SC, error: E) -> Self {
        Self {
            status_code,
            user_msg: user_msg.to_string(),
            error: error.into(),
        }
    }
    pub fn new_msg<M: Display + Debug + Send + Sync + 'static>(user_msg: M) -> Self {
        Self {
            status_code: SC::OK,
            user_msg: user_msg.to_string(),
            error: failure::err_msg(user_msg),
        }
    }
    pub fn new_msg_sc<M: Display + Debug + Send + Sync + 'static>(
        user_msg: M,
        status_code: SC,
    ) -> Self {
        Self {
            status_code,
            user_msg: user_msg.to_string(),
            error: failure::err_msg(user_msg),
        }
    }
    pub fn from_std<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self {
            status_code: SC::INTERNAL_SERVER_ERROR,
            user_msg: "internal error".to_string(),
            error: error.into(),
        }
    }
}

impl From<Error> for UserError {
    fn from(error: Error) -> Self {
        Self {
            status_code: SC::INTERNAL_SERVER_ERROR,
            user_msg: "internal error".to_string(),
            error,
        }
    }
}

impl Display for UserError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        Display::fmt(&self.error, f)
    }
}

#[macro_export]
macro_rules! ue {
    ($msg:expr) => {
        UserError::new_msg($msg)
    };
    ($msg:expr, $sc:expr) => {
        UserError::new_msg_sc($msg, $sc)
    };
}

#[macro_export]
macro_rules! map_ue {
    ($msg:expr) => {
        |e| UserError::new($msg, Error::from(e))
    };
    ($msg:expr, $sc:expr) => {
        |e| UserError::new_sc($msg, $sc, Error::from(e))
    };
}

pub const DEFAULT_DISTANCE: i64 = 1;

#[derive(Deserialize, Debug)]
pub struct Submission {
    pub id_int: i64,
    pub id: String,
    pub author: Option<String>,
    pub created_utc: i64,
    pub is_self: bool,
    pub over_18: bool,
    pub permalink: String,
    pub score: i64,
    pub spoiler: Option<bool>,
    pub subreddit: String,
    pub title: String,
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
    pool: &r2d2::Pool<PostgresConnectionManager<NoTls>>,
    post: &Submission,
    image_id: i64,
) {
    lazy_static! {
        static ref ID_RE: Regex = Regex::new(r"/comments/([^/]+)/").map_err(le!()).unwrap();
    }

    let reddit_id = String::from(
        match ID_RE.captures(&post.permalink).and_then(|cap| cap.get(1)) {
            Some(reddit_id) => reddit_id.as_str(),
            None => {
                error!("Couldn't find ID in {}", post.permalink);
                return;
            }
        },
    );

    pool.get().map_err(le!())
        .and_then(
            |mut client|
            client.transaction().map_err(le!())
                .and_then(|mut trans| {
                    trans.execute(
                        "INSERT INTO posts (reddit_id, link, permalink, author, created_utc, score, subreddit, title, nsfw, spoiler, image_id, reddit_id_int) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
                         ON CONFLICT DO NOTHING",
                        &[
                            &reddit_id,
                            &post.url,
                            &post.permalink,
                            &post.author,
                            &(NaiveDateTime::from_timestamp(post.created_utc, 0)),
                            &post.score,
                            &post.subreddit,
                            &post.title,
                            &post.over_18,
                            &post.spoiler.unwrap_or(false),
                            &image_id,
                            &i64::from_str_radix(&reddit_id, 36).map_err(le!())?
                        ],
                    ).map_err(le!())?;
                    trans.commit().map_err(le!())
                }))
        .map(|_| ())
        .unwrap_or_else(|_| ());
}

#[derive(Debug)]
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
    ) -> Result<types::IsNull, Box<std::error::Error + Sync + Send>> {
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

pub const IMAGE_MIMES: [&str; 11] = [
    "image/png",
    "image/jpeg",
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

#[derive(Copy, Debug, Clone)]
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

lazy_static! {
    static ref DB_POOL: r2d2::Pool<PostgresConnectionManager<NoTls>> =
        r2d2::Pool::new(PostgresConnectionManager::new(
            format!(
                "dbname=tidder host=/run/postgresql user={}",
                SECRETS.postgres.username
            )
            .parse()
            .unwrap(),
            NoTls,
        ))
        .unwrap();
}

pub enum GetKind {
    Cache(i64),
    Request(HeaderMap),
}

fn get_existing(link: &str, hash_dest: HashDest) -> Result<Option<(Hash, i64)>, UserError> {
    let mut client = DB_POOL.get().map_err(Error::from)?;
    let mut trans = client.transaction().map_err(Error::from)?;

    trans
        .query(
            format!(
                "SELECT hash, id FROM {} WHERE link=$1",
                hash_dest.table_name()
            )
            .as_str(),
            &[&link],
        )
        .map_err(UserError::from_std)
        .map(|rows| {
            rows.get(0)
                .map(|row| (Hash(row.get::<_, i64>("hash") as u64), row.get("id")))
        })
}

pub fn get_hash(link: &str, hash_dest: HashDest) -> Result<(Hash, Cow<str>, GetKind), UserError> {
    lazy_static! {
        static ref REQW_CLIENT: reqwest::Client = reqwest::Client::builder()
            .timeout(Some(std::time::Duration::from_secs(5)))
            .build()
            .unwrap();
        static ref IMGUR_SEL: Selector = Selector::parse(".post-image-container").unwrap();
        static ref IMGUR_GIFV_RE: Regex = Regex::new(r"([^.]+)\.gifv$").unwrap();
    }

    fn error_for_status_ue(e: reqwest::Error) -> UserError {
        let msg = match e.status() {
            None => Cow::Borrowed("recieved error status from image host"),
            Some(sc) => Cow::Owned(format!("recieved error status from image host: {}", sc)),
        };

        UserError::new(msg, e)
    }

    let url = Url::parse(link).map_err(map_ue!("not a valid URL", SC::BAD_REQUEST))?;

    let scheme = url.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(ue!("unsupported scheme in URL"));
    }

    let host = url
        .host_str()
        .ok_or_else(|| ue!("no host in URL", SC::BAD_REQUEST))?;

    let path = url.path();

    let link: Cow<str> = if host == "imgur.com" {
        if !EXT_RE.is_match(&link) {
            let mut path_segs = url
                .path_segments()
                .ok_or_else(|| ue!("cannot-be-a-base URL", SC::BAD_REQUEST))?;
            let first = path_segs
                .next()
                .ok_or_else(|| ue!("no first path segment in URL", SC::BAD_REQUEST))?;

            match first {
                "a" | "gallery" => {
                    let mut resp = REQW_CLIENT
                        .get(link)
                        .send()
                        .and_then(|resp| {
                            if resp.status() == SC::NOT_FOUND && link.contains("imgur.com/gallery/")
                            {
                                REQW_CLIENT
                                    .get(&link.replace("/gallery/", "/a/"))
                                    .send()
                                    .and_then(Response::error_for_status)
                            } else {
                                resp.error_for_status()
                            }
                        })
                        .map_err(error_for_status_ue)?;

                    let mut doc_string = String::new();

                    resp.read_to_string(&mut doc_string).map_err(Error::from)?;

                    Cow::Owned(
                        Html::parse_document(&doc_string)
                            .select(&IMGUR_SEL)
                            .next()
                            .and_then(|el| {
                                Some(
                                    "https://i.imgur.com/".to_string()
                                        + el.value().attr("id")?
                                        + ".jpg",
                                )
                            })
                            .ok_or_else(|| {
                                UserError::new_msg("couldn't extract image from Imgur album")
                            })?,
                    )
                }
                hash => Cow::Owned(format!("https://i.imgur.com/{}.jpg", hash)),
            }
        } else {
            Cow::Borrowed(link)
        }
    } else if host == "i.imgur.com" && IMGUR_GIFV_RE.is_match(path) {
        Cow::Owned(
            IMGUR_GIFV_RE
                .replace(path, "https://i.imgur.com/$1.gif")
                .to_string(),
        )
    } else {
        Cow::Borrowed(link)
    };

    if let Some(exists) = get_existing(&link, hash_dest)? {
        return Ok((exists.0, link, GetKind::Cache(exists.1)));
    }

    let mut resp = REQW_CLIENT
        .get(&utf8_percent_encode(&link, QUERY_ENCODE_SET).collect::<String>())
        .header(header::ACCEPT, IMAGE_MIMES.join(","))
        .header(
            header::USER_AGENT,
            "Mozilla/5.0 (X11; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0",
        )
        .send()
        .map_err(map_ue!("couldn't connect to image host"))?;

    resp.error_for_status_ref().map_err(error_for_status_ue)?;

    if let Some(ct) = resp.headers().get(header::CONTENT_TYPE) {
        let ct = ct
            .to_str()
            .map_err(map_ue!("non-ASCII Content-Type header", SC::BAD_REQUEST))?;
        if !IMAGE_MIMES.contains(&ct) {
            return Err(ue!(format!("unsupported Content-Type: {}", ct)));
        }
    }

    let url = resp.url();
    if url
        .host_str()
        .map(|host| host == "i.imgur.com")
        .unwrap_or(false)
        && url.path() == "/removed.png"
    {
        return Err(UserError::new_msg("removed from Imgur"));
    }

    let mut image = Vec::<u8>::with_capacity(
        resp.headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|hv| hv.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(2048),
    );

    BufReader::new(resp.by_ref())
        .read_to_end(&mut image)
        .map_err(Error::from)?;

    Ok((
        hash_from_memory(&image)?,
        link,
        GetKind::Request(resp.headers().to_owned()),
    ))
}

pub fn save_hash(link: &str, hash_dest: HashDest) -> Result<(Hash, i64, bool), UserError> {
    let (hash, link, get_kind) = get_hash(link, hash_dest)?;

    match get_kind {
        GetKind::Cache(id) => Ok((hash, id, true)),
        GetKind::Request(headers) => {
            let now = chrono::offset::Utc::now().naive_utc();
            let cc: Option<CacheControl> = headers
                .get(header::CACHE_CONTROL)
                .and_then(|hv| hv.to_str().ok())
                .and_then(|s| cache_control::with_str(s).ok());
            let cc = cc.as_ref();

            let mut client = DB_POOL.get().map_err(Error::from)?;
            let mut trans = client.transaction().map_err(Error::from)?;
            let rows = trans
                .query(
                    format!(
                        "INSERT INTO {} (link, hash, no_store, no_cache, expires, \
                         etag, must_revalidate, retrieved_on) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
                         ON CONFLICT DO NOTHING \
                         RETURNING id",
                        hash_dest.table_name()
                    )
                    .as_str(),
                    &[
                        &link,
                        &hash,
                        &cc.map(|cc| cc.no_store),
                        &cc.map(|cc| cc.no_cache),
                        &cc.and_then(|cc| cc.max_age)
                            .map(|n| NaiveDateTime::from_timestamp(n as i64, 0))
                            .or_else(|| {
                                headers
                                    .get(header::EXPIRES)
                                    .and_then(|hv| hv.to_str().ok())
                                    .and_then(|s| DateTime::parse_from_rfc2822(s).ok())
                                    .map(|dt| dt.naive_utc())
                            }),
                        &headers.get(header::ETAG).and_then(|hv| hv.to_str().ok()),
                        &cc.map(|cc| cc.must_revalidate),
                        &now,
                    ],
                )
                .map_err(Error::from)?;
            trans.commit().map_err(Error::from)?;

            match rows.get(0) {
                Some(row) => Ok((hash, row.try_get("id").map_err(Error::from)?, false)),
                None => get_existing(&link, hash_dest)?
                    .map(|ex| (ex.0, ex.1, true))
                    .ok_or_else(|| ue!("conflict but no existing match")),
            }
        }
    }
}

pub fn setup_logging() {
    fern::Dispatch::new()
        .format(|out, message, record| {
            let level = record.level();
            out.finish(format_args!(
                "{}[{}{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d %H:%M:%S]"),
                record.target(),
                if level != LevelFilter::Info && level != LevelFilter::Warn {
                    match record.file() {
                        Some(file) => format!(
                            ":{}{}",
                            file,
                            match record.line() {
                                Some(line) => format!("#{}", line),
                                None => "".to_string(),
                            }
                        ),
                        None => "".to_string(),
                    }
                } else {
                    "".to_string()
                },
                record.level(),
                message
            ))
        })
        .level(LevelFilter::Warn)
        .level_for("site", LevelFilter::Info)
        .level_for("watcher", LevelFilter::Info)
        .level_for("hasher", LevelFilter::Info)
        .level_for("ingest", LevelFilter::Info)
        .level_for("common", LevelFilter::Info)
        .chain(std::io::stderr())
        .chain(
            fern::log_file(format!(
                "output_{}.log",
                chrono::Local::now().format("%Y-%m-%d_%H:%M:%S")
            ))
            .unwrap(),
        )
        .apply()
        .unwrap();
}

pub mod secrets {
    use failure::Error;
    use serde::Deserialize;
    use std::io::Read;

    #[derive(Debug, Deserialize)]
    pub struct Imgur {
        pub client_id: String,
        pub client_secret: String,
    }
    #[derive(Debug, Deserialize)]
    pub struct Postgres {
        pub username: String,
    }
    #[derive(Debug, Deserialize)]
    pub struct Secrets {
        pub imgur: Imgur,
        pub postgres: Postgres,
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
