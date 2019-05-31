use cache_control::CacheControl;
use chrono::{DateTime, NaiveDateTime};
use failure::Fail;
use image::{imageops, load_from_memory, DynamicImage};
use lazy_static::lazy_static;
use log::{error, LevelFilter};
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use regex::Regex;
use reqwest::{header, Response, StatusCode};
use scraper::{Html, Selector};
use serde::Deserialize;
use std::borrow::Cow;
use std::fmt;
use std::io::{BufReader, Read};
use std::string::ToString;
use tokio_postgres::{to_sql_checked, types, NoTls};
use url::{
    percent_encoding::{utf8_percent_encode, QUERY_ENCODE_SET},
    Url,
};

pub use failure::{self, format_err, Error};

lazy_static! {
    pub static ref EXT_RE: Regex =
        Regex::new(r"\W(?:png|jpe?g|gif|webp|p[bgpn]m|tiff?|bmp|ico|hdr)\b").unwrap();
}

include!(concat!(env!("OUT_DIR"), "/codegen.rs"));

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

// Log error, returning Error::From
#[macro_export]
macro_rules! lef {
    () => {
        |e| {
            error!("{}", e);
            Error::from(e)
        }
    };
}

// Log custom error, returning Format_Err!
#[macro_export]
macro_rules! lfe {
    ($fs:expr $(,$args:expr)*) => {{
        error!($fs, $($args),*);
        format_err!($fs, $($args),*)
    }};
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

#[derive(Debug, Fail)]
#[fail(display = "Got error status {}", status)]
pub struct StatusFail {
    pub status: StatusCode,
}

#[derive(Debug, Fail)]
#[fail(display = "Getting {} failed: {}", link, error)]
pub struct GetHashFail {
    pub link: String,
    pub error: Error,
}

impl GetHashFail {
    pub fn new(link: &str, error: Error) -> Self {
        GetHashFail {
            link: String::from(link),
            error,
        }
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
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

macro_rules! map_ghf {
    ($link:expr) => {
        |e| GetHashFail {
            link: $link.to_string(),
            error: Error::from(e),
        }
    };
}

pub fn hash_from_memory(image: &[u8]) -> Result<Hash, Error> {
    Ok(dhash(
        // match format {
        //     Some(format) => load_from_memory_with_format(&file, format),
        // None =>
        load_from_memory(&image)
            // }
            .map_err(Error::from)?,
    ))
}

pub enum HashDest {
    Images,
    ImageCache,
}

impl HashDest {
    pub fn table_name(&self) -> &'static str {
        match self {
            HashDest::Images => "images",
            HashDest::ImageCache => "image_cache",
        }
    }
}

pub fn get_hash(link: &str, hash_dest: HashDest) -> Result<(Hash, i64, bool), GetHashFail> {
    lazy_static! {
        static ref REQW_CLIENT: reqwest::Client = reqwest::Client::builder()
            .timeout(Some(std::time::Duration::from_secs(5)))
            .build()
            .unwrap();
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
        static ref IMGUR_SEL: Selector = Selector::parse(".post-image-container").unwrap();
    }

    let url = Url::parse(link).map_err(map_ghf!(link))?;

    let link = if let Some(host) = url.host_str() {
        if host == "imgur.com" && !EXT_RE.is_match(&link) {
            let mut path_segs = url.path_segments().ok_or_else(|| GetHashFail {
                link: link.to_string(),
                error: format_err!("cannot-be-a-base URL"),
            })?;
            let first = path_segs.next().ok_or_else(|| GetHashFail {
                link: link.to_string(),
                error: format_err!("base URL"),
            })?;

            match first {
                "a" | "gallery" => {
                    let mut resp = REQW_CLIENT
                        .get(link)
                        .send()
                        .and_then(Response::error_for_status)
                        .map_err(map_ghf!(link))?;

                    let mut doc_string = String::new();

                    resp.read_to_string(&mut doc_string)
                        .map_err(map_ghf!(link))?;

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
                                GetHashFail::new(
                                    link,
                                    format_err!("couldn't extract image from Imgur album"),
                                )
                            })?,
                    )
                }
                hash => Cow::Owned(format!("https://i.imgur.com/{}.jpg", hash)),
            }
        } else {
            Cow::Borrowed(link)
        }
    } else {
        Cow::Borrowed(link)
    };

    let mut client = DB_POOL.get().map_err(map_ghf!(link))?;
    let mut trans = client.transaction().map_err(map_ghf!(link))?;

    let mut get_existing = || {
        trans
            .query(
                format!(
                    "SELECT hash, id FROM {} WHERE link=$1",
                    hash_dest.table_name()
                )
                .as_str(),
                &[&link],
            )
            .map_err(map_ghf!(link))
            .map(|rows| {
                rows.get(0)
                    .map(|row| (Hash(row.get::<_, i64>("hash") as u64), row.get("id"), true))
            })
    };

    if let Some(exists) = get_existing()? {
        return Ok(exists);
    }

    let mut resp = REQW_CLIENT
        .get(&utf8_percent_encode(&link, QUERY_ENCODE_SET).collect::<String>())
        .header(header::ACCEPT, IMAGE_MIMES.join(","))
        .header(
            header::USER_AGENT,
            "Mozilla/5.0 (X11; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0",
        )
        .send()
        .map_err(map_ghf!(link))?;

    let status = resp.status();

    let _format = if status.is_success() {
        let url = resp.url();
        if url
            .host_str()
            .map(|host| host == "i.imgur.com")
            .unwrap_or(false)
            && url.path() == "/removed.png"
        {
            return Err(GetHashFail {
                link: link.to_string(),
                error: format_err!("removed from Imgur"),
            });
        }
        resp.headers()
            .get(header::CONTENT_TYPE)
            .map(|ctype| {
                let val = ctype.to_str().map_err(map_ghf!(link))?;
                match IMAGE_MIME_MAP.get(val) {
                    Some(format) => Ok(*format),
                    None => Err(GetHashFail {
                        link: link.to_string(),
                        error: format_err!("got unsupported MIME type {}", val),
                    }),
                }
            })
            .transpose()?
    } else {
        return Err(GetHashFail {
            link: link.to_string(),
            error: Error::from(StatusFail { status }),
        });
    };

    let now = chrono::offset::Utc::now().naive_utc();

    let mut image = Vec::<u8>::with_capacity(
        resp.headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|hv| hv.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(2048),
    );

    BufReader::new(resp.by_ref())
        .read_to_end(&mut image)
        .map_err(map_ghf!(link))?;

    let hash = hash_from_memory(&image).map_err(map_ghf!(link))?;

    let headers = resp.headers();

    let cc: Option<CacheControl> = headers
        .get(header::CACHE_CONTROL)
        .and_then(|hv| hv.to_str().ok())
        .and_then(|s| cache_control::with_str(s).ok());
    let cc = cc.as_ref();

    let mut client = DB_POOL.get().map_err(map_ghf!(link))?;
    let mut trans = client.transaction().map_err(map_ghf!(link))?;
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
        .map_err(map_ghf!(link))?;

    trans.commit().map_err(map_ghf!(link))?;

    match rows.get(0) {
        Some(row) => Ok((hash, row.try_get("id").map_err(map_ghf!(link))?, false)),
        None => get_existing()?
            .ok_or_else(|| format_err!("conflict but no existing match"))
            .map_err(map_ghf!(link)),
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