use cache_control::CacheControl;
use chrono::{DateTime, NaiveDateTime};
use failure::{format_err, Fail};
use image::{imageops, load_from_memory, load_from_memory_with_format, DynamicImage};
use lazy_static::lazy_static;
use log::{error, LevelFilter};
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use regex::Regex;
use reqwest::{header, StatusCode};
use serde::Deserialize;
use std::fmt;
use std::io::{BufReader, Read};
use tokio_postgres::{to_sql_checked, types, NoTls};
use url::percent_encoding::{utf8_percent_encode, QUERY_ENCODE_SET};

pub use failure::{self, Error};

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

#[derive(Deserialize, Debug)]
pub struct Submission {
    pub author: Option<String>,
    pub created_utc: i64,
    pub is_self: bool,
    pub over_18: bool,
    pub permalink: String,
    pub score: i32,
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
                        "INSERT INTO posts (reddit_id, link, permalink, author, created_utc, score, subreddit, title, nsfw, spoiler, image_id) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
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
            let bit = ((small_img.get_pixel(x, y).data[0] > small_img.get_pixel(x + 1, y).data[0])
                as u64)
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
            link: $link.clone(),
            error: Error::from(e),
        }
    };
}

pub fn get_hash(link: String) -> Result<(Hash, i64), GetHashFail> {
    lazy_static! {
        static ref REQW_CLIENT: reqwest::Client = reqwest::Client::new();
        static ref DB_POOL: r2d2::Pool<PostgresConnectionManager<NoTls>> =
            r2d2::Pool::new(PostgresConnectionManager::new(
                "dbname=tidder host=/run/postgresql user=postgres"
                    .parse()
                    .unwrap(),
                NoTls,
            ))
            .unwrap();
    }

    let mut client = DB_POOL.get().map_err(map_ghf!(link))?;
    let mut trans = client.transaction().map_err(map_ghf!(link))?;
    if let Some(row) = trans
        .query("SELECT hash, id FROM images WHERE link=$1", &[&link])
        .map_err(map_ghf!(link))?
        .get(0)
    {
        return Ok((Hash(row.get::<_, i64>("hash") as u64), row.get("id")));
    }

    let mut this_link = link;

    let (resp, format) = loop {
        let resp = REQW_CLIENT
            .get(&utf8_percent_encode(&this_link, QUERY_ENCODE_SET).collect::<String>())
            .header(header::ACCEPT, IMAGE_MIMES.join(","))
            .header(
                header::USER_AGENT,
                "Mozilla/5.0 (X11; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0",
            )
            .send()
            .map_err(map_ghf!(this_link))?;

        let status = resp.status();

        if status.is_success() {
            match resp.headers().get(header::CONTENT_TYPE) {
                Some(ctype) => {
                    let val = ctype.to_str().map_err(map_ghf!(this_link))?;
                    match IMAGE_MIME_MAP.get(val) {
                        Some(format) => {
                            break (resp, Some(*format));
                        }
                        None => {
                            return Err(GetHashFail {
                                link: this_link.clone(),
                                error: format_err!("Got unsupported MIME type {}", val),
                            });
                        }
                    }
                }
                None => {
                    break (resp, None);
                }
            }
        } else if status.is_redirection() {
            this_link = String::from(
                resp.headers()
                    .get(header::LOCATION)
                    .ok_or_else(|| GetHashFail {
                        link: this_link.clone(),
                        error: format_err!("Redirected without location"),
                    })?
                    .to_str()
                    .map_err(map_ghf!(this_link))?,
            );
            continue;
        } else {
            return Err(GetHashFail {
                link: this_link,
                error: Error::from(StatusFail { status }),
            });
        }
    };

    let now = chrono::offset::Utc::now().naive_utc();

    let headers = resp.headers().clone();

    let mut file = Vec::<u8>::with_capacity(
        headers
            .get(header::CONTENT_LENGTH)
            .and_then(|hv| hv.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(2048),
    );

    let cc: Option<CacheControl> = headers
        .get(header::CACHE_CONTROL)
        .and_then(|hv| hv.to_str().ok())
        .and_then(|s| cache_control::from_str(s).ok());
    let cc = cc.as_ref();

    {
        BufReader::new(resp)
            .read_to_end(&mut file)
            .map_err(map_ghf!(this_link))?;
    }

    let hash = dhash(
        match format {
            Some(format) => load_from_memory_with_format(&file, format),
            None => load_from_memory(&file),
        }
        .map_err(map_ghf!(this_link))?,
    );

    let mut client = DB_POOL.get().map_err(map_ghf!(this_link))?;
    let mut trans = client.transaction().map_err(map_ghf!(this_link))?;
    let image_id = trans
        .query(
            "INSERT INTO images (link, hash, no_store, no_cache, expires, \
             etag, must_revalidate, retrieved_on) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             RETURNING id",
            &[
                &this_link,
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
        .map_err(map_ghf!(this_link))?[0]
        .get("id");
    trans.commit().map_err(map_ghf!(this_link))?;

    Ok((hash, image_id))
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
        .chain(fern::log_file("output.log").unwrap())
        .apply()
        .unwrap();
}

pub mod secrets {
    use failure::Error;
    use serde::Deserialize;
    use std::io::Read;

    #[derive(Debug, Deserialize)]
    pub struct Secrets {}

    pub fn load() -> Result<Secrets, Error> {
        let mut s = String::new();
        std::fs::File::open("secrets/secrets.toml")
            .map_err(Error::from)?
            .read_to_string(&mut s)
            .map_err(Error::from)?;
        toml::from_str::<Secrets>(&s).map_err(Error::from)
    }
}
