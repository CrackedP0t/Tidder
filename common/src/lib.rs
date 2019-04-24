use chrono::NaiveDateTime;
use failure::{format_err, Fail};
use image::{imageops, load_from_memory, DynamicImage};
use lazy_static::lazy_static;
use log::error;
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use regex::Regex;
use reqwest::{header, StatusCode};
use serde::Deserialize;
use std::fmt;
use std::io::{BufReader, Read};
use tokio_postgres::{to_sql_checked, types, NoTls};
use toml;
use url::percent_encoding::{utf8_percent_encode, QUERY_ENCODE_SET};

pub use failure::{self, Error};

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
    hash: Hash,
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
                        "INSERT INTO posts (reddit_id, link, permalink, hash, author, created_utc, score, subreddit, title, nsfw, spoiler) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                        &[
                            &reddit_id,
                            &post.url,
                            &post.permalink,
                            &hash,
                            &post.author,
                            &(NaiveDateTime::from_timestamp(post.created_utc, 0)),
                            &post.score,
                            &post.subreddit,
                            &post.title,
                            &post.over_18,
                            &post.spoiler.unwrap_or(false),
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
pub struct GetImageFail {
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

pub fn image_type_map(mime: &str) -> Result<image::ImageFormat, Error> {
    use image::ImageFormat::*;
    match mime {
        "image/png" => Ok(PNG),
        "image/jpeg" => Ok(JPEG),
        "image/gif" => Ok(GIF),
        "image/webp" => Ok(WEBP),
        "image/x-portable-anymap" => Ok(PNM),
        "image/tiff" => Ok(TIFF),
        "image/x-targa" | "image/x-tga" => Ok(TGA),
        "image/bmp" => Ok(BMP),
        "image/vnd.microsoft.icon" => Ok(ICO),
        "image/vnd.radiance" => Ok(HDR),
        _ => Err(format_err!("Unsupported MIME type {}", mime)),
    }
}

macro_rules! map_gif {
    ($link:expr) => {
        |e| GetImageFail {
            link: $link.clone(),
            error: Error::from(e),
        }
    };
}

pub fn get_image(
    link: String,
) -> Result<
    // (
    DynamicImage,
    // Option<String>, // Etag
    // Option<NaiveDateTime>, // Date
    // Option<NaiveDateTime>, // Expires
    // ),
    GetImageFail,
> {
    lazy_static! {
        static ref REQW_CLIENT: reqwest::Client = reqwest::Client::new();
    }

    let mut this_link = link;

    let resp = loop {
        let resp = REQW_CLIENT
            .get(&utf8_percent_encode(&this_link, QUERY_ENCODE_SET).collect::<String>())
            .header(header::ACCEPT, IMAGE_MIMES.join(","))
            .header(
                header::USER_AGENT,
                "Mozilla/5.0 (X11; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0",
            )
            .send()
            .map_err(map_gif!(this_link))?;

        let status = resp.status();

        if status.is_success() {
            match resp.headers().get(header::CONTENT_TYPE) {
                Some(ctype) => {
                    let val = ctype.to_str().map_err(map_gif!(this_link))?;
                    if IMAGE_MIMES.iter().any(|t| *t == val) {
                        break resp;
                    } else {
                        return Err(GetImageFail {
                            link: this_link.clone(),
                            error: format_err!("Got unsupported MIME type {}", val),
                        });
                    };
                }
                None => {
                    break resp;
                }
            }
        } else if status.is_redirection() {
            this_link = String::from(
                resp.headers()
                    .get(header::LOCATION)
                    .ok_or_else(|| GetImageFail {
                        link: this_link.clone(),
                        error: format_err!("Redirected without location"),
                    })?
                    .to_str()
                    .map_err(map_gif!(this_link))?,
            );
            continue;
        } else {
            return Err(GetImageFail {
                link: this_link,
                error: Error::from(StatusFail { status }),
            });
        }
    };

    let headers = resp.headers();

    let mut file = Vec::<u8>::with_capacity(
        headers
            .get(header::CONTENT_LENGTH)
            .and_then(|c_l| std::str::from_utf8(c_l.as_bytes()).ok())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(2048),
    );
    BufReader::new(resp)
        .read_to_end(&mut file)
        .map_err(map_gif!(this_link))?;

    load_from_memory(&file).map_err(map_gif!(this_link))
}

pub fn setup_logging() {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d %H:%M:%S]"),
                record.target(),
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
                },
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Warn)
        .level_for("ingest", log::LevelFilter::Info)
        .level_for("common", log::LevelFilter::Info)
        // .chain(
        //     fern::Dispatch::new()
        //         .filter(|metadata| {
        //             // Only show 'Info' messages on the terminal
        //             metadata.level() == log::LevelFilter::Info
        //         })
        .chain(std::io::stderr())
        // )
        .chain(fern::log_file("output.log").unwrap())
        .apply()
        .unwrap();
}

pub mod secrets {
    use failure::Error;
    use serde::Deserialize;
    use std::io::Read;

    #[derive(Debug, Deserialize)]
    pub struct BigQuery {
        pub key_file: String,
    }

    #[derive(Debug, Deserialize)]
    pub struct Secrets {
        pub bigquery: BigQuery,
    }

    pub fn load() -> Result<Secrets, Error> {
        let mut s = String::new();
        std::fs::File::open("secrets/secrets.toml")
            .map_err(Error::from)?
            .read_to_string(&mut s)
            .map_err(Error::from)?;
        toml::from_str::<Secrets>(&s).map_err(Error::from)
    }
}
