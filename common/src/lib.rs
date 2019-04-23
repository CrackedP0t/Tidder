use chrono::NaiveDateTime;
use failure::{format_err, Error, Fail};
use futures::future::{loop_fn, ok, result, Loop};
use futures::{Future, Stream};
use hyper::client::connect::Connect;
use hyper::{header, Body, Client, Request, StatusCode};
use image::{imageops, load_from_memory, DynamicImage};
use log::error;
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use regex::Regex;
use serde::Deserialize;
use std::fmt;
use tokio_postgres::{to_sql_checked, types, NoTls};
use url::percent_encoding::{utf8_percent_encode, QUERY_ENCODE_SET};

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
    hash: Option<Hash>,
    status_code: Option<StatusCode>,
) {
    let id_re = Regex::new(r"/comments/([^/]+)/").map_err(le!()).unwrap();

    let reddit_id = String::from(
        match id_re.captures(&post.permalink).and_then(|cap| cap.get(1)) {
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
                        "INSERT INTO posts (reddit_id, link, permalink, is_hashable, hash, status_code, author, created_utc, score, subreddit, title, nsfw, spoiler, is_self) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
                        &[
                            &reddit_id,
                            &post.url,
                            &post.permalink,
                            &hash.is_some(),
                            &hash,
                            &(status_code.map(|sc| sc.as_u16() as i16)),
                            &post.author,
                            &(NaiveDateTime::from_timestamp(post.created_utc, 0)),
                            &post.score,
                            &post.subreddit,
                            &post.title,
                            &post.over_18,
                            &post.spoiler.unwrap_or(false),
                            &post.is_self
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

pub fn image_type_map(mime: &str) -> Result<image::ImageFormat, &str> {
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
        _ => Err("Unsupported image format"),
    }
}

pub fn get_image<C>(
    client: Client<C, Body>,
    link: String,
) -> impl Future<
    Item = (
        DynamicImage,
        StatusCode,
        // Option<String>, // Etag
        // Option<NaiveDateTime>, // Date
        // Option<NaiveDateTime>, // Expires
    ),
    Error = GetImageFail,
>
where
    C: 'static + Connect,
{
    let link2 = link.clone();
    let map_gif = |e| GetImageFail {
        link: link2,
        error: e,
    };
    loop_fn((client, link), move |(client, this_link): (_, String)| {
        result(
            Request::get(utf8_percent_encode(&this_link, QUERY_ENCODE_SET).collect::<String>())
                .header(header::ACCEPT, IMAGE_MIMES.join(","))
                .header(
                    header::USER_AGENT,
                    "Mozilla/5.0 (X11; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0",
                )
                .body(Body::empty()),
        )
        .map_err(Error::from)
        .and_then(|request| {
            client
                .request(request)
                .map_err(Error::from)
                .and_then(move |resp| {
                    let status = resp.status();
                    if status.is_success() {
                        match resp.headers().get(header::CONTENT_TYPE) {
                            Some(ctype) => {
                                let val = ctype.to_str().map_err(Error::from)?;
                                if IMAGE_MIMES.iter().any(|t| *t == val) {
                                    Ok(Loop::Break(resp))
                                } else {
                                    Err(lfe!("Got unsupported MIME type {}", val))
                                }
                            }
                            None => Ok(Loop::Break(resp)),
                        }
                    } else if status.is_redirection() {
                        Ok(Loop::Continue((
                            client,
                            String::from(
                                resp.headers()
                                    .get(header::LOCATION)
                                    .ok_or_else(|| lfe!("Redirected without location"))?
                                    .to_str()
                                    .map_err(Error::from)?,
                            ),
                        )))
                    } else {
                        Err(Error::from(StatusFail { status }))
                    }
                })
        })
    })
    .and_then(|resp| {
        // let resp_time = Utc::now().naive_utc();
        let (parts, body) = resp.into_parts();
        // let headers = parts.headers;

        // enum CacheDirective {
        //     NoCache,
        //     NoStore,
        //     NoTransform,
        //     OnlyIfCached,
        //     MaxAge(u32),
        //     MaxStale(u32),
        //     MinFresh(u32),
        //     MustRevalidate,
        //     Public,
        //     Private,
        //     ProxyRevalidate,
        //     SMaxAge(u32),
        //     Extension(String, Option<String>),
        // };

        (body.concat2().map_err(Error::from), ok(parts.status))
    })
    .and_then(move |(body, status)| (load_from_memory(&body).map_err(Error::from), ok(status)))
    .map_err(map_gif)
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
        .level_for("hasher", log::LevelFilter::Info)
        .chain(std::io::stderr())
        .chain(
            fern::log_file("output.log")
                .map_err(|e| eprintln!("{}", e))
                .unwrap(),
        )
        .apply()
        .map_err(|e| eprintln!("{}", e))
        .unwrap();
}
