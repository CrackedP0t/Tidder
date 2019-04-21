use chrono::NaiveDateTime;
use common::{dhash, get_image, Hash, StatusFail};
use fallible_iterator::FallibleIterator;
use fern;
use futures::future::lazy;
use futures::{Future, Stream};
use hyper::{client::HttpConnector, Body, Client, StatusCode};
use hyper_tls::HttpsConnector;
use lazy_static::lazy_static;
use log::LevelFilter;
use log::{error, info, warn};
use postgres::{self, NoTls};
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use regex::Regex;
use serde::Deserialize;
use std::env;

#[derive(Deserialize, Debug)]
struct Post {
    author: String,
    created_utc: i64,
    #[serde(rename = "url")]
    link: String,
    #[serde(rename = "over_18")]
    nsfw: bool,
    permalink: String,
    score: i32,
    spoiler: bool,
    subreddit: String,
    title: String,
}

#[derive(Deserialize, Debug)]
struct PostDoc {
    #[serde(rename = "_source")]
    source: Post,
}

#[derive(Deserialize, Debug)]
struct Hits {
    hits: Vec<PostDoc>,
}

#[derive(Deserialize, Debug)]
struct PushShiftSearch {
    hits: Hits,
}

macro_rules! pe {
    () => {
        |e| error!("{}", fe!(e))
    };
}

macro_rules! fe {
    ($e:expr) => {
        if cfg!(feature = "error_lines") {
            format!("Error at line {}: {}", line!(), $e)
        } else {
            format!("Error: {}", $e)
        }
    };
    () => {
        move |e| fe!(e)
    };
}

fn print_err(e: String) {
    error!("{}", e);
}

fn image_type_map(mime: &str) -> Result<image::ImageFormat, &str> {
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

struct PostRow {
    author: String,
    created_utc: i64,
    hash: Option<Hash>,
    is_hashable: bool,
    link: String,
    nsfw: bool,
    permalink: String,
    reddit_id: String,
    score: i32,
    spoiler: bool,
    status_code: Option<StatusCode>,
    subreddit: String,
    title: String,
}

impl PostRow {
    fn from_post(
        post: Post,
        hash: Option<Hash>,
        reddit_id: String,
        status_code: Option<StatusCode>,
    ) -> PostRow {
        let is_hashable = hash.is_some();
        PostRow {
            author: post.author,
            created_utc: post.created_utc,
            hash,
            is_hashable,
            link: post.link,
            nsfw: post.nsfw,
            permalink: post.permalink,
            reddit_id,
            spoiler: post.spoiler,
            score: post.score,
            status_code,
            subreddit: post.subreddit,
            title: post.title,
        }
    }
}

struct Saver {
    post: Post,
    reddit_id: String,
}

impl Saver {
    fn save(self, hash: Option<Hash>, status_code: Option<StatusCode>) {
        let post = PostRow::from_post(self.post, hash, self.reddit_id, status_code);

        let client = DB_POOL
            .get()
            .map_err(pe!())
            .and_then(
                |mut client|
                client.transaction().map_err(pe!())
                    .and_then(|mut trans| {
                        trans.execute(
                            "INSERT INTO posts (reddit_id, link, permalink, is_hashable, hash, status_code, author, created_utc, score, subreddit, title, nsfw, spoiler) \
                             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
                            &[
                                &post.reddit_id,
                                &Some(post.link),
                                &post.permalink,
                                &post.is_hashable,
                                &post.hash,
                                &(post.status_code.map(|sc| sc.as_u16() as i16)),
                                &post.author,
                                &(NaiveDateTime::from_timestamp(post.created_utc, 0)),
                                &post.score,
                                &post.subreddit,
                                &post.title,
                                &post.nsfw,
                                &post.spoiler,
                            ],
                        ).map_err(pe!())
                    }))
            .map(|_| ())
            .unwrap_or_else(|_| ());
    }
}

lazy_static! {
    static ref DB_POOL: r2d2::Pool<PostgresConnectionManager<NoTls>> =
        r2d2::Pool::new(PostgresConnectionManager::new(
            "dbname=tidder host=/run/postgresql user=postgres"
                .parse()
                .unwrap(),
            NoTls,
        ))
        .unwrap();
    static ref ID_RE: Regex = Regex::new(r"/comments/([^/]+)/").unwrap();
    static ref HTTPS: HttpsConnector<HttpConnector> = HttpsConnector::new(4).unwrap();
}

fn download(size: u64) -> Result<(), ()> {
    tokio::run(lazy(move || {
        info!("Querying PushShift");
        let r_client = reqwest::Client::new();
        let search_resp = r_client
            .get("https://elastic.pushshift.io/rs/submissions/_search")
            .query(&[
                ("sort", "created_utc:desc"),
                ("size", size.to_string().as_str()),
                (
                    "_source",
                    "permalink,url,author,created_utc,subreddit,score,title,over_18,spoiler",
                ),
            ])
            .send()
            .map_err(pe!())?
            .error_for_status()
            .map_err(pe!())?;

        let search: PushShiftSearch = serde_json::from_reader(search_resp).map_err(pe!())?;

        for postdoc in search.hits.hits {
            let link = postdoc.source.link.clone();
            let permalink = &postdoc.source.permalink;

            let reddit_id = String::from(
                match ID_RE.captures(&permalink).and_then(|cap| cap.get(1)) {
                    Some(reddit_id) => reddit_id.as_str(),
                    None => {
                        error!("Couldn't find ID in {}", permalink);
                        continue;
                    }
                },
            );

            let mut db_client = DB_POOL.get().map_err(pe!())?;
            if db_client
                .query_iter(
                    "SELECT EXISTS(SELECT FROM posts WHERE reddit_id = $1)",
                    &[&reddit_id],
                )
                .map_err(pe!())?
                .next()
                .map_err(pe!())?
                .unwrap()
                .try_get::<_, bool>(0)
                .map_err(pe!())?
            {
                error!("{} already exists", permalink);
                continue;
            }

            let saver = Saver {
                reddit_id,
                post: postdoc.source,
            };

            let client = Client::builder().build::<_, Body>((*HTTPS).clone());
            tokio::spawn(get_image(client, link.clone()).then(move |res| match res {
                Ok((status, img)) => {
                    saver.save(Some(dhash(img)), Some(status));
                    info!("{} successfully hashed", link);
                    Ok(())
                }
                Err(e) => {
                    error!("{}", e);
                    let ie = e.error;
                    let status = match ie.downcast::<StatusFail>() {
                        Ok(se) => Some(se.status),
                        Err(_) => None,
                    };
                    saver.save(None, status);
                    Err(())
                }
            }));
        }

        info!("Reached end of link list");

        Ok(())
    }));

    Ok(())
}

fn main() {
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
        .level(LevelFilter::Warn)
        .level_for("hasher", LevelFilter::Info)
        .chain(std::io::stdout())
        .chain(fern::log_file("output.log").unwrap())
        .apply()
        .unwrap();
    let size = env::args().nth(1);
    if let Some(size) = size {
        if let Ok(size) = size.parse::<u64>() {
            match download(size) {
                Ok(()) => info!("Hashing completed successfully!"),
                Err(()) => error!("Hashing unsuccessful!"),
            }
        } else {
            println!("Size is not a valid u64");
        }
    } else {
        println!("Expected an argument");
    }
}
