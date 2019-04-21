use chrono::NaiveDateTime;
use common::{dhash, get_image, Hash, StatusFail};
use failure::Error;
use fern;
use futures::future::lazy;
use futures::sync::mpsc::{self, SendError, UnboundedSender};
use futures::{Future, Stream};
use hyper::{Body, Client, StatusCode};
use hyper_tls::HttpsConnector;
use log::LevelFilter;
use log::{error, info, warn};
use regex::Regex;
use serde::Deserialize;
use std::env;
use std::io::BufReader;
use tokio_postgres::NoTls;

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
struct Posts {
    hits: Hits,
}

macro_rules! pe {
    () => {
        |e| error!("{}", fe!(e))
    };
}

macro_rules! pef {
    ($s:expr) => {
        pef!($s,)
    };
    ($s:expr, $($fargs:expr),*) => {
        |e| {error!(concat!("Error at line {}: ", $s), line!(), $($fargs,)*); ()}
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
    tx: UnboundedSender<PostRow>,
}

impl Saver {
    fn save(
        self,
        hash: Option<Hash>,
        status_code: Option<StatusCode>,
    ) -> Result<(), SendError<PostRow>> {
        self.tx.unbounded_send(PostRow::from_post(
            self.post,
            hash,
            self.reddit_id,
            status_code,
        ))
    }
}

fn download(size: u64) -> Result<(), ()> {
    tokio::run(lazy(move || {
        info!("Querying PushShift");
        let ingest = BufReader::new(reqwest::get(
            format!("https://elastic.pushshift.io/rs/submissions/_search?sort=created_utc:desc&size={}&_source=permalink,url,author,created_utc,subreddit,score,title,over_18,spoiler", size).as_str()
        ).map_err(pe!())?);

        let posts: Posts = serde_json::from_reader(ingest).map_err(pe!())?;

        let https = HttpsConnector::new(4).map_err(pe!())?;

        let (tx, rx) = mpsc::unbounded();

        tokio::spawn(
            tokio_postgres::connect("postgres://postgres@%2Frun%2Fpostgresql/tidder", NoTls)
                .map_err(pe!())
                .and_then(|(mut client, conn)| {
                    tokio::spawn(conn.map_err(pe!()));

                    client.prepare(
                        "INSERT INTO posts (reddit_id, link, permalink, is_hashable, hash, status_code, author, created_utc, score, subreddit, title, nsfw, spoiler) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
                    )
                        .map_err(pe!())
                        .and_then(|stmt| {
                            rx.and_then(move |post: PostRow| {
                                client.build_transaction().build(
                                    client.execute(
                                        &stmt,
                                        &[&post.reddit_id,
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
                                          &post.spoiler
                                        ],
                                    ))
                                    .map_err(pe!())
                            })
                                .then(|_| Ok(()))
                                .for_each(|_| Ok(()))
                        })
                }));

        for postdoc in posts.hits.hits {
            let id_re: Regex = Regex::new(r"/comments/([^/]+)/").map_err(pe!())?;
            let link = postdoc.source.link.clone();
            let permalink = &postdoc.source.permalink;

            let reddit_id = String::from(
                match id_re.captures(&permalink).and_then(|cap| cap.get(1)) {
                    Some(reddit_id) => reddit_id.as_str(),
                    None => {
                        error!("Couldn't find ID in {}", permalink);
                        continue;
                    }
                },
            );

            let tx = tx.clone();

            let saver = Saver {
                tx,
                reddit_id,
                post: postdoc.source,
            };

            let client = Client::builder().build::<_, Body>(https.clone());
            tokio::spawn(get_image(client, link.clone()).then(move |res| {
                match res {
                    Ok((status, img)) => saver
                        .save(Some(dhash(img)), Some(status))
                        .map_err(pe!())
                        .map(|_| info!("{} successfully hashed", link)),
                    Err(e) => {
                        error!("{}", e);
                        let ie = e.error;
                        let status = match ie.downcast::<StatusFail>() {
                            Ok(se) => Some(se.status),
                            Err(_) => None,
                        };
                        saver.save(None, status).unwrap_or_else(pe!());
                        Err(())
                    }
                }
            }));
        }

        drop(tx);

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
