#![allow(dead_code)]

use futures::future::{lazy, loop_fn, ok, result, Loop};
use futures::sync::mpsc::{self, SendError, UnboundedSender};
use futures::{Future, Stream};
use hyper::{header, Body, Client, Request, StatusCode};
use hyper_tls::HttpsConnector;
use image::{imageops, load_from_memory, DynamicImage};
use regex::Regex;
use serde::Deserialize;
use std::error::Error;
use std::fmt;
// use std::fs::File;
use std::io::BufReader;
use tokio_postgres::{to_sql_checked, types, NoTls};
use url::percent_encoding::{utf8_percent_encode, QUERY_ENCODE_SET};

#[derive(Deserialize, Debug)]
struct Post {
    #[serde(rename = "url")]
    link: String,
    permalink: String,
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

#[derive(Debug)]
pub struct Hash(u64);

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
    ) -> Result<types::IsNull, Box<Error + Sync + Send>> {
        (self.0 as i64).to_sql(t, w)
    }

    fn accepts(t: &types::Type) -> bool {
        i64::accepts(t)
    }

    to_sql_checked!();
}

fn dhash(img: DynamicImage) -> Hash {
    let small_img = imageops::resize(&img.to_luma(), 9, 8, image::Triangle);

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

fn distance(a: Hash, b: Hash) -> u32 {
    (a.0 ^ b.0).count_ones()
}
macro_rules! pe {
    () => {
        |e| eprintln!("{}", fe!(e))
    };
}

macro_rules! fef {
    ($s:expr) => {
        fef!($s,)
    };
    ($s:expr, $($fargs:expr),*) => {
        if cfg!(feature = "error_lines") {
            format!(concat!("Error at line {}: ", $s), line!(), $($fargs,)*)
        } else {
            format!("Error: {}", $s)
        }
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
    eprintln!("{}", e);
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
    reddit_id: String,
    link: String,
    permalink: String,
    is_hashable: bool,
    hash: Option<Hash>,
    status_code: Option<StatusCode>,
}

const IMAGE_MIMES: [&str; 11] = [
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

#[derive(Clone)]
struct Saver {
    tx: UnboundedSender<PostRow>,
    reddit_id: String,
    link: String,
    permalink: String,
    // is_link: bool,
    // is_hashable: bool,
    // hash: Option<Hash>,
}

impl Saver {
    fn save_errored(self, status_code: Option<StatusCode>) -> Result<(), SendError<PostRow>> {
        self.tx.unbounded_send(PostRow {
            reddit_id: self.reddit_id,
            link: self.link,
            permalink: self.permalink,
            is_hashable: false,
            hash: None,
            status_code,
        })
    }

    fn save(self, hash: Hash, status_code: Option<StatusCode>) -> Result<(), SendError<PostRow>> {
        self.tx.unbounded_send(PostRow {
            reddit_id: self.reddit_id,
            link: self.link,
            permalink: self.permalink,
            is_hashable: true,
            hash: Some(hash),
            status_code,
        })
    }
}

macro_rules! sce {
    ($sc:expr) => {
        move |e| -> (String, Option<StatusCode>) { (fe!(e), Some($sc)) }
    };
    () => {
        move |e| -> (String, Option<StatusCode>) { (fe!(e), None) }
    };
}

macro_rules! scef {
    ($sc:expr, $s:expr) => {
        scnef!($sc, $s,)
    };
    ($sc:expr, $s:expr, $($fargs:expr),*) => {
        (fef!($s, $($fargs),*), Some($sc))
    };
}

macro_rules! scnef {
    ($s:expr) => {
        scnef!($s,)
    };
    ($s:expr, $($fargs:expr),*) => {
        (fef!($s, $($fargs),*), None)
    };
}

fn download() -> Result<(), ()> {
    tokio::run(lazy(|| {
        let ingest = BufReader::new(reqwest::get("https://elastic.pushshift.io/rs/submissions/_search?sort=created_utc:desc&size=5&_source=permalink,url").map_err(fe!())?);

        let posts: Posts = serde_json::from_reader(ingest).map_err(fe!())?;

        let https = HttpsConnector::new(4).map_err(fe!())?;

        let (tx, rx) = mpsc::unbounded();

        tokio::spawn(
            tokio_postgres::connect("postgres://postgres@%2Frun%2Fpostgresql/tidder", NoTls)
                .map_err(pe!())
                .and_then(|(mut client, conn)| {
                    tokio::spawn(conn.map_err(pe!()));

                    client.prepare(
                        "INSERT INTO posts (reddit_id, link, permalink, is_hashable, hash, status_code) \
                         VALUES ($1, $2, $3, $4, $5, $6)",
                    )
                        .map_err(pe!())
                        .and_then(|stmt| {
                            rx.and_then(move |post: PostRow| {
                                client.build_transaction().build(
                                    client.execute(
                                        &stmt,
                                        &[&post.reddit_id, &Some(post.link), &post.permalink, &post.is_hashable, &post.hash, &(post.status_code.map(|sc| sc.as_u16() as i16))],
                                    ))
                                    .map_err(pe!())
                            })
                                .then(|_| Ok(()))
                                .for_each(|_| Ok(()))
                        })
                }));

        for postdoc in posts.hits.hits {
            let id_re: Regex = Regex::new(r"/comments/([^/]+)/").map_err(fe!())?;
            let link = postdoc.source.link;
            let permalink = postdoc.source.permalink;

            let reddit_id = String::from(
                id_re
                    .captures(&permalink)
                    .ok_or(fef!("Couldn't find ID in {}", permalink))?
                    .get(1)
                    .ok_or(fef!("Couldn't find ID in {}", permalink))?
                    .as_str(),
            );

            let tx = tx.clone();

            let saver = Saver {
                tx,
                reddit_id,
                link: link.clone(),
                permalink
            };


            let client = Client::builder().build::<_, hyper::Body>(https.clone());

            tokio::spawn(
                loop_fn((client, link), move |(client, this_link)| {
                    let rel_abs_re = Regex::new(r"^/").unwrap();
                    let this_link = rel_abs_re.replace(this_link.as_str(), "https://reddit.com/").into_owned();
                    let tl2 = this_link.clone();

                    result(Request::get(
                        utf8_percent_encode(&this_link, QUERY_ENCODE_SET).to_string().as_str()
                    )
                           .header(header::ACCEPT, IMAGE_MIMES.join(","))
                           .header(header::USER_AGENT, "Mozilla/5.0 (X11; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0")
                           .body(Body::empty())
                           .map_err(sce!())).and_then(
                        |request| client
                            .request(request)
                            .map_err(move |e| scnef!("{} caused: {}", tl2, e))
                            .and_then(move |res| {
                                let status = res.status();
                                if status.is_success() {
                                    match res.headers().get(header::CONTENT_TYPE) {
                                        Some(ctype) => {
                                            let val = ctype.to_str().map_err(sce!())?;
                                            if IMAGE_MIMES.iter().any(|t| *t == val) {
                                                Ok(Loop::Break((this_link, res)))
                                            } else {
                                                Err(scnef!(
                                                    "{} sent unsupported MIME type {}",
                                                    this_link, val
                                                ))
                                            }
                                        },
                                        None => Ok(Loop::Break((this_link, res))),
                                    }
                                } else if status.is_redirection() {
                                    Ok(Loop::Continue(
                                        (client,
                                            String::from(
                                            res.headers()
                                                .get(header::LOCATION)
                                                .ok_or_else(||
                                                            scnef!("{} redirected without location", this_link))?
                                                .to_str()
                                                .map_err(sce!())?,
                                        )),
                                    ))
                                } else {
                                    Err(scef!(status, "{} sent status {}", this_link, status))
                                }
                            }))
                })
                    .and_then(|(this_link, resp)| {
                        let (parts, body) = resp.into_parts();
                        (ok(this_link), ok(parts.status), body.concat2().map_err(sce!(parts.status)))
                    })
                    .and_then(move |(this_link, status, body)| {
                        (ok(this_link), ok(status), load_from_memory(&body).map_err(sce!(status)))
                    })
                    .then(|res| {
                        match res {
                            Ok((this_link, status, img)) => saver.save(dhash(img), Some(status)).map_err(fe!()).map(|_| eprintln!("{} successfully hashed", this_link)).map_err(pe!()),
                            Err((e, status)) => {
                                saver
                                    .save_errored(status)
                                    .unwrap_or_else(pe!());
                                print_err(e);
                                Err(())
                            }
                        }
                    }),
            );
        }

        drop(tx);

        Ok(())
    }).map_err(print_err)
    );

    Ok(())
}

fn main() {
    download().unwrap();
}
