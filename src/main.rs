#![allow(dead_code)]

use futures::future::{lazy, loop_fn, ok, Loop};
use futures::sync::mpsc::{self, SendError, UnboundedSender};
use futures::{Future, Stream};
use hyper::{header, Body, Client, Request};
use hyper_tls::HttpsConnector;
use image::{imageops, load_from_memory, DynamicImage};
use regex::Regex;
use serde::Deserialize;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::BufReader;
use tokio_postgres::{to_sql_checked, types, NoTls};

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

// fn fe<E>(e: E) -> String
// where
//     E: fmt::Display,
// {
//     format!("Error: {}", e)
// }

// fn fel<E>(line: u32, e: E) -> String
// where
//     E: fmt::Display,
// {
//     format!("Error at line {}: {}", line, e)
// }

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
    // ($e:expr) => {};
    () => {
        |e| fe!(e)
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
    is_link: bool,
    is_hashable: bool,
    hash: Option<Hash>,
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
    fn save_errored(self) -> Result<(), SendError<PostRow>> {
        self.tx.unbounded_send(PostRow {
            reddit_id: self.reddit_id,
            link: self.link,
            permalink: self.permalink,
            is_link: true,
            is_hashable: false,
            hash: None,
        })
    }

    fn save(self, hash: Hash) -> Result<(), SendError<PostRow>> {
        self.tx.unbounded_send(PostRow {
            reddit_id: self.reddit_id,
            link: self.link,
            permalink: self.permalink,
            is_link: true,
            is_hashable: true,
            hash: Some(hash),
        })
    }
}

fn download() -> Result<(), ()> {
    tokio::run(lazy(|| {
        let data_file = BufReader::new(File::open("aww.json").map_err(fe!())?);

        let posts: Posts = serde_json::from_reader(data_file).map_err(fe!())?;

        let https = HttpsConnector::new(4).map_err(fe!())?;

        let (tx, rx) = mpsc::unbounded();

        tokio::spawn(
            tokio_postgres::connect("postgres://postgres@%2Frun%2Fpostgresql/tidder", NoTls)
                .map_err(pe!())
                .and_then(|(mut client, conn)| {
                    tokio::spawn(conn.map_err(pe!()));

                    client
                        .prepare(
                            "INSERT INTO posts (reddit_id, link, permalink, is_link, is_hashable, hash) \
                             VALUES ($1, $2, $3, $4, $5, $6)",
                        )
                        .map_err(pe!())
                        .and_then(|stmt| {
                            rx.and_then(move |post: PostRow| {
                                client
                                    .execute(
                                        &stmt,
                                        &[&post.reddit_id, &Some(post.link), &post.permalink, &post.is_link, &post.is_hashable, &post.hash],
                                    )
                                    .map_err(pe!())
                                    .map(|_| {})
                            })
                                .for_each(|_| Ok(()))
                        })
                }));

        for postdoc in posts.hits.hits {
            let id_re: Regex = Regex::new(r"^/r/(?:[^/]+)/comments/([^/]+)/").map_err(fe!())?;
            let link = postdoc.source.link;
            let permalink = postdoc.source.permalink;

            let reddit_id = String::from(
                id_re
                    .captures(&permalink)
                    .ok_or(fef!("Couldn't find ID"))?
                    .get(1)
                    .ok_or(fef!("Couldn't find ID"))?
                    .as_str(),
            );

            let saver = Saver {
                tx: tx.clone(),
                reddit_id: reddit_id.clone(),
                link: link.clone(),
                permalink: permalink.clone(),
            };

            let saver2 = saver.clone();

            let client = Client::builder().build::<_, hyper::Body>((&https).clone());

            tokio::spawn(
                loop_fn(None, move |this_link: Option<String>| {
                    let this_link = this_link.unwrap_or_else(|| link.clone());

                    let request = Request::get(&this_link)
                        .header(header::ACCEPT, IMAGE_MIMES.join(","))
                        .body(Body::empty())
                        .map_err(pe!()).unwrap();

                    client
                        .clone()
                        .request(request)
                        .map_err(fe!())
                        .and_then(move |res| {
                            let status = res.status();
                            if status.is_success() {
                                match res.headers().get(header::CONTENT_TYPE) {
                                    Some(ctype) => {
                                        let val = ctype.to_str().map_err(fe!())?;
                                        if IMAGE_MIMES.iter().any(|t| *t == val) {
                                            Ok(Loop::Break(res))
                                        } else {
                                            Err(fef!(
                                                "{} sent unsupported file format {}",
                                                this_link, val
                                            ))
                                        }
                                    },
                                    None => Ok(Loop::Break(res)),
                                }
                            } else if status.is_redirection() {
                                Ok(Loop::Continue(
                                    Some(String::from(
                                        res.headers()
                                            .get(header::LOCATION)
                                            .ok_or_else(||
                                                        fef!("{} redirected without location", this_link))?
                                            .to_str()
                                            .map_err(fe!())?,
                                    )),
                                ))
                            } else {
                                Err(fef!("{} sent status {}", this_link, status))
                            }
                        })
                })
                    .and_then(|res| {
                        let (parts, body) = res.into_parts();
                        (ok(parts), body.concat2().map_err(fe!()))
                    })
                    .and_then(move |(_parts, body)| {
                        let img = load_from_memory(&body).map_err(fe!())?;

                        saver.save(dhash(img)).map_err(fe!())
                    })
                    .map_err(|e| {
                        saver2
                            .save_errored()
                            .unwrap_or_else(pe!());
                        print_err(e);
                    }),
            );
        }

        drop(tx);
        Ok::<(), String>(())
    }).map_err(print_err)
    );

    Ok(())
}

fn main() {
    // let mut res = reqwest::get("https://elastic.pushshift.io/rs/submissions/_search?q=subreddit:aww%20AND%20url:jpg&size=500&sort=score:desc&pretty=true").unwrap();
    // let mut out_file = std::fs::OpenOptions::new()
    //     .write(true)
    //     .open("aww.json")
    //     .unwrap();
    // std::io::copy(&mut res, &mut out_file).unwrap();

    download().unwrap();
}
