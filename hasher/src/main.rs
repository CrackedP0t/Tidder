use common::{dhash, get_image, Hash, StatusFail};
use failure::Error;
use futures::future::lazy;
use futures::sync::mpsc::{self, SendError, UnboundedSender};
use futures::{Future, Stream};
use hyper::{Body, Client, StatusCode};
use hyper_tls::HttpsConnector;
use regex::Regex;
use serde::Deserialize;
use std::io::BufReader;
use tokio_postgres::NoTls;

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

            let client = Client::builder().build::<_, Body>(https.clone());
            tokio::spawn(
                    get_image(client, link.clone())
                    .then(|res| {
                        match res {
                            Ok((status, img)) => saver.save(dhash(img), Some(status)).map_err(fe!()).map(|_| eprintln!("{} successfully hashed", link)).map_err(pe!()),
                            Err(e) => {
                                let (e, status) = match e.downcast::<StatusFail>() {
                                    Ok(se) => {
                                        let status = se.status;
                                        (Error::from(se), Some(status))
                                    },
                                    Err(e) => (e, None)
                                };
                                saver
                                    .save_errored(status)
                                    .unwrap_or_else(pe!());
                                eprintln!("{}", e.context(link));
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
