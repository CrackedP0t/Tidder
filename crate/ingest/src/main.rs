use clap::{clap_app, crate_authors, crate_description, crate_version};
use common::*;
use failure::{format_err, Error};
use fallible_iterator::FallibleIterator;
use lazy_static::lazy_static;
use log::{error, info, warn};
use postgres::NoTls;
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use rayon::prelude::*;
use regex::Regex;
use reqwest::{Client, StatusCode};
use serde_json::from_value;
use serde_json::{Deserializer, Value};
use std::fs::File;
use std::io::{BufReader, Read};
use std::iter::Iterator;

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

struct Check<I> {
    iter: I,
}

impl<I> Check<I> {
    fn new(iter: I) -> Check<I> {
        Check { iter }
    }
}

impl<I, T, E> Iterator for Check<I>
where
    I: Iterator<Item = Result<T, E>>,
    E: std::fmt::Display,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(res) => res.map(Some).map_err(le!()).unwrap_or(None),
            None => None,
        }
    }
}

fn ingest_json<R: Read + Send>(json_stream: R, min_skip: Option<i64>, max_skip: Option<i64>) {
    let json_iter = Deserializer::from_reader(json_stream).into_iter::<Value>();

    info!("Starting ingestion!");

    let check_json = Check::new(json_iter);

    let to_submission = |mut post: Value| -> Result<Submission, Error> {
        let id: String = from_value(post["id"].take()).map_err(Error::from)?;
        Ok(Submission {
            id_int: i64::from_str_radix(&id, 36)
                .map_err(|e| format_err!("Couldn't parse number from ID '{}': {}", &id, e))?,
            id,
            author: from_value(post["author"].take()).map_err(Error::from)?,
            created_utc: match post["created_utc"].take() {
                Value::Number(n) => n
                    .as_i64()
                    .ok_or_else(|| format_err!("'created_utc' is not a valid i64"))?,
                Value::String(n) => n
                    .parse()
                    .map_err(|e| format_err!("'created_utc' can't be parsed as an i64: {}", e))?,
                _ => return Err(format_err!("'created_utc' is not a number or string")),
            },
            is_self: from_value(post["is_self"].take()).map_err(Error::from)?,
            over_18: from_value(post["over_18"].take()).map_err(Error::from)?,
            permalink: from_value(post["permalink"].take()).map_err(Error::from)?,
            score: from_value(post["score"].take()).map_err(Error::from)?,
            spoiler: from_value(post["spoiler"].take()).map_err(Error::from)?,
            subreddit: from_value(post["subreddit"].take()).map_err(Error::from)?,
            title: from_value(post["title"].take()).map_err(Error::from)?,
            url: from_value(post["url"].take()).map_err(Error::from)?,
        })
    };

    check_json
        .filter_map(|post| {
            let post = to_submission(post).map_err(le!()).ok()?;

            if !post.is_self
                && EXT_RE.is_match(&post.url)
                && min_skip
                    .map(|min_skip| post.id_int < min_skip)
                    .unwrap_or(true)
                && max_skip
                    .map(|max_skip| post.id_int > max_skip)
                    .unwrap_or(true)
            {
                Some(post)
            } else {
                None
            }
        })
        .par_bridge()
        .for_each(|mut post: Submission| {
            post.url = post
                .url
                .replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">");
            match get_hash(&post.url, HashDest::Images) {
                Ok((_hash, image_id, exists)) => {
                    if exists {
                        info!("{} already exists", post.url);
                    } else {
                        info!("{} successfully hashed", post.url);
                    }
                    save_post(&DB_POOL, &post, image_id);
                }
                Err(ghf) => {
                    let msg = ghf.to_string();
                    let ie = ghf.error;
                    if let Ok(sf) = ie.downcast::<StatusFail>() {
                        if sf.status != StatusCode::NOT_FOUND {
                            warn!("{}", msg);
                        }
                    } else {
                        warn!("{}", msg);
                    }
                }
            }
        });
}

fn main() -> Result<(), Error> {
    lazy_static::lazy_static! {
        static ref REQW_CLIENT: Client = Client::new();
        static ref MONTH_RE: Regex = Regex::new(r"(\d\d)\..+$").unwrap();
        static ref YEAR_RE: Regex = Regex::new(r"\d\d\d\d").unwrap();
    }

    setup_logging();
    let matches = clap_app!(
        ingest =>
            (version: crate_version!())
            (author: crate_authors!(","))
            (about: crate_description!())
            (@group from +required =>
             (@arg PATHS: +multiple "The URL or path of the file to ingest")
            )
    )
    .get_matches();

    for path in matches.values_of_lossy("PATHS").unwrap() {
        info!("Ingesting {}", &path);

        let month: i32 = MONTH_RE
            .captures(&path)
            .and_then(|caps| caps.get(1))
            .ok_or_else(|| format_err!("couldn't find month in {}", path))
            .and_then(|m| m.as_str().parse().map_err(Error::from))?;

        let year: i32 = YEAR_RE
            .find(&path)
            .ok_or_else(|| format_err!("couldn't find year in {}", path))
            .and_then(|m| m.as_str().parse().map_err(Error::from))?;

        let (next_month, next_year) = if month == 12 {
            (1, year + 1)
        } else {
            (month + 1, year)
        };

        let month = f64::from(month);
        let year = f64::from(year);
        let next_month = f64::from(next_month);
        let next_year = f64::from(next_year);

        if DB_POOL
            .get()
            .map_err(Error::from)?
            .query_iter(
                "SELECT EXISTS(SELECT FROM posts \
                 WHERE EXTRACT(MONTH FROM created_utc) = $1 \
                 AND EXTRACT(YEAR FROM created_utc) = $2)",
                &[&next_month, &next_year],
            )
            .and_then(|mut q_i| q_i.next())
            .map(|row_opt| row_opt.map(|row| row.get::<usize, bool>(0)).unwrap())
            .map_err(Error::from)?
        {
            info!("Already have {}-{}", year, month);
            continue;
        }

        let min_skip: Option<i64> = DB_POOL
            .get()
            .map_err(Error::from)?
            .query_iter(
                "SELECT reddit_id_int FROM posts \
                 WHERE EXTRACT(MONTH FROM created_utc) = $1 \
                 AND EXTRACT(YEAR FROM created_utc) = $2 \
                 ORDER BY reddit_id_int ASC LIMIT 1",
                &[&month, &year],
            )
            .and_then(|mut q_i| q_i.next())
            .map(|row_opt| row_opt.map(|row| row.get("reddit_id_int")))
            .map_err(Error::from)?;

        let max_skip: Option<i64> = DB_POOL
            .get()
            .map_err(Error::from)?
            .query_iter(
                "SELECT reddit_id_int FROM posts \
                 WHERE EXTRACT(MONTH FROM created_utc) = $1 \
                 ORDER BY reddit_id_int DESC LIMIT 1",
                &[&month],
            )
            .and_then(|mut q_i| q_i.next())
            .map(|row_opt| row_opt.map(|row| row.get("reddit_id_int")))
            .map_err(Error::from)?;

        let input: BufReader<Box<Read + Send>> = BufReader::new(
            if path.starts_with("http://") || path.starts_with("https://") {
                Box::new(REQW_CLIENT.get(&path).send().map_err(Error::from)?)
            } else {
                Box::new(File::open(&path).map_err(Error::from)?)
            },
        );

        if path.ends_with("bz2") {
            ingest_json(bzip2::bufread::BzDecoder::new(input), min_skip, max_skip);
        } else if path.ends_with("xz") {
            ingest_json(xz2::bufread::XzDecoder::new(input), min_skip, max_skip);
        } else if path.ends_with("zst") {
            ingest_json(
                zstd::stream::read::Decoder::new(input).map_err(Error::from)?,
                min_skip,
                max_skip,
            );
        } else {
            error!("Unknown file extension in {}", path);
            continue;
        }

        info!("Done ingesting {}", &path);
    }

    Ok(())
}
