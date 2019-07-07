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
use reqwest::{Client, Url};
use serde_json::from_value;
use serde_json::{Deserializer, Value};
use std::collections::BTreeSet;
use std::fs::{remove_file, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::iter::Iterator;
use std::path::Path;

lazy_static! {
    static ref DB_POOL: r2d2::Pool<PostgresConnectionManager<NoTls>> = r2d2::Pool::new(
        PostgresConnectionManager::new(SECRETS.postgres.connect.parse().unwrap(), NoTls)
    )
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

fn ingest_json<R: Read + Send>(
    title: &str,
    mut already_have: BTreeSet<i64>,
    json_stream: R,
    min_skip: Option<i64>,
    max_skip: Option<i64>,
) {
    let json_iter = Deserializer::from_reader(json_stream).into_iter::<Value>();

    info!("Starting ingestion!");

    let check_json = Check::new(json_iter);

    let to_submission = |mut post: Value| -> Result<Option<Submission>, Error> {
        let promo = post["promoted"].take();
        if !promo.is_null() && from_value(promo).map_err(Error::from)? {
            return Ok(None);
        }
        let id: String = from_value(post["id"].take()).map_err(Error::from)?;
        Ok(Some(Submission {
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
        }))
    };

    let timeouts = std::sync::RwLock::new(std::collections::HashSet::<String>::new());

    check_json
        .filter_map(|post| {
            let post = to_submission(post).map_err(le!()).ok()??;
            if !post.is_self
                && (EXT_RE.is_match(&post.url)
                    || Url::parse(&post.url)
                        .ok()?
                        .domain()
                        .map(|d| is_host_special(d))
                        .unwrap_or(false))
                && !already_have.remove(&post.id_int)
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

            let post_url = match Url::parse(&post.url) {
                Ok(url) => url,
                Err(e) => {
                    warn!(
                        "{}: {}: {} is an invalid URL: {}",
                        title, post.id, post.url, e
                    );
                    return;
                }
            };

            if post_url
                .domain()
                .map(|domain| timeouts.read().unwrap().contains(domain))
                .unwrap_or(false)
            {
                warn!("{}: {}: {} has timed out before", title, post.id, post.url);
                return;
            }

            match save_hash(&post.url, HashDest::Images) {
                Ok((_hash, _hash_dest, image_id, exists)) => {
                    match save_post(&DB_POOL, &post, image_id) {
                        Ok(post_exists) => {
                            if !post_exists {
                                if exists {
                                    info!("{}: {}: {} already exists", title, post.id, post.url);
                                } else {
                                    info!(
                                        "{}: {}: {} successfully hashed",
                                        title, post.id, post.url
                                    );
                                }
                            } else {
                                warn!("{}: post ID {} already recorded", title, post.id);
                            }
                        }
                        Err(ue) => match ue.source {
                            Source::Internal => {
                                error!("{}: {}: {}", title, post.id, ue.error);
                                std::process::exit(1);
                            }
                            _ => {
                                warn!("{}: saving post ID {} failed: {}", title, post.id, ue.error)
                            }
                        },
                    }
                }
                Err(ue) => match ue.source {
                    Source::Internal => {
                        error!("{}: {}: {}", title, post.id, ue.error);
                        std::process::exit(1);
                    }
                    _ => {
                        if let Some(e) = ue.error.downcast_ref::<reqwest::Error>() {
                            if e.is_timeout() {
                                if let Ok(url) = Url::parse(&post.url) {
                                    if let Some(domain) = url.domain() {
                                        timeouts.write().unwrap().insert(domain.to_string());
                                    }
                                }
                            }
                        }
                        warn!("{}: {}: {} failed: {}", title, post.id, post.url, ue.error)
                    }
                },
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
            (@arg NO_SKIP_MONTHS: -M --("no-skip-months") "Don't skip past months we already have")
            (@arg NO_SKIP_IDS: -I --("no-skip-ids") "Don't skip past IDs we already have")
            (@arg PATHS: +required +multiple "The URLs or paths of the files to ingest")
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

        let month_f = f64::from(month);
        let year_f = f64::from(year);

        if !matches.is_present("NO_SKIP_MONTHS") {
            let (next_month, next_year) = if month == 12 {
                (1, year + 1)
            } else {
                (month + 1, year)
            };

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
                info!("Already have {:02}-{}", year, month);
                continue;
            }
        }

        let (min_skip, max_skip) = if matches.is_present("NO_SKIP_IDS") {
            (None, None)
        } else {
            let min_skip: Option<i64> = DB_POOL
                .get()
                .map_err(Error::from)?
                .query_iter(
                    "SELECT reddit_id_int FROM posts \
                     WHERE EXTRACT(MONTH FROM created_utc) = $1 \
                     AND EXTRACT(YEAR FROM created_utc) = $2 \
                     ORDER BY reddit_id_int ASC LIMIT 1",
                    &[&month_f, &year_f],
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
                     AND EXTRACT(YEAR FROM created_utc) = $2 \
                     ORDER BY reddit_id_int DESC LIMIT 1",
                    &[&month_f, &year_f],
                )
                .and_then(|mut q_i| q_i.next())
                .map(|row_opt| row_opt.map(|row| row.get("reddit_id_int")))
                .map_err(Error::from)?;

            (min_skip, max_skip)
        };

        let (input, arch_path): (Box<Read + Send>, _) =
            if path.starts_with("http://") || path.starts_with("https://") {
                let arch_path = std::env::var("HOME").map_err(Error::from)?
                    + "/archives/"
                    + Url::parse(&path)
                        .map_err(Error::from)?
                        .path_segments()
                        .ok_or_else(|| format_err!("cannot-be-a-base-url"))?
                        .next_back()
                        .ok_or_else(|| format_err!("no last path segment"))?;

                let arch_file = if Path::exists(Path::new(&arch_path)) {
                    info!("Found existing archive file");

                    OpenOptions::new()
                        .read(true)
                        .open(&arch_path)
                        .map_err(Error::from)?
                } else {
                    info!("Downloading archive file");
                    let mut arch_file = OpenOptions::new()
                        .create_new(true)
                        .read(true)
                        .write(true)
                        .open(&arch_path)
                        .map_err(Error::from)?;

                    io::copy(
                        &mut BufReader::new(REQW_CLIENT.get(&path).send().map_err(Error::from)?),
                        &mut arch_file,
                    )
                    .map_err(Error::from)?;

                    arch_file.seek(SeekFrom::Start(0)).map_err(Error::from)?;

                    arch_file
                };

                (Box::new(arch_file), Some(arch_path))
            } else {
                (Box::new(File::open(&path).map_err(Error::from)?), None)
            };

        info!("Processing posts we already have");

        let mut already_have = BTreeSet::new();

        DB_POOL
            .get()
            .map_err(Error::from)?
            .query_iter(
                "SELECT reddit_id_int FROM posts \
                 WHERE EXTRACT(month FROM created_utc) = $1 \
                 AND EXTRACT(year FROM created_utc) = $2",
                &[&month_f, &year_f],
            )
            .map_err(Error::from)?
            .for_each(|row| {
                already_have.insert(row.get(0));
                Ok(())
            })
            .map_err(Error::from)?;

        let already_have_len = already_have.len();
        info!(
            "Already have {} post{}",
            already_have_len,
            if already_have_len == 1 { "" } else { "s" }
        );

        let input = BufReader::new(input);

        let title = format!("{:02}-{}", month, year);

        if path.ends_with("bz2") {
            ingest_json(
                &title,
                already_have,
                bzip2::bufread::BzDecoder::new(input),
                min_skip,
                max_skip,
            );
        } else if path.ends_with("xz") {
            ingest_json(
                &title,
                already_have,
                xz2::bufread::XzDecoder::new(input),
                min_skip,
                max_skip,
            );
        } else if path.ends_with("zst") {
            ingest_json(
                &title,
                already_have,
                zstd::stream::read::Decoder::new(input).map_err(Error::from)?,
                min_skip,
                max_skip,
            );
        } else {
            error!("Unknown file extension in {}", path);
            continue;
        }

        if let Some(arch_path) = arch_path {
            remove_file(arch_path).map_err(Error::from)?;
        }

        info!("Done ingesting {}", &path);
    }

    Ok(())
}
