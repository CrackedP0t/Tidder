#![recursion_limit = "128"]

use clap::{clap_app, crate_authors, crate_description, crate_version};
use common::*;
use future::poll_fn;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use futures::task::Poll;
use log::{error, info, warn};
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Deserializer;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::error::Error as _;
use std::fs::{remove_file, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::iter::Iterator;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock, TryLockError};
use tokio::executor::{DefaultExecutor, Executor};
use url::Url;

struct CheckIter<I> {
    iter: I,
}

impl<I> CheckIter<I> {
    fn new(iter: I) -> CheckIter<I> {
        CheckIter { iter }
    }
}

impl<I, T, E> Iterator for CheckIter<I>
where
    I: Iterator<Item = Result<T, E>>,
    E: std::fmt::Display,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                None => return None,
                Some(Err(e)) => {
                    warn!("{}", e);
                    continue;
                }
                Some(Ok(v)) => return Some(v),
            }
        }
    }
}

async fn ingest_post(
    mut post: Submission,
    verbose: bool,
    blacklist: &RwLock<HashSet<String>>,
    in_flight: &RwLock<HashMap<String, u32>>,
) {
    post.url = post
        .url
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">");

    let post_info = format!("{}: {}: {}", post.created_utc, post.id, post.url);

    if verbose {
        info!("{} starting to hash", post_info);
    }

    let post_url_res = (|| {
        if CONFIG.banned.iter().any(|banned| banned.matches(&post.url)) {
            return Err(ue_save!("banned", "banned"));
        }

        let post_url = Url::parse(&post.url).map_err(map_ue_save!("invalid URL", "url_invalid"))?;

        let blacklist_guard = blacklist.read().unwrap();
        if post_url
            .host_str()
            .map(|host| blacklist_guard.contains(host))
            .unwrap_or(false)
        {
            return Err(ue_save!("blacklisted", "blacklisted"));
        }
        drop(blacklist_guard);

        Ok(post_url)
    })();

    let save_res = match post_url_res {
        Ok(post_url) => {
            let host = post_url.host_str().unwrap();

            let custom_limit: Option<&Option<_>> = CONFIG.custom_limits.get(host);

            poll_fn(|context| {
                let guard = in_flight.read().unwrap();
                let limit = match custom_limit {
                    None => Some(CONFIG.in_flight_limit),
                    Some(&Some(limit)) => Some(limit),
                    Some(&None) => None,
                };

                let ready = limit
                    .map(|limit| {
                        guard
                            .get(host)
                            .map(|in_flight| *in_flight < limit)
                            .unwrap_or(true)
                    })
                    .unwrap_or(true);

                if ready {
                    drop(guard);
                    let mut write_guard = in_flight.write().unwrap();
                    *(write_guard.entry(host.to_owned()).or_insert(0)) += 1;
                    drop(write_guard);
                    Poll::Ready(host.to_owned())
                } else {
                    drop(guard);
                    context.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;

            let res = save_hash(&post.url, HashDest::Images).await;

            *in_flight.write().unwrap().get_mut(host).unwrap() -= 1;

            res
        }
        Err(e) => Err(e),
    };

    let image_id = match save_res {
        Ok(hash_gotten) => {
            info!("{} successfully hashed", post_info);

            Ok(hash_gotten.id)
        }
        Err(ue) => match ue.source {
            Source::Internal => {
                error!(
                    "{}{}{}\n{:#?}\n{:#?}",
                    ue.file.unwrap_or(""),
                    ue.line
                        .map(|line| Cow::Owned(format!("#{}", line)))
                        .unwrap_or(Cow::Borrowed("")),
                    if ue.file.is_some() || ue.line.is_some() {
                        ": "
                    } else {
                        ""
                    },
                    ue.error,
                    post
                );
                std::process::exit(1)
            }
            _ => {
                let reqwest_save_error = ue.error.downcast_ref::<reqwest::Error>().and_then(|e| {
                    let hyper_error = e.source().and_then(|he| he.downcast_ref::<hyper::Error>());

                    if e.is_timeout() || hyper_error.is_some() {
                        if let Ok(url) = Url::parse(&post.url) {
                            if let Some(host) = url.host_str() {
                                if !CONFIG.no_blacklist.iter().any(|n| host.ends_with(n)) {
                                    blacklist.write().unwrap().insert(host.to_string());
                                }
                            }
                        }
                    }

                    e.status()
                        .map(|status| format!("http_{}", status.as_str()).into())
                        .or_else(|| {
                            if e.is_timeout() {
                                Some("timeout".into())
                            } else {
                                None
                            }
                        })
                        .or_else(|| hyper_error.map(|_| "hyper".into()))
                });

                let save_error = ue.save_error.or(reqwest_save_error);

                warn!(
                    "{} failed{}: {}",
                    post_info,
                    save_error
                        .as_ref()
                        .map(|se| Cow::Owned(format!(" ({})", se)))
                        .unwrap_or_else(|| Cow::Borrowed("")),
                    ue.error
                );

                Err(save_error)
            }
        },
    };

    match post.save(image_id).await {
        Ok(_) => {
            if verbose {
                info!("{} successfully saved", post_info);
            }
        }
        Err(e) => {
            error!("{} failed to save: {:?}", post_info, e);
            std::process::exit(1);
        }
    }
}

async fn ingest_json<R: Read + Send + 'static>(
    verbose: bool,
    mut already_have: Option<BTreeSet<i64>>,
    json_stream: R,
) {
    const MAX_SPAWNED: u32 = 256;

    let json_iter = Deserializer::from_reader(json_stream)
        .into_iter::<Submission>()
        .map(|res| res.map_err(map_ue!()).and_then(|sub| sub.finalize()));

    let json_iter = CheckIter::new(json_iter).filter(move |post| {
        !post.is_self
            && post.promoted.map(|promoted| !promoted).unwrap_or(true)
            && ((EXT_RE.is_match(&post.url) && URL_RE.is_match(&post.url))
                || is_link_special(&post.url))
            && match already_have {
                None => true,
                Some(ref mut set) => {
                    let had = set.remove(&post.id_int);
                    if set.is_empty() {
                        already_have = None;
                    }
                    !had
                }
            }
    });

    let blacklist = Arc::new(RwLock::new(HashSet::<String>::new()));
    let in_flight = Arc::new(RwLock::new(HashMap::<String, u32>::new()));
    let json_iter = Arc::new(Mutex::new(json_iter));

    info!("Starting ingestion!");

    (0..MAX_SPAWNED)
        .map(|_i| {
            let blacklist = blacklist.clone();
            let in_flight = in_flight.clone();
            let json_iter = json_iter.clone();

            (&mut DefaultExecutor::current() as &mut dyn Executor)
                .spawn_with_handle(Box::pin(async move {
                    while let Some(post) = {
                        poll_fn(|context| match json_iter.try_lock() {
                            Ok(mut guard) => {
                                let post = guard.next();
                                drop(guard);
                                Poll::Ready(post)
                            }
                            Err(TryLockError::WouldBlock) => {
                                context.waker().wake_by_ref();
                                Poll::Pending
                            }
                            Err(poison_error) => panic!("{}", poison_error),
                        })
                        .await
                    } {
                        ingest_post(post, verbose, &blacklist, &in_flight).await;
                    }
                }))
                .unwrap()
        })
        .collect::<FuturesUnordered<_>>()
        .map(|_| Ok(()))
        .forward(futures::sink::drain())
        .await
        .unwrap();
}

#[tokio::main]
async fn main() -> Result<(), UserError> {
    static MONTH_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"-(\d\d)").unwrap());
    static YEAR_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\d\d\d\d").unwrap());

    setup_logging!();
    let matches = clap_app!(
        ingest =>
            (version: crate_version!())
            (author: crate_authors!(","))
            (about: crate_description!())
            (@arg NO_DELETE: -D --("no-delete") "Don't delete archive files when done")
            (@arg PATH: +required "The URL or path of the file to ingest")
            (@arg VERBOSE: -v --verbose "Print out each step in processing an image")
    )
    .get_matches();

    let no_delete = matches.is_present("NO_DELETE");
    let path = matches.value_of("PATH").unwrap().to_string();
    let verbose = matches.is_present("VERBOSE");

    let month: i32 = MONTH_RE
        .captures(&path)
        .and_then(|caps| caps.get(1))
        .ok_or_else(|| ue!(format!("couldn't find month in {}", path)))
        .and_then(|m| m.as_str().parse().map_err(map_ue!()))?;

    let year: i32 = YEAR_RE
        .find(&path)
        .ok_or_else(|| ue!(format!("couldn't find year in {}", path)))
        .and_then(|m| m.as_str().parse().map_err(map_ue!()))?;

    let month_f = f64::from(month);
    let year_f = f64::from(year);

    info!("Ingesting {}", path);

    let (input_file, arch_path): (File, _) =
        if path.starts_with("http://") || path.starts_with("https://") {
            let arch_path = std::env::var("HOME")?
                + "/archives/"
                + Url::parse(&path)?
                    .path_segments()
                    .ok_or_else(|| ue!("cannot-be-a-base-url"))?
                    .next_back()
                    .ok_or_else(|| ue!("no last path segment"))?;

            let arch_file = if Path::exists(Path::new(&arch_path)) {
                info!("Found existing archive file");

                OpenOptions::new().read(true).open(&arch_path)?
            } else {
                info!("Downloading archive file");
                let mut arch_file = OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .open(&arch_path)?;

                let mut resp = REQW_CLIENT.get(&path).send().await?.error_for_status()?;

                while let Some(chunk) = resp.chunk().await? {
                    arch_file.write_all(&chunk)?;
                }

                arch_file.seek(SeekFrom::Start(0))?;

                arch_file
            };

            (arch_file, Some(arch_path))
        } else {
            (File::open(&path)?, None)
        };

    info!("Processing posts we already have");

    let client = PG_POOL.take().await?;
    let stmt = client
        .prepare(
            "SELECT reddit_id_int FROM posts \
             WHERE EXTRACT(month FROM created_utc) = $1 \
             AND EXTRACT(year FROM created_utc) = $2",
        )
        .await?;

    let already_have = client
        .query(&stmt, &[&month_f, &year_f])
        .await?
        .into_iter()
        .fold(BTreeSet::new(), move |mut already_have, row| {
            already_have.insert(row.get(0));
            already_have
        });

    drop(client);

    let already_have_len = already_have.len();
    info!(
        "Already have {} post{}",
        already_have_len,
        if already_have_len == 1 { "" } else { "s" }
    );

    let already_have = if already_have_len > 0 {
        Some(already_have)
    } else {
        None
    };

    let input = BufReader::new(input_file);

    if path.ends_with("bz2") {
        ingest_json(verbose, already_have, bzip2::bufread::BzDecoder::new(input)).await;
    } else if path.ends_with("xz") {
        ingest_json(verbose, already_have, xz2::bufread::XzDecoder::new(input)).await;
    } else if path.ends_with("zst") {
        ingest_json(
            verbose,
            already_have,
            zstd::stream::read::Decoder::new(input)?,
        )
        .await;
    } else {
        ingest_json(verbose, already_have, input).await;
    };

    if !no_delete {
        if let Some(arch_path) = arch_path {
            remove_file(arch_path)?;
        }
    }

    info!("Done ingesting {}", &path);

    Ok(())
}
