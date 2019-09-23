#![recursion_limit = "128"]

use clap::{clap_app, crate_authors, crate_description, crate_version};
use common::*;
use failure::{format_err, Error};
use lazy_static::lazy_static;
use log::{error, info, warn};
use regex::Regex;
use reqwest::r#async::Client;
use serde_json::Deserializer;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::fs::{remove_file, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::iter::FromIterator;
use std::iter::Iterator;
use std::path::Path;
use std::sync::{Arc, RwLock, TryLockError};
use tokio::prelude::*;
use url::Url;

use future::{err, ok, poll_fn};

pub enum Banned {
    TLD(&'static str),
    Host(&'static str),
    Full(&'static str),
}

impl Banned {
    pub fn matches(&self, url: &Url) -> bool {
        use Banned::*;
        match self {
            TLD(tld) => get_tld(url) == *tld,
            Host(host) => url
                .host_str()
                .map(|host_str| host_str == *host)
                .unwrap_or(false),
            Full(link) => url.as_str() == *link,
        }
    }
}

const BANNED: [Banned; 4] = [
    Banned::TLD("fbcdn.net"),
    Banned::TLD("livememe.com"),
    Banned::Full("http://i.imgur.com/JwhvGDV.jpg"),
    Banned::Full("http://i.imgur.com/4nmJMzR.jpg"),
];
const IN_FLIGHT_LIMIT: u32 = 1;
const NO_BLACKLIST: [&str; 1] = ["gifsound.com"];

lazy_static! {
    static ref CUSTOM_LIMITS: HashMap<&'static str, Option<u32>> = {
        let mut map = HashMap::new();
        map.insert("imgur.com", Some(3));
        map.insert("i.imgur.com", Some(7));
        map
    };
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
    mut already_have: Option<BTreeSet<i64>>,
    json_stream: R,
    verbose: bool,
) -> impl Future<Item = (), Error = ()> {
    let blacklist = Arc::new(RwLock::new(HashMap::<String, bool>::from_iter(
        NO_BLACKLIST.iter().map(|h| (h.to_string(), false)),
    )));

    let in_flight = Arc::new(RwLock::new(HashMap::<String, u32>::new()));

    const MAX_SPAWNED: u32 = 128;

    let all_spawned = Arc::new(RwLock::new(0u32));

    let json_iter = Deserializer::from_reader(json_stream)
        .into_iter::<Submission>()
        .map(|res| res.map_err(map_ue!()).and_then(|sub| sub.finalize()));

    let check_json = Check::new(json_iter);

    info!("Starting ingestion!");

    check_json
        .filter(|post| {
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
        })
        .for_each(|mut post: Submission| {
            let lazy_blacklist = blacklist.clone();
            let blacklist = blacklist.clone();

            let in_flight = in_flight.clone();
            let end_in_flight = in_flight.clone();

            while {
                let all_spawned_lock = all_spawned.read().unwrap();
                let maxed_out = *all_spawned_lock >= MAX_SPAWNED;
                drop(all_spawned_lock);
                maxed_out
            } {
                std::thread::sleep(std::time::Duration::from_micros(1));
            }

            let mut all_spawned_lock = all_spawned.write().unwrap();
            *all_spawned_lock += 1;
            drop(all_spawned_lock);

            let end_all_spawned = all_spawned.clone();

            tokio::spawn(
                future::lazy(move || {
                    let blacklist = lazy_blacklist;
                    post.url = post
                        .url
                        .replace("&amp;", "&")
                        .replace("&lt;", "<")
                        .replace("&gt;", ">");

                    let post_url = match Url::parse(&post.url) {
                        Ok(url) => url,
                        Err(e) => {
                            warn!(
                                "{}: {}: {} is invalid: {}",
                                post.created_utc, post.id, post.url, e
                            );
                            return err(post);
                        }
                    };

                    if verbose && BANNED.iter().any(|banned| banned.matches(&post_url)) {
                        warn!("{}: {}: {} is banned", post.created_utc, post.id, post.url);
                        return err(post);
                    }

                    let blacklist_guard = blacklist.read().unwrap();
                    if post_url
                        .host_str()
                        .and_then(|host| blacklist_guard.get(host).copied())
                        .unwrap_or(false)
                    {
                        if verbose {
                            warn!(
                                "{}: {}: {} is blacklisted",
                                post.created_utc, post.id, post.url
                            );
                        }
                        return err(post);
                    }
                    drop(blacklist_guard);

                    ok((post_url, post))
                })
                .and_then(move |(post_url, post)| {
                    poll_fn(move || {
                        let tld = get_tld(&post_url);
                        match in_flight.try_read() {
                            Ok(guard) => {
                                let custom_limit: Option<&Option<_>> = post_url
                                    .host_str()
                                    .and_then(|host| CUSTOM_LIMITS.get(&host));

                                let limit = match custom_limit {
                                    None => Some(IN_FLIGHT_LIMIT),
                                    Some(&Some(limit)) => Some(limit),
                                    Some(&None) => None,
                                };

                                let ready = limit
                                    .map(|limit| {
                                        guard
                                            .get::<str>(&tld)
                                            .map(|in_flight| *in_flight < limit)
                                            .unwrap_or(true)
                                    })
                                    .unwrap_or(true);

                                if ready {
                                    drop(guard);
                                    let mut write_guard = in_flight.write().unwrap();
                                    *(write_guard.entry(tld.to_owned()).or_insert(0)) += 1;
                                    drop(write_guard);
                                    Ok(Async::Ready(tld.to_owned()))
                                } else {
                                    drop(guard);
                                    Ok(Async::NotReady)
                                }
                            }
                            Err(TryLockError::WouldBlock) => Ok(Async::NotReady),
                            Err(TryLockError::Poisoned(e)) => panic!(e.to_string()),
                        }
                    })
                    .map(|tld| (tld, post))
                    .and_then(move |(tld, post)| {
                        save_hash(post.url.clone(), HashDest::Images)
                            .then(move |res| {
                                *end_in_flight.write().unwrap().get_mut(&tld).unwrap() -= 1;
                                match res {
                                    Ok(o) => Ok((post, o)),
                                    Err(e) => Err((post, e)),
                                }
                            })
                            .map(move |(post, (_hash, _hash_dest, image_id, exists))| {
                                if verbose {
                                    if exists {
                                        info!(
                                            "{}: {}: {} already exists",
                                            post.created_utc, post.id, post.url
                                        );
                                    } else {
                                        info!(
                                            "{}: {}: {} successfully hashed",
                                            post.created_utc, post.id, post.url
                                        );
                                    }
                                }

                                (post, image_id)
                            })
                            .map_err(move |(post, ue)| {
                                match ue.source {
                                    Source::Internal => {
                                        error!(
                                            "{}: {}: {}: {}{}{}{}",
                                            post.created_utc,
                                            post.id,
                                            post.url,
                                            ue.file.unwrap_or(""),
                                            ue.line
                                                .map(|line| Cow::Owned(format!("#{}", line)))
                                                .unwrap_or(Cow::Borrowed("")),
                                            if ue.file.is_some() || ue.line.is_some() {
                                                ": "
                                            } else {
                                                ""
                                            },
                                            ue.error
                                        );
                                        std::process::exit(1);
                                    }
                                    _ => {
                                        warn!(
                                            "{}: {}: {} failed: {}",
                                            post.created_utc, post.id, post.url, ue.error
                                        );
                                        if let Some(e) = ue.error.downcast_ref::<reqwest::Error>() {
                                            if e.is_timeout()
                                                || e.is_server_error()
                                                || e.get_ref()
                                                    .and_then(|e| {
                                                        e.downcast_ref::<hyper::Error>()
                                                            .map(hyper::Error::is_connect)
                                                    })
                                                    .unwrap_or(false)
                                            {
                                                if is_link_special(&post.url) {
                                                    error!(
                                                        "{}: {}: {}: Special link server error: {:?}",
                                                        post.created_utc, post.id, post.url, e
                                                    );
                                                    std::process::exit(1);
                                                }
                                                if let Ok(url) = Url::parse(&post.url) {
                                                    if let Some(host) = url.host_str() {
                                                        blacklist
                                                            .write()
                                                            .unwrap()
                                                            .insert(host.to_string(), true);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                };

                                post
                            })
                    })
                })
                .then(move |res| {
                    let (post, image_id) = res
                        .map(|(post, image_id)| (post, Some(image_id)))
                        .unwrap_or_else(|post| (post, None));
                    save_post(post, image_id).then(move |res| {
                        let mut all_spawned_lock = end_all_spawned.write().unwrap();
                        *all_spawned_lock -= 1;
                        drop(all_spawned_lock);
                        res
                    })
                })
                .map(|_| ())
                .map_err(|e| {
                    error!("Saving post failed: {}", e);
                    std::process::exit(1);
                }),
            );
        });

    ok(())
}

fn main() {
    lazy_static::lazy_static! {
        static ref REQW_CLIENT: Client = Client::new();
        static ref MONTH_RE: Regex = Regex::new(r"(\d\d)\..+$").unwrap();
        static ref YEAR_RE: Regex = Regex::new(r"\d\d\d\d").unwrap();
    }

    setup_logging!();
    let matches = clap_app!(
        ingest =>
            (version: crate_version!())
            (author: crate_authors!(","))
            (about: crate_description!())
            (@arg VERBOSE: -v --verbose "Verbose logging")
            (@arg NO_DELETE: -D --("no-delete") "Don't delete archive files when done")
            (@arg PATH: +required "The URL or path of the file to ingest")
    )
    .get_matches();

    let verbose = matches.is_present("VERBOSE");
    let no_delete = matches.is_present("NO_DELETE");
    let path = matches.value_of("PATH").unwrap().to_string();

    let future = future::lazy(move || {
        let month: i32 = MONTH_RE
            .captures(&path)
            .and_then(|caps| caps.get(1))
            .ok_or_else(|| format_err!("couldn't find month in {}", path))
            .and_then(|m| m.as_str().parse().map_err(Error::from))
            .unwrap();

        let year: i32 = YEAR_RE
            .find(&path)
            .ok_or_else(|| format_err!("couldn't find year in {}", path))
            .and_then(|m| m.as_str().parse().map_err(Error::from))
            .unwrap();

        let month_f = f64::from(month);
        let year_f = f64::from(year);

        info!("Ingesting {}", path);

        let (input_future, arch_path): (Box<dyn Future<Item = File, Error = Error> + Send>, _) =
            if path.starts_with("http://") || path.starts_with("https://") {
                let arch_path = std::env::var("HOME").map_err(Error::from).unwrap()
                    + "/archives/"
                    + Url::parse(&path)
                        .map_err(Error::from)
                        .unwrap()
                        .path_segments()
                        .ok_or_else(|| format_err!("cannot-be-a-base-url"))
                        .unwrap()
                        .next_back()
                        .ok_or_else(|| format_err!("no last path segment"))
                        .unwrap();

                let arch_file = if Path::exists(Path::new(&arch_path)) {
                    info!("Found existing archive file");

                    Box::new(future::result(
                        OpenOptions::new()
                            .read(true)
                            .open(&arch_path)
                            .map_err(Error::from),
                    )) as _
                } else {
                    info!("Downloading archive file");
                    let arch_file = OpenOptions::new()
                        .create_new(true)
                        .read(true)
                        .write(true)
                        .open(&arch_path)
                        .map_err(Error::from)
                        .unwrap();

                    Box::new(REQW_CLIENT.get(&path).send().map_err(Error::from).and_then(
                        move |resp| {
                            resp.into_body()
                                .map_err(Error::from)
                                .fold(arch_file, |mut arch_file, chunk| {
                                    io::copy(&mut chunk.as_ref(), &mut arch_file)
                                        .map(move |_| arch_file)
                                        .map_err(Error::from)
                                })
                                .and_then(|mut arch_file| {
                                    arch_file.seek(SeekFrom::Start(0)).map_err(Error::from)?;

                                    Ok(arch_file)
                                })
                        },
                    )) as _
                };

                (arch_file, Some(arch_path))
            } else {
                (
                    Box::new(future::result(File::open(&path).map_err(Error::from))) as _,
                    None,
                )
            };

        input_future.map_err(|e| panic!(e)).and_then(move |input| {
            info!("Processing posts we already have");

            PG_POOL
                .take()
                .map_err(Error::from)
                .and_then(move |mut client| {
                    client
                        .prepare(
                            "SELECT reddit_id_int FROM posts \
                             WHERE EXTRACT(month FROM created_utc) = $1 \
                             AND EXTRACT(year FROM created_utc) = $2",
                        )
                        .and_then(move |stmt| {
                            client.query(&stmt, &[&month_f, &year_f]).fold(
                                BTreeSet::new(),
                                |mut already_have, row| {
                                    already_have.insert(row.get(0));
                                    ok(already_have)
                                },
                            )
                        })
                        .map_err(Error::from)
                })
                .map_err(|e| panic!(e))
                .and_then(move |already_have| {
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

                    let input = BufReader::new(input);

                    let ingest_fut: Box<dyn future::Future<Item = (), Error = ()> + Send> =
                        if path.ends_with("bz2") {
                            Box::new(ingest_json(
                                already_have,
                                bzip2::bufread::BzDecoder::new(input),
                                verbose,
                            )) as _
                        } else if path.ends_with("xz") {
                            Box::new(ingest_json(
                                already_have,
                                xz2::bufread::XzDecoder::new(input),
                                verbose,
                            )) as _
                        } else if path.ends_with("zst") {
                            Box::new(ingest_json(
                                already_have,
                                zstd::stream::read::Decoder::new(input)
                                    .map_err(Error::from)
                                    .unwrap(),
                                verbose,
                            )) as _
                        } else {
                            Box::new(ingest_json(already_have, input, verbose)) as _
                        };

                    ingest_fut.map(move |_| {
                        if !no_delete {
                            if let Some(arch_path) = arch_path {
                                remove_file(arch_path).map_err(Error::from).unwrap();
                            }
                        }

                        info!("Done ingesting {}", &path);
                    })
                })
        })
    })
    .map_err(|_| ());

    let runtime = tokio::runtime::Runtime::new().expect("failed to start new Runtime");
    runtime.block_on_all(future).ok();
}
