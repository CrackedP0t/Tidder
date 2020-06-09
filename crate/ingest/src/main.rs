#![recursion_limit = "128"]

use chrono::prelude::*;
use clap::{clap_app, crate_authors, crate_description, crate_version};
use common::format;
use common::*;
use dashmap::DashMap;
use future::poll_fn;
use futures::prelude::*;
use futures::task::Poll;
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Deserializer;
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::error::Error as _;
use std::fs::{remove_file, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::iter::Iterator;
use std::path::Path;
use std::sync::Arc;
use tokio_postgres::types::ToSql;
use tracing_futures::Instrument;
use url::Url;

async fn ingest_post(
    post: Submission,
    verbose: bool,
    blacklist: &DashMap<String, ()>,
    in_flight: &DashMap<String, u32>,
) {
    debug!("Starting to ingest");

    let is_video = post.is_video;

    let post_url_res = (|| async {
        let mut post_url = post.url.as_str();

        if get_host(&post_url)
            .map(|host| blacklist.contains_key(&host))
            .unwrap_or(false)
        {
            return Err(ue_save!("blacklisted", "blacklisted"));
        }

        if CONFIG.banned.iter().any(|banned| banned.matches(post_url)) {
            return Err(ue_save!("banned", "banned"));
        }

        if is_video {
            post_url = post
                .preview
                .as_ref()
                .ok_or_else(|| ue_save!("is_video but no preview", "video_no_preview"))?
        }
        let post_url = Url::parse(&post_url).map_err(map_ue_save!("invalid URL", "url_invalid"))?;

        let post_url = if let Some("v.redd.it") = post_url.host_str() {
            Url::parse(
                post.preview
                    .as_ref()
                    .ok_or_else(|| ue_save!("v.redd.it but no preview", "v_redd_it_no_preview"))?,
            )?
        } else {
            post_url
        };

        Ok(post_url)
    })()
    .await;

    let save_res = match post_url_res {
        Ok(post_url) => {
            let host = post_url.host_str().unwrap();

            let custom_limit: Option<&Option<_>> = CONFIG.custom_limits.get(host);

            let limit = match custom_limit {
                None => Some(CONFIG.in_flight_limit),
                Some(&Some(limit)) => Some(limit),
                Some(&None) => None,
            };

            poll_fn(|context| {
                let ready = limit
                    .map(|limit| {
                        in_flight
                            .get(host)
                            .map(|in_flight| *in_flight < limit)
                            .unwrap_or(true)
                    })
                    .unwrap_or(true);

                if ready {
                    *(in_flight.entry(host.to_owned()).or_insert(0)) += 1;

                    Poll::Ready(host.to_owned())
                } else {
                    context.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;

            debug!("Starting to save");

            let res = save_hash(post_url.as_str(), HashDest::Images).await;

            *in_flight.get_mut(host).unwrap() -= 1;

            res
        }
        Err(e) => Err(e),
    };

    let image_id = match save_res {
        Ok(hash_gotten) => {
            info!("successfully hashed");

            Ok(hash_gotten.id)
        }
        Err(ue) => match ue.source {
            Source::Internal => {
                eprintln!(
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
                let reqwest_save_error = match ue.error.downcast_ref::<reqwest::Error>() {
                    Some(e) => {
                        let hyper_error =
                            e.source().and_then(|he| he.downcast_ref::<hyper::Error>());

                        if e.is_timeout() || hyper_error.is_some() {
                            if let Ok(url) = Url::parse(&post.url) {
                                if let Some(host) = url.host_str() {
                                    if !CONFIG.no_blacklist.iter().any(|n| host.ends_with(n)) {
                                        blacklist.insert(host.to_string(), ());
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
                    }
                    None => None,
                };

                let save_error = ue.save_error.or(reqwest_save_error);

                warn!(
                    "failed to save{}: {}",
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
                info!("successfully saved");
            }
        }
        Err(e) => {
            eprintln!("failed to save: {:?}", e);
            std::process::exit(1);
        }
    }
}

async fn ingest_json<R: Read + Send + 'static>(
    verbose: bool,
    mut already_have: Option<BTreeSet<i64>>,
    json_stream: R,
) {
    let json_iter = Deserializer::from_reader(json_stream).into_iter::<Submission>();

    let json_iter = json_iter.filter_map(move |post| {
        let post = match post {
            Ok(post) => post,
            Err(e) => {
                if e.is_data() {
                    if verbose {
                        warn!("{}", e);
                    }
                    return None;
                } else {
                    panic!("{}", e)
                }
            }
        };

        let post = post.finalize().unwrap();

        if !post.is_self
            && post.promoted.map_or(true, |promoted| !promoted)
            && (post.is_video
                || (EXT_RE.is_match(&post.url) && URL_RE.is_match(&post.url))
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
        {
            Some(post)
        } else {
            None
        }
    });

    let blacklist = Arc::new(DashMap::<String, ()>::new());
    let in_flight = Arc::new(DashMap::<String, u32>::new());

    info!("Starting ingestion!");

    futures::stream::iter(json_iter.map(|post| {
        let blacklist = blacklist.clone();
        let in_flight = in_flight.clone();

        tokio::spawn(Box::pin(async move {
            let span = info_span!(
                "ingest_post",
                id = post.id.as_str(),
                date = post.created_utc.to_string().as_str(),
                url = post.url.as_str()
            );
            ingest_post(post, verbose, &blacklist, &in_flight)
                .instrument(span)
                .await;
        }))
    }))
    .buffer_unordered(CONFIG.worker_count)
    .map(|t| t.unwrap())
    .collect::<()>()
    .await
}

#[tokio::main]
async fn main() -> Result<(), UserError> {
    static DATE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(\d\d\d\d)-(\d\d)-(\d\d)?").unwrap());

    tracing_subscriber::fmt::init();

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

    let (year, month, day): (i32, u32, Option<u32>) = DATE_RE
        .captures(&path)
        .ok_or_else(|| ue!(format!("couldn't find date in {}", path)))
        .and_then(|caps| {
            Ok((
                caps.get(1)
                    .ok_or_else(|| ue!(format!("couldn't find year in {}", path)))?
                    .as_str()
                    .parse()
                    .map_err(map_ue!())?,
                caps.get(2)
                    .ok_or_else(|| ue!(format!("couldn't find month in {}", path)))?
                    .as_str()
                    .parse()
                    .map_err(map_ue!())?,
                caps.get(3)
                    .map(|s| s.as_str().parse().map_err(map_ue!()))
                    .transpose()?,
            ))
        })?;

    let date = NaiveDate::from_ymd(year, month, day.unwrap_or(1)).and_hms(0, 0, 0);

    let next_date = if let Some(day) = day {
        const MONTH_LENGTHS: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

        // >= for leap years
        if day >= MONTH_LENGTHS[month as usize - 1] {
            if month == 12 {
                NaiveDate::from_ymd(year + 1, 1, 1)
            } else {
                NaiveDate::from_ymd(year, month + 1, 1)
            }
        } else {
            NaiveDate::from_ymd(year, month, day + 1)
        }
    } else if month == 12 {
        NaiveDate::from_ymd(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd(year, month + 1, 1)
    }
    .and_hms(0, 0, 0);

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

                let no_timeout_client = reqwest::Client::builder().build()?;

                let mut resp = no_timeout_client
                    .get(&path)
                    .send()
                    .await?
                    .error_for_status()?;

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

    let client = PG_POOL.get().await?;
    let already_have = client
        .query_raw(
            "SELECT reddit_id_int FROM posts \
             WHERE created_utc >= $1 and created_utc < $2",
            [&date as &dyn ToSql, &next_date as &dyn ToSql].iter().copied(),
        )
        .await?
        .try_fold(BTreeSet::new(), move |mut already_have, row| async move {
            already_have.insert(row.get(0));
            Ok(already_have)
        })
        .await?;

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
    } else if path.ends_with("gz") {
        ingest_json(
            verbose,
            already_have,
            flate2::bufread::GzDecoder::new(input),
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
