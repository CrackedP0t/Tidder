use common::*;

use bytes::BytesMut;
use futures::prelude::*;
use once_cell::sync::Lazy;
use regex::Regex;
use std::borrow::Cow;
use std::error::Error;
use std::sync::{Arc, Mutex};
use tokio::time::{delay_for, Duration};
use tracing_futures::Instrument;

const BASE_STREAM_URL: &str = "http://stream.pushshift.io?type=submissions&is_self=false";

const NEWLINE_CODE: u8 = 10;

async fn ingest_post(post: Submission) -> bool {
    let post_url_res = post.choose_url();

    let save_res = match post_url_res {
        Ok(post_url) => save_hash(post_url.as_str(), HashDest::Images).await,
        Err(e) => Err(e),
    };

    let image_id = match save_res {
        Ok(hash_gotten) => Ok(hash_gotten.id),
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
                    "failed to hash{}: {}",
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

    let good = image_id.is_ok();

    match post.save(image_id).await {
        Ok(already_have) => {
            if good {
                if already_have {
                    info!("already have");
                } else {
                    info!("successfully saved");
                }
            }
            already_have
        }
        Err(e) => {
            eprintln!("failed to save: {:?}", e);
            std::process::exit(1);
        }
    }
}

async fn process_events(data: &[u8], counter: Arc<Mutex<u64>>) -> Result<Option<i64>, UserError> {
    static PARSER: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"id: (\d+)\nevent: (\w+)\ndata: (.+)\n\n").unwrap());

    let text = std::str::from_utf8(data)?;

    let mut one = false;

    let iter = PARSER.captures_iter(text).filter_map(|captures| {
        one = true;

        let (id, event, data) = (
            captures.get(1).unwrap().as_str().parse().unwrap(),
            captures.get(2).unwrap().as_str(),
            captures.get(3).unwrap().as_str(),
        );

        match event {
            "rs" => {
                let post = serde_json::from_str::<Submission>(data)
                    .unwrap()
                    .finalize()
                    .unwrap();

                if post.desirable() {
                    let y_counter = counter.clone();

                    Some(tokio::spawn(async move {
                        let span = {
                            let mut guard = y_counter.lock().unwrap();
                            *guard += 1;
                            info_span!(
                                "ingest_post",
                                id = post.id.as_str(),
                                date = post.created_utc.to_string().as_str(),
                                url = post.url.as_str(),
                                counter = *guard
                            )
                        };
                        ingest_post(post).instrument(span).await;
                        *y_counter.lock().unwrap() -= 1;

                        id
                    }))
                } else {
                    None
                }
            }
            "keepalive" => {
                println!("keepalive");
                None
            }
            other => panic!("Unexpected event `{}`", other),
        }
    });

    let last_id = futures::stream::iter(iter)
        .buffer_unordered(CONFIG.worker_count)
        .fold(None, |largest, r| async move {
            let id = r.unwrap();
            Some(if let Some(largest) = largest {
                std::cmp::max(largest, id)
            } else {
                id
            })
        })
        .await;

    if !one {
        return Err(ue!(format!("Unexpected event format: `{}`", text)));
    }

    Ok(last_id)
}

async fn stream(mut last_id: Option<i64>) -> Result<(), (Option<i64>, UserError)> {
    let client = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .build()
        .map_err(|e| (last_id, e.into()))?;

    let req_url = match last_id {
        None => BASE_STREAM_URL.to_string(),
        Some(last_id) => format!("{}&submission_start_id={}", BASE_STREAM_URL, last_id),
    };

    let mut bytes_stream = client
        .get(&req_url)
        .send()
        .await
        .map(|r| {
            if r.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                panic!("Too many requests!")
            } else {
                r
            }
        })
        .and_then(|r| r.error_for_status())
        .map_err(|e| (last_id, e.into()))?
        .bytes_stream();

    let mut current_data = BytesMut::new();

    let counter = Arc::new(Mutex::new(0));

    loop {
        let bytes = bytes_stream
            .try_next()
            .await
            .map_err(|e| (last_id, e.into()))?
            .unwrap();

        current_data.extend_from_slice(&bytes);

        let boundary = current_data
            .windows(2)
            .rev()
            .position(|window| window[0] == NEWLINE_CODE && window[1] == NEWLINE_CODE);

        if let Some(index) = boundary {
            info!("Done collecting chunks; processing events");

            last_id = process_events(
                &current_data[0..current_data.len() - index],
                counter.clone(),
            )
            .await
            .map_err(|e| (last_id, e))?
            .or(last_id);

            current_data.clear();
            current_data.extend_from_slice(&bytes.slice(bytes.len() - index..bytes.len()));

            info!("Done processing events; collecting chunks");
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), UserError> {
    tracing_subscriber::fmt::init();

    let mut get_id = !std::env::args().skip(1).any(|a| a == "-i");

    let client = PG_POOL.get().await?;

    loop {
        let last_id = if get_id {
            let last_id = client
                .query_one(
                    "SELECT reddit_id_int FROM posts ORDER BY reddit_id_int DESC LIMIT 1",
                    &[],
                )
                .await?
                .get("reddit_id_int");

            info!("Last ID: {}", last_id);
            Some(last_id)
        } else {
            get_id = true;
            None
        };

        if let Err((_last_id, ue)) = stream(last_id).await {
            error!("{}", ue);
        }

        delay_for(Duration::from_secs(5)).await;
    }
}
