use common::*;

use futures::prelude::*;
use std::borrow::Cow;
use std::error::Error;
use tokio::time::{delay_until, Duration, Instant};
use tracing_futures::Instrument;

mod info;

const BASE_GET_URL: &str = "https://api.reddit.com/api/info/?id=";

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

async fn get_100(range: impl Iterator<Item = i64>) -> Result<Vec<Submission>, UserError> {
    let client = reqwest::Client::builder().user_agent(USER_AGENT).build()?;

    let mut url = BASE_GET_URL.to_string();

    for id in range {
        url += &format!("t3_{},", Base36::new(id));
    }

    let info = client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .json::<info::Info>()
        .await?;

    Ok(info
        .data
        .children
        .into_iter()
        .map(|c| c.data.finalize().unwrap())
        .collect())
}

async fn ingest_100(posts: Vec<Submission>) -> Result<i64, UserError> {
    futures::stream::iter(posts.into_iter())
        .filter_map(|post| async move {
            if post.desirable() {
                Some(tokio::spawn(async move {
                    let span = info_span!(
                        "ingest_post",
                        id = post.id.as_str(),
                        date = post.created_utc.to_string().as_str(),
                        url = post.url.as_str(),
                    );
                    let id = post.id_int;
                    ingest_post(post).instrument(span).await;
                    id
                }))
            } else {
                None
            }
        })
        .buffer_unordered(CONFIG.worker_count)
        .try_fold(0, |l, t| async move { Ok(std::cmp::max(l, t)) })
        .await
        .map_err(From::from)
}

#[tokio::main]
async fn main() -> Result<(), UserError> {
    tracing_subscriber::fmt::init();

    let mut start_id = i64::from_str_radix(&std::env::args().nth(1).unwrap(), 36)?;

    loop {
        let next_id = start_id + 99;

        info!(
            "Requesting from {} ({}) to {} ({})",
            start_id,
            Base36::new(start_id),
            next_id,
            Base36::new(next_id)
        );

        let next_100 = get_100(start_id..=next_id).await.unwrap();

        let later = Instant::now() + Duration::from_secs(2);

        if let Some(next_post) = next_100.last() {
            let next_id = next_post.id_int;

            info!(
                "Downloading {} posts within {} ({}) and {} ({})",
                next_100.len(),
                start_id,
                Base36::new(start_id),
                next_id,
                Base36::new(next_id)
            );

            let next_id = ingest_100(next_100).await.unwrap();

            if next_id != 0 {
                start_id = next_id + 1;
            }
        }

        delay_until(later).await;
    }
}
