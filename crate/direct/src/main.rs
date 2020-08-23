use common::*;

use futures::prelude::*;
use futures::stream::poll_fn;
use futures::task::Poll;
use std::borrow::Cow;
use std::error::Error;
use tokio::time::{delay_until, Duration, Instant};
use tracing_futures::Instrument;

mod info;

const BASE_GET_URL: &str = "https://api.reddit.com/api/info/?id=";

const RATE_LIMIT_WAIT: Duration = Duration::from_secs(1);
const ERROR_WAIT: Duration = Duration::from_secs(5);

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

                let save_error = reqwest_save_error.or(ue.save_error);

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

async fn get_100(
    next_req: Instant,
    range: impl Iterator<Item = i64>,
) -> Result<Vec<Submission>, UserError> {
    delay_until(next_req).await;

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

#[tokio::main]
async fn main() -> Result<(), UserError> {
    tracing_subscriber::fmt::init();

    let start_id = i64::from_str_radix(&std::env::args().nth(1).unwrap(), 36)?;

    let mut getter_fut = Box::pin(tokio::spawn(get_100(Instant::now(), start_id..start_id + 100)));
    let mut this_id = start_id;
    let get_stream = poll_fn(|ctx| match Future::poll(getter_fut.as_mut(), ctx) {
        Poll::Pending => Poll::Pending,
        Poll::Ready(Err(e)) => {
            panic!("tokio error: {}", e)
        }
        Poll::Ready(Ok(Err(e))) => {
            error!(
                "Error getting posts starting at {} ({}): {}",
                this_id,
                Base36::new(this_id),
                e
            );
            getter_fut = Box::pin(tokio::spawn(get_100(Instant::now() + ERROR_WAIT, this_id..this_id + 100)));

            ctx.waker().wake_by_ref();

            Poll::Pending
        }
        Poll::Ready(Ok(Ok(this_100))) => {
            this_id = this_100
                .iter()
                .map(|p| p.id_int)
                .max()
                .unwrap()
                + 1;

            getter_fut = Box::pin(tokio::spawn(get_100(
                Instant::now() + RATE_LIMIT_WAIT,
                this_id..this_id + 100,
            )));

            info!(
                "Ingesting {} posts within {} ({}) and {} ({})",
                this_100.len(),
                this_id,
                Base36::new(this_id),
                this_id + 99,
                Base36::new(this_id + 99)
            );

            Poll::Ready(Some(futures::stream::iter(this_100)))
        }
    });

    get_stream
        .flatten()
        .filter_map(|post| async move {
            if post.desirable() {
                Some(tokio::spawn(async move {
                    let span = info_span!(
                        "ingest_post",
                        id = post.id.as_str(),
                        date = post.created_utc.to_string().as_str(),
                        url = post.url.as_str(),
                    );
                    ingest_post(post).instrument(span).await;
                }))
            } else {
                None
            }
        })
        .buffer_unordered(CONFIG.worker_count)
        .try_collect::<()>()
        .await
        .map_err(From::from)
}
