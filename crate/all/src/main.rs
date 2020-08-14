use common::*;
use futures::prelude::*;
use std::borrow::Cow;
use std::error::Error;
use tokio::time::{Duration, Instant};
use tracing_futures::Instrument;
use chrono::{DateTime, NaiveDateTime};

mod reddit_api;
use reddit_api::SubredditListing;

struct RedditClient {
    client: reqwest::Client,
    next_request: Instant,
    last_modhash: Option<String>,
}

const INTERVAL: Duration = Duration::from_secs(5);

impl RedditClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .user_agent(USER_AGENT)
                .build()
                .unwrap(),
            next_request: Instant::now(),
            last_modhash: None,
        }
    }

    pub async fn get_sub_listing(&mut self, url: &str) -> Result<(SubredditListing, NaiveDateTime), UserError> {
        tokio::time::delay_until(self.next_request).await;

        let mut req = self.client.get(url);

        self.next_request = Instant::now() + INTERVAL;

        if let Some(modhash) = self.last_modhash.clone() {
            req = req.header("X-Modhash", modhash);
        }

        let resp = req
            .send()
            .map_err(map_ue!("Couldn't access Reddit API"))
            .await?
            .error_for_status()?;

        let date = DateTime::parse_from_rfc2822(resp.headers()["date"].to_str()?)?.naive_utc();

        let text = resp.text().await?;

        let listing_res: Result<SubredditListing, _> = serde_json::from_str(&text);

        match listing_res {
            Ok(listing) => {
                self.last_modhash = Some(listing.data.modhash.clone());
                Ok((listing, date))
            }
            Err(e) => {
                Err(e.into())
            }
        }
    }
}

async fn ingest_post(post: Submission) -> bool {
    let post_url_res = post.choose_url();

    let save_res = match post_url_res {
        Ok(post_url) => save_hash(post_url.as_str(), HashDest::Images).await,
        Err(e) => Err(e),
    };

    let image_id = match save_res {
        Ok(hash_gotten) => {
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

    let image_id_ok = image_id.is_ok();

    match post.save(image_id).await {
        Ok(already_have) => {
            if image_id_ok {
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

const ALL_BASE_URL: &str = "https://api.reddit.com/r/all/new?limit=100";

async fn get_latest(client: &mut RedditClient) -> Result<(), UserError> {
    let mut all_url = ALL_BASE_URL.to_string();
    let mut count = 0;

    loop {
        let (listing, date) = client.get_sub_listing(&all_url).await?;

        info!("Downloading new listing; recieved {} posts so far", count);

        count += listing.data.dist;

        let old = futures::stream::iter(
            listing
                .data
                .children
                .into_iter()
                .filter_map(|child| {
                    let reddit_api::Child { data } = child;
                    let post = data.finalize().unwrap();
                    if post.desirable() {
                        Some(post)
                    } else {
                        None
                    }
                })
                .map(|mut post| {
                    tokio::spawn(async move {
                        post.updated = Some(date);
                        let span = info_span!(
                            "ingest_post",
                            id = post.id.as_str(),
                            date = post.created_utc.to_string().as_str(),
                            url = post.url.as_str()
                        );
                        ingest_post(post).instrument(span).await
                    })
                }),
        )
        .buffer_unordered(CONFIG.worker_count)
        .fold(false, |a, b| async move { a || b.unwrap() })
        .await;

        if old {
            info!("found posts we already have after {} posts; going to start!", count);
            break Ok(());
        }

        all_url = format!(
            "{}&after={}&count={}",
            ALL_BASE_URL, listing.data.after, count
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), UserError> {
    tracing_subscriber::fmt::init();

    let mut client = RedditClient::new();

    loop {
        get_latest(&mut client).await?;
    }
}
