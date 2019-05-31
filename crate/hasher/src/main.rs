use common::*;
use lazy_static::lazy_static;
use log::{error, info, warn};
use postgres::{self, NoTls};
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use reqwest::StatusCode;
use serde_json::json;
use std::env;

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
    static ref REQW_CLIENT: reqwest::Client = reqwest::Client::new();
}

fn download_search(search: PushShiftSearch) -> Result<(), ()> {
    use rayon::prelude::*;

    search
        .hits
        .hits
        .into_iter()
        .filter_map(|post| {
            let post = post.source;
            if !post.is_self && EXT_RE.is_match(&post.url) {
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
                Err(e) => {
                    let msg = e.to_string();
                    if let Ok(sf) = e.downcast::<StatusFail>() {
                        if sf.status != StatusCode::NOT_FOUND {
                            warn!("{}", msg);
                        }
                    } else {
                        warn!("{}", msg);
                    }
                }
            }
        });

    Ok(())
}

fn download(size: usize) -> Result<(), ()> {
    let body = json! ({
        "sort": [
            {"created_utc": "desc"}
        ],
        "size": size
    });

    let req = REQW_CLIENT
        .get("http://elastic.pushshift.io/rs/submissions/_search")
        .json(&body);

    let resp = req
        .send()
        .map_err(le!())?
        .error_for_status()
        .map_err(le!())?;

    let search: PushShiftSearch = serde_json::from_reader(resp).map_err(le!())?;

    download_search(search)?;

    Ok(())
}

fn main() -> Result<(), ()> {
    setup_logging();
    let size = env::args().nth(1);
    if let Some(size) = size {
        if let Ok(size) = size.parse::<usize>() {
            info!("Downloading {} posts", size);
            download(size)
        } else {
            error!("Size is not a valid usize");
            Err(())
        }
    } else {
        error!("Please provide a size");
        Err(())
    }
}
