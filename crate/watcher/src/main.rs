use common::*;
use lazy_static::lazy_static;
use log::{error, info, warn};
use postgres::{self, NoTls};
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use serde_json::json;
use std::env;

lazy_static! {
    static ref DB_POOL: r2d2::Pool<PostgresConnectionManager<NoTls>> = r2d2::Pool::new(
        PostgresConnectionManager::new(SECRETS.postgres.connect.parse().unwrap(), NoTls,)
    )
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
            match save_hash(&post.url, HashDest::Images) {
                Ok((_hash, _hash_dest, image_id, exists)) => {
                    if exists {
                        info!("{} already exists", post.url);
                    } else {
                        info!("{} successfully hashed", post.url);
                    }
                    save_post(&DB_POOL, &post, image_id);
                }
                Err(ue) => {
                    warn!("{} failed: {}", post.url, ue.error);
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
