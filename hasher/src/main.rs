use common::*;
use lazy_static::lazy_static;
use log::{error, info, warn};
use postgres::{self, NoTls};
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use serde_json::json;
use std::env;

lazy_static! {
    static ref DB_POOL: r2d2::Pool<PostgresConnectionManager<NoTls>> =
        r2d2::Pool::new(PostgresConnectionManager::new(
            "dbname=tidder host=/run/postgresql user=postgres"
                .parse()
                .unwrap(),
            NoTls,
        ))
        .unwrap();
    static ref REQW_CLIENT: reqwest::Client = reqwest::Client::new();
}

fn download_search(search: PushShiftSearch) -> Result<(), ()> {
    use rayon::prelude::*;

    search.hits.hits.into_par_iter().for_each(|post: Hit| {
        let post = post.source;
        match get_image(post.url.clone()) {
            Ok(img) => {
                save_post(&DB_POOL, &post, dhash(img));
                info!("{} successfully hashed", post.url);
            }
            Err(gif) => {
                let msg = format!("{}", gif);
                let ie = gif.error;
                if let Ok(sf) = ie.downcast::<StatusFail>() {
                    if sf.status != reqwest::StatusCode::NOT_FOUND {
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
