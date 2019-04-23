use common::*;
use fern;
use futures::future::lazy;
use futures::Future;
use hyper::{client::HttpConnector, Body, Client};
use hyper_tls::HttpsConnector;
use lazy_static::lazy_static;
use log::LevelFilter;
use log::{error, info, warn};
use postgres::{self, NoTls};
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use serde_json::json;
use std::env;
use std::sync::Arc;

lazy_static! {
    static ref DB_POOL: r2d2::Pool<PostgresConnectionManager<NoTls>> =
        r2d2::Pool::new(PostgresConnectionManager::new(
            "dbname=tidder host=/run/postgresql user=postgres"
                .parse()
                .unwrap(),
            NoTls,
        ))
        .unwrap();
    static ref HTTPS: HttpsConnector<HttpConnector> = HttpsConnector::new(4).unwrap();
    static ref REQW_CLIENT: reqwest::Client = reqwest::Client::new();
}

fn download_search(search: PushShiftSearch) -> Result<(), ()> {
    let thread_counter = Arc::new(());

    'posts: for post in search.hits.hits {
        let post = post.source;
        let link = post.url.clone();

        // let mut db_client = DB_POOL.get().map_err(le!())?;
        // if db_client
        //     .query_iter(
        //         "SELECT EXISTS(SELECT FROM posts WHERE reddit_id = $1)",
        //         &[&post.data.id],
        //     )
        //     .map_err(le!())?
        //     .next()
        //     .map_err(le!())?
        //     .unwrap()
        //     .try_get::<_, bool>(0)
        //     .map_err(le!())?
        // {
        //     info!("{} already exists", post.data.permalink);
        //     continue 'posts;
        // }

        let new_tc = Arc::clone(&thread_counter);

        let client = Client::builder().build::<_, Body>((*HTTPS).clone());
        tokio::spawn(get_image(client, link.clone()).then(move |res| {
            let tc = new_tc;
            let ret = match res {
                Ok((img, status)) => {
                    save_post(&DB_POOL, &post, Some(dhash(img)), Some(status));
                    info!("{} successfully hashed", link);
                    Ok(())
                }
                Err(e) => {
                    warn!("{}", e);
                    let ie = e.error;
                    let status = match ie.downcast::<StatusFail>() {
                        Ok(se) => Some(se.status),
                        Err(_) => None,
                    };
                    save_post(&DB_POOL, &post, None, status);
                    Err(())
                }
            };
            drop(tc);
            ret
        }));
    }

    while Arc::strong_count(&thread_counter) > 1 {}

    info!("Reached end of this search");

    Ok(())
}

fn download(size: usize) -> Result<(), ()> {
    // let mut gotten: usize = 0;
    // let mut search_after: Option<(u64,)> = None;
    // while match size {
    //     Some(size) => gotten < size,
    //     None => true,
    // } {
    let body = json! ({
        "sort": [
            {"created_utc": "desc"}
        ],
        "size": size
    });
    // if let Some(search_after) = search_after {
    //     body["search_after"] = serde_json::to_value(search_after).map_err(le!())?;
    // }

    let req = REQW_CLIENT
        .get("http://elastic.pushshift.io/rs/submissions/_search")
        .json(&body);

    let resp = req
        .send()
        .map_err(le!())?
        .error_for_status()
        .map_err(le!())?;

    // let val: serde_json::Value = serde_json::from_reader(resp).map_err(le!())?;

    // println!("{}", serde_json::to_string_pretty(&val).unwrap());

    // return Ok(());

    let search: PushShiftSearch = serde_json::from_reader(resp).map_err(le!())?;

    // match search.hits.hits.first() {
    //     Some(hit) => {
    //         println!("{:#?}", hit);
    //         // search_after = Some(hit.sort);
    //     }
    //     None => {
    //         info!("No more hits");
    //         return Ok(());
    //     }
    // }

    // gotten += search.hits.hits.len();
    download_search(search)?;
    // }

    Ok(())
}

fn main() {
    tokio::run(lazy(move || {
        fern::Dispatch::new()
            .format(|out, message, record| {
                out.finish(format_args!(
                    "{}[{}{}][{}] {}",
                    chrono::Local::now().format("[%Y-%m-%d %H:%M:%S]"),
                    record.target(),
                    match record.file() {
                        Some(file) => format!(
                            ":{}{}",
                            file,
                            match record.line() {
                                Some(line) => format!("#{}", line),
                                None => "".to_string(),
                            }
                        ),
                        None => "".to_string(),
                    },
                    record.level(),
                    message
                ))
            })
            .level(LevelFilter::Warn)
            .level_for("hasher", LevelFilter::Info)
            .chain(std::io::stderr())
            .chain(fern::log_file("output.log").unwrap())
            .apply()
            .unwrap();
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
            // info!("Downloading unlimited posts");
            // download(None)
            error!("Please provide a size");
            Err(())
        }
    }));
}
