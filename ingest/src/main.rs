use clap::{clap_app, crate_authors, crate_description, crate_version};
use common::*;
use lazy_static::lazy_static;
use log::{error, info, warn};
use postgres::NoTls;
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use rayon::iter::ParallelBridge;
use rayon::prelude::*;
use regex::Regex;
use reqwest::StatusCode;
use serde_json::Deserializer;
use std::fs::File;
use std::io::Read;
use zstd::stream::read::Decoder;

lazy_static! {
    static ref DB_POOL: r2d2::Pool<PostgresConnectionManager<NoTls>> =
        r2d2::Pool::new(PostgresConnectionManager::new(
            "dbname=tidder host=/run/postgresql user=postgres"
                .parse()
                .unwrap(),
            NoTls,
        ))
        .unwrap();
    static ref EXT_RE: Regex =
        Regex::new(r"\W(?:png|jpe?g|gif|webp|p[bgpn]m|tiff?|bmp|ico|hdr)\b").unwrap();
}

fn ingest<R: Read + Send>(reader: R) {
    info!("Creating iterator...");

    let json_iter =
        Deserializer::from_reader(Decoder::new(reader).unwrap()).into_iter::<Submission>();

    info!("Starting ingestion!");

    json_iter
        .filter_map(|post| match post {
            Err(e) => {
                error!("{}", e);
                None
            }
            Ok(post) => {
                if !post.is_self && EXT_RE.is_match(&post.url) {
                    Some(post)
                } else {
                    None
                }
            }
        })
        .par_bridge()
        .for_each(|post: Submission| match get_image(post.url.clone()) {
            Ok(img) => {
                save_post(&DB_POOL, &post, dhash(img));
                info!("{} successfully hashed", post.url);
            }
            Err(gif) => {
                let msg = format!("{}", gif);
                let ie = gif.error;
                if let Ok(sf) = ie.downcast::<StatusFail>() {
                    if sf.status != StatusCode::NOT_FOUND {
                        warn!("{}", msg);
                    }
                } else {
                    warn!("{}", msg);
                }
            }
        });
}

fn main() {
    setup_logging();
    let matches = clap_app!(
        ingest =>
            (version: crate_version!())
            (author: crate_authors!(","))
            (about: crate_description!())
            (@arg URL: -u --url +takes_value "The URL of the file to ingest")
            (@arg FILE: -f --file +takes_value "The path of the file to ingest")
    )
    .get_matches();

    if let Some(file) = matches.value_of("FILE") {
        ingest(File::open(file).unwrap());
    } else if let Some(url) = matches.value_of("URL") {
        ingest(reqwest::get(url).unwrap());
    } else {
        error!("No file or URL passed");
    }
}
