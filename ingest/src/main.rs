use clap::{clap_app, crate_authors, crate_description, crate_version};
use common::*;
use google_bigquery2::{Bigquery, QueryRequest};
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
use yup_oauth2::{service_account_key_from_file, ServiceAccountAccess};
use zstd::stream::read::Decoder;

mod archive;

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

const BQ_PROJECT: &str = "tidder";

fn ingest<R: Read + Send>(reader: Option<R>) {
    println!("Creating iterator...");

    match reader {
        None => {
            use hyper::net::HttpsConnector;
            use hyper_rustls::TlsClient;

            let client_secret =
                service_account_key_from_file(&secrets::load().unwrap().bigquery.key_file).unwrap();
            let access = ServiceAccountAccess::new(
                client_secret,
                hyper::Client::with_connector(HttpsConnector::new(TlsClient::new())),
            );
            let hub = Bigquery::new(
                hyper::Client::with_connector(HttpsConnector::new(TlsClient::new())),
                access,
            );

            let q_res = hub
                .jobs()
                .query(
                    QueryRequest {
                        query: Some(
                            "SELECT author, created_utc, is_self, over_18, permalink, score, spoiler, `".to_string(),
                        ),
                        use_legacy_sql: Some(false),
                        ..Default::default()
                    },
                    BQ_PROJECT,
                )
                .doit();

            println!("{:#?}", q_res.unwrap());
        }
        Some(reader) => {
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
    }
}

fn main() {
    setup_logging();
    let matches = clap_app!(
        ingest =>
            (version: crate_version!())
            (author: crate_authors!(","))
            (about: crate_description!())
            (@group from +required =>
             (@arg URL: -u --url +takes_value "The URL of the file to ingest")
             (@arg FILE: -f --file +takes_value "The path of the file to ingest")
             (@arg bigquery: -b --bigquery "Use BigQuery")
             (@arg DOWNLOAD: -d --download "Download all of PushShift's archives")
            )
    )
    .get_matches();

    println!("{:?}", matches.value_of("bigquery"));

    if let Some(file) = matches.value_of("FILE") {
        ingest(Some(File::open(file).unwrap()));
    } else if let Some(url) = matches.value_of("URL") {
        ingest(Some(reqwest::get(url).unwrap()));
    } else if matches.is_present("bigquery") {
        ingest::<std::io::Empty>(None)
    }
}
