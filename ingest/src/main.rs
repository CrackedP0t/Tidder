use clap::{clap_app, crate_authors, crate_description, crate_version};
use common::*;
use futures::future::Future;
use futures::lazy;
use hyper::{client::HttpConnector, Body, Client, StatusCode};
use hyper_tls::HttpsConnector;
use lazy_static::lazy_static;
use log::{error, info, warn};
use postgres::NoTls;
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use regex::Regex;
use reqwest;
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
    static ref HTTPS: HttpsConnector<HttpConnector> = HttpsConnector::new(4).unwrap();
    static ref EXT_RE: Regex =
        Regex::new(r"\.(?:(?:jpe?g)|(?:png)|(?:gif)|(?:webp)|(?:bmp))\b").unwrap();
}

fn main() {
    tokio::run(lazy(|| {
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

        let json_iter = Deserializer::from_reader(
            Decoder::new(if let Some(file) = matches.value_of("FILE") {
                Box::new(File::open(file).map_err(le!())?) as Box<dyn Read>
            } else if let Some(url) = matches.value_of("URL") {
                Box::new(reqwest::get(url).map_err(le!())?) as Box<dyn Read>
            } else {
                panic!("No file or URL passed");
            })
            .map_err(le!())?,
        )
        .into_iter::<Submission>();

        info!("Starting ingestion!");

        for post in json_iter {
            match post {
                Err(e) => {
                    error!("{}", e);
                    return Err(());
                }
                Ok(post) => {
                    if post.is_self || !EXT_RE.is_match(&post.url) {
                        continue;
                    }
                    tokio::spawn(lazy(move || {
                        let client = Client::builder().build::<_, Body>((*HTTPS).clone());
                        get_image(client, post.url.clone()).then(move |res| match res {
                            Ok((img, _status)) => {
                                save_post(&DB_POOL, &post, dhash(img));
                                info!("{} successfully hashed", post.url);
                                Ok(())
                            }
                            Err(e) => {
                                let msg = format!("{}", e);
                                let ie = e.error;
                                if let Ok(sf) = ie.downcast::<StatusFail>() {
                                    if sf.status != StatusCode::NOT_FOUND {
                                        warn!("{}", msg);
                                    }
                                }
                                Err(())
                            }
                        })
                    }));
                }
            }
        }

        Ok(())
    }));
}
