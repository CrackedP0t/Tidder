use clap::{clap_app, crate_authors, crate_description, crate_version};
use common::*;
use lazy_static::lazy_static;
use log::{error, info, warn};
use postgres::NoTls;
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use rayon::prelude::*;
use reqwest::{Client, StatusCode};
use serde_json::Deserializer;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};

lazy_static! {
    static ref DB_POOL: r2d2::Pool<PostgresConnectionManager<NoTls>> =
        r2d2::Pool::new(PostgresConnectionManager::new(
            "dbname=tidder host=/run/postgresql user=postgres"
                .parse()
                .unwrap(),
            NoTls,
        ))
        .unwrap();
}

fn ingest_json<R: Read + Send>(json_stream: R) {
    let json_iter = Deserializer::from_reader(json_stream).into_iter::<Submission>();

    info!("Starting ingestion!");

    json_iter
        .filter_map(|post| {
            let post = post.unwrap();
            if !post.is_self && EXT_RE.is_match(&post.url) {
                Some(post)
            } else {
                None
            }
        })
        .par_bridge()
        .for_each(|post: Submission| match get_hash(post.url.clone()) {
            Ok((_hash, image_id, exists)) => {
                if exists {
                    info!("{} already exists", post.url);
                } else {
                    info!("{} successfully hashed", post.url);
                }
                save_post(&DB_POOL, &post, image_id);
            }
            Err(ghf) => {
                let msg = format!("{}", ghf);
                let ie = ghf.error;
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

fn main() -> Result<(), ()> {
    lazy_static::lazy_static! {
        static ref REQW_CLIENT: Client = Client::new();
    }
    setup_logging();
    let matches = clap_app!(
        ingest =>
            (version: crate_version!())
            (author: crate_authors!(","))
            (about: crate_description!())
            (@group from +required =>
             (@arg URL: -u --url +takes_value "The URL of the file to ingest")
             (@arg FILE: -f --file +takes_value "The path of the file to ingest")
             (@arg ALL: "Download all of PushShift's archives")
            )
    )
    .get_matches();

    if let Some(path) = matches.value_of("FILE") {
        let file = BufReader::new(std::fs::File::open(path).map_err(le!())?);
        if path.ends_with("bz2") {
            ingest_json(bzip2::bufread::BzDecoder::new(file));
        } else if path.ends_with("xz") {
            ingest_json(xz2::bufread::XzDecoder::new(file));
        } else if path.ends_with("zst") {
            ingest_json(zstd::stream::read::Decoder::new(file).map_err(le!())?);
        } else {
            error!("Unknown extension type {}", path);
        }
    } else if let Some(url) = matches.value_of("URL") {
        let resp = BufReader::new(REQW_CLIENT.get(url).send().map_err(le!())?);
        if url.ends_with("bz2") {
            ingest_json(bzip2::bufread::BzDecoder::new(resp));
        } else if url.ends_with("xz") {
            ingest_json(xz2::bufread::XzDecoder::new(resp));
        } else if url.ends_with("zst") {
            ingest_json(zstd::stream::read::Decoder::new(resp).map_err(le!())?);
        } else {
            error!("Unknown file extension {}", url);
        }
    } else if matches.is_present("") {
        for line in BufReader::new(File::open("pushshift_files.tx").map_err(le!())?).lines() {
            let url = "https://files.pushshift.io/reddit/submissions/".to_string()
                + &line.map_err(le!())?;
            let resp = BufReader::new(REQW_CLIENT.get(&url).send().map_err(le!())?);

            info!("Downloading archive {}", url);

            if url.ends_with("bz2") {
                ingest_json(bzip2::bufread::BzDecoder::new(resp));
            } else if url.ends_with("xz") {
                ingest_json(xz2::bufread::XzDecoder::new(resp));
            } else if url.ends_with("zst") {
                ingest_json(zstd::stream::read::Decoder::new(resp).map_err(le!())?);
            } else {
                error!("Unknown file extension {}", url);
            }
        }
    }

    Ok(())
}
