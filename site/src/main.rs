use askama::Template;
use common::{dhash, get_image, Hash};
use failure::Error;
use fallible_iterator::FallibleIterator;
use futures::future::{ok, result, Either, Future};
use http::StatusCode;
use hyper::{self, Body};
use hyper_tls::HttpsConnector;
use postgres::{self, NoTls};
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use serde::Deserialize;
use std::vec::Vec;
use url::Url;
use warp::path::{full, FullPath};
use warp::{get2, path, query, reply, Filter, Rejection, Reply};

#[derive(Deserialize)]
struct SearchQuery {
    imageurl: Option<String>,
}

struct Match {
    permalink: String,
    link: String,
    reddit_id: String,
}

#[derive(Template)]
#[template(path = "findings.html")]
struct Findings {
    matches: Vec<Match>,
}

struct Sent {
    url: String,
    findings: Result<Findings, Error>,
}

#[derive(Template)]
#[template(path = "search.html")]
struct Search {
    sent: Option<Sent>,
}

fn reply_not_found(path: FullPath) -> impl Reply {
    reply::with_status(
        reply::html(format!(
            "<h1>Error 404: Not Found</h1><h2>{}</h2>",
            path.as_str()
        )),
        StatusCode::NOT_FOUND,
    )
}

fn run_server() {
    let manager = PostgresConnectionManager::new(
        "dbname=tidder host=/run/postgresql user=postgres"
            .parse()
            .unwrap(),
        NoTls,
    );

    let pool = r2d2::Pool::new(manager).unwrap();

    let https = HttpsConnector::new(4).unwrap();

    let get_search = move |qs: SearchQuery| {
        match qs.imageurl {
            Some(url) => {
                let valid = Url::parse(&url.clone());
                Either::A(
                    result(valid.map_err(Error::from))
                        .and_then(|url| {
                            let url = url.to_string();
                            let h_client = hyper::Client::builder().build::<_, Body>(https.clone());
                            get_image(h_client, url.clone()).and_then(|(status, image)| {
                                let hash = dhash(image);
                                ok(pool
                                    .get()
                                    .unwrap()
                                    .query_iter(
                                        "SELECT reddit_id, link, permalink FROM posts WHERE hash <@ ($1, 10)",
                                        &[&hash],
                                    )
                                    .map(|rows_iter| Search {
                                        sent: Some(Sent {
                                            url: url.clone(),
                                            findings: Ok(Findings {
                                                matches: rows_iter
                                                    .map(|row| {
                                                        Ok(Match {
                                                            permalink: format!("https://reddit.com{}", row.get::<_, &str>("permalink")),
                                                            link: row.get("link"),
                                                            reddit_id: row.get("reddit_id"),
                                                        })
                                                    })
                                                    .collect()
                                                    .unwrap(),
                                            }),
                                        }),
                                    })
                                    .unwrap_or_else(|e| Search {
                                        sent: Some(Sent {
                                            url,
                                            findings: Err(Error::from(e)),
                                        }),
                                    }))
                            })
                        })
                        .or_else(|e| {
                            ok::<Search, Rejection>(Search {
                                sent: Some(Sent {
                                    url,
                                    findings: Err(Error::from(e)),
                                }),
                            })
                        }),
                )
            }
            None => Either::B(ok(Search { sent: None })),
        }
        .map(|tpl| reply::html(tpl.render().unwrap_or_else(|e| e.to_string().clone())))
    };

    let router = path("search")
        .and(query::<SearchQuery>())
        .and(get2())
        .and_then(get_search)
        .or(full().map(reply_not_found));

    warp::serve(router).run(([127, 0, 0, 1], 7878));
}

pub fn main() {
    run_server();
}
