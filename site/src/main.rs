use askama::Template;
use common::*;
use fallible_iterator::FallibleIterator;
use futures::future::{ok, result, Either, Future};
use http::{Response, StatusCode};
use hyper::{self, Body};
use lazy_static::lazy_static;
use postgres::NoTls;
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use serde::Deserialize;
use std::vec::Vec;
use url::Url;
use warp::path::{full, FullPath};
use warp::{get2, path, query, reply, Filter, Rejection, Reply};

#[derive(Deserialize)]
struct SearchQuery {
    imagelink: Option<String>,
    distance: Option<i64>
}

struct Match {
    author: String,
    created_utc: chrono::NaiveDateTime,
    distance: i64,
    link: String,
    permalink: String,
    score: i32,
    subreddit: String,
    title: String,
}

#[derive(Template)]
#[template(path = "findings.html")]
struct Findings {
    matches: Vec<Match>,
}

struct Sent {
    link: String,
    distance: Option<i64>,
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

lazy_static! {
    static ref POOL: r2d2::Pool<PostgresConnectionManager<NoTls>> =
        r2d2::Pool::new(PostgresConnectionManager::new(
            "dbname=tidder host=/run/postgresql user=postgres"
                .parse()
                .unwrap(),
            NoTls,
        ))
        .unwrap();
}

fn get_search(qs: SearchQuery) -> impl Future<Item = Response<Body>, Error = Rejection> {
    match qs.imagelink {
        Some(link) => {
            let valid = Url::parse(&link.clone());
            let distance = qs.distance;
            Either::A(
                result(valid.map_err(Error::from))
                    .and_then(move |link| {
                        let link = link.to_string();
                        let link2 = link.clone();
                        get_hash(link.clone()).map_err(Error::from).and_then(|(hash, _image_id, _exists)| {
                            Ok(match POOL.get() {
                                Ok(mut conn) =>
                                    conn.query_iter(
                                        "SELECT  hash <-> $1 as distance, posts.link, permalink, score, author, created_utc, subreddit, title FROM posts INNER JOIN images ON hash <@ ($1, $2) AND image_id = images.id ORDER BY distance ASC, created_utc DESC",
                                        &[&hash, &distance.unwrap_or(DEFAULT_DISTANCE)],
                                    )
                                    .and_then(move |rows_iter| Ok(Search {
                                        sent: Some(Sent {
                                            link: link2,
                                            distance,
                                            findings: Ok(Findings {
                                                matches: rows_iter
                                                    .map(move |row| {
                                                        Ok(Match {
                                                            permalink: format!("https://reddit.com{}", row.get::<_, &str>("permalink")),
                                                            distance: row.get("distance"),
                                                            link: row.get("link"),
                                                            score: row.get("score"),
                                                            author: row.get("author"),
                                                            created_utc: row.get("created_utc"),
                                                            subreddit: row.get("subreddit"),
                                                            title: row.get("title")
                                                        })
                                                    })
                                                    .collect()?,
                                            }),
                                        }),
                                    }))
                                    .unwrap_or_else(move |e| Search {
                                        sent: Some(Sent {
                                            link,
                                            distance,
                                            findings: Err(Error::from(e)),
                                        }),
                                    }),
                                Err(e) => Search {
                                    sent: Some(Sent {
                                        link,
                                        distance,
                                        findings: Err(Error::from(e))
                                    })
                                }
                            })
                        })
                    })
                    .or_else(move |e| {
                        ok(Search {
                            sent: Some(Sent {
                                link,
                                distance,
                                findings: Err(e),
                            }),
                        })
                    }),
            )
        }
        None => Either::B(ok(Search { sent: None })),
    }
    .map(|tpl| {
        let out = tpl.render();
        Response::builder()
            .status(if out.is_ok() {200} else {500})
            .header("Content-Type",  "text/html")
            .body(Body::from(out.unwrap_or_else(|_| "<h1>Error 500: Internal Server Error</h1>".to_string()))).unwrap()
    })
}

fn run_server() {
    setup_logging();
    let router = path("search")
        .and(get2())
        .and(query::<SearchQuery>().and_then(get_search))
        .or(full().map(reply_not_found));

    warp::serve(router).run(([127, 0, 0, 1], 7878));
}

pub fn main() {
    run_server();
}
