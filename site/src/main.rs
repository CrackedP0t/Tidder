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
    imageurl: Option<String>,
}

struct Match {
    permalink: String,
    link: String,
    author: String,
    score: i32,
    created_utc: chrono::NaiveDateTime,
    subreddit: String,
    title: String,
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
    match qs.imageurl {
        Some(url) => {
            let valid = Url::parse(&url.clone());
            Either::A(
                result(valid.map_err(Error::from))
                    .and_then(|url| {
                        let url = url.to_string();
                        let url2 = url.clone();
                        get_hash(url.clone()).map_err(Error::from).and_then(|(hash, _image_id)| {
                            Ok(match POOL.get() {
                                Ok(mut conn) =>
                                    conn.query_iter(
                                        "SELECT posts.link, permalink, score, author, created_utc, subreddit, title FROM posts INNER JOIN images ON hash <@ ($1, 9) AND image_id = images.id ORDER BY created_utc DESC",
                                        &[&hash],
                                    )
                                    .map(move |rows_iter| Search {
                                        sent: Some(Sent {
                                            url: url2,
                                            findings: Ok(Findings {
                                                matches: rows_iter
                                                    .map(move |row| {
                                                        Ok(Match {
                                                            permalink: format!("https://reddit.com{}", row.get::<_, &str>("permalink")),
                                                            link: row.get("link"),
                                                            score: row.get("score"),
                                                            author: row.get("author"),
                                                            created_utc: row.get("created_utc"),
                                                            subreddit: row.get("subreddit"),
                                                            title: row.get("title")
                                                        })
                                                    })
                                                    .collect()
                                                    .unwrap(),
                                            }),
                                        }),
                                    })
                                    .unwrap_or_else(move |e| Search {
                                        sent: Some(Sent {
                                            url,
                                            findings: Err(Error::from(e)),
                                        }),
                                    }),
                                Err(e) => Search {
                                    sent: Some(Sent {
                                        url,
                                        findings: Err(Error::from(e))
                                    })
                                }
                            })
                        })
                    })
                    .or_else(|e| {
                        ok(Search {
                            sent: Some(Sent {
                                url,
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
