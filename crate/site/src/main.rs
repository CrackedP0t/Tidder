#![type_length_limit = "5802293"]

use common::*;
use futures::TryFutureExt;
use once_cell::sync::Lazy;
use warp::filters::*;
use warp::http::{header, Response, StatusCode};
use warp::path::path;
use warp::{Filter, Rejection};

mod search;
use search::SearchQuery;
mod rankings;

mod render;

#[derive(Debug)]
struct UEReject(UserError);

impl warp::reject::Reject for UEReject {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    Lazy::force(&render::TERA);

    let head = method::head().map(|| StatusCode::OK);

    let router = warp::path::end()
        .and(
            method::get()
                .and(query::query::<SearchQuery>().and_then(|query| async {
                    Ok::<_, Rejection>(search::get_response(query).await)
                }))
                .or(method::post()
                    .and(multipart::form())
                    .and_then(|form| async move {
                        Ok::<_, Rejection>(search::post_response(form).await)
                    }))
                .or(head),
        )
        .or(path("rankings").and(
            method::get()
                .and_then(|| async {
                    rankings::get_response()
                        .map_err(|ue| {
                            println!("{:?}", ue);
                            warp::reject::custom(UEReject(ue))
                        })
                        .await
                })
                .or(head),
        ))
        .or(path("robots.txt").and(
            method::get()
                .and_then(|| async {
                    let out = tokio::fs::read_to_string(concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/templates/robots.txt"
                    ))
                    .await
                    .map_err(|_e| warp::reject::not_found())?;

                    Ok::<_, Rejection>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .header(header::CONTENT_TYPE, "text/plain")
                            .body(out)
                            .unwrap(),
                    )
                })
                .or(head),
        ))
        .with(warp::log("site"));

    let ip: std::net::IpAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1".to_string())
        .parse()
        .map_err(|_| "Invalid IP address")?;
    let port = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "7878".to_string())
        .parse()
        .map_err(|_| "Invalid port number")?;

    println!("Serving on http://{}:{}", ip, port);

    warp::serve(router).run((ip, port)).await;

    Ok(())
}
