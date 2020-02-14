use common::*;
use http::StatusCode;
use serde::Serialize;
use tera::Context;

#[derive(Serialize)]
struct Rankings {
    as_of: String,
    common_images: Vec<CommonImage>,
}

pub async fn get_response() -> Result<impl warp::Reply, UserError> {
    let images: CommonImages = ron::de::from_reader(std::fs::File::open(
        std::env::var("HOME")? + "/stats/top100.ron",
    )?)?;

    let rankings = Rankings {
        as_of: images.as_of.format("%F %T %Z").to_string(),
        common_images: images.common_images,
    };

    let tera = super::get_tera!();

    let out = tera.render("rankings.html", &Context::from_serialize(&rankings)?)?;

    Ok(warp::reply::with_status(
        warp::reply::html(out),
        StatusCode::OK,
    ))
}
