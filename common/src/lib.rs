use failure::{format_err, Error, Fail};
use futures::future::{loop_fn, ok, result, Loop};
use futures::{Future, Stream};
use http::StatusCode;
use hyper::client::connect::Connect;
use hyper::{header, Body, Client, Request};
use image::{imageops, load_from_memory, DynamicImage};
use std::fmt;
use tokio_postgres::{to_sql_checked, types};
use url::percent_encoding::{utf8_percent_encode, QUERY_ENCODE_SET};

#[derive(Debug)]
pub struct Hash(u64);

#[derive(Debug, Fail)]
#[fail(display = "Got status {}", status)]
pub struct StatusFail {
    pub status: StatusCode,
}

#[derive(Debug, Fail)]
#[fail(display = "Getting {} failed: {}", link, error)]
pub struct GetImageFail {
    pub link: String,
    pub error: Error,
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl types::ToSql for Hash {
    fn to_sql(
        &self,
        t: &types::Type,
        w: &mut Vec<u8>,
    ) -> Result<types::IsNull, Box<std::error::Error + Sync + Send>> {
        (self.0 as i64).to_sql(t, w)
    }

    fn accepts(t: &types::Type) -> bool {
        i64::accepts(t)
    }

    to_sql_checked!();
}

pub fn dhash(img: DynamicImage) -> Hash {
    let small_img = imageops::thumbnail(&img.to_luma(), 9, 8);

    let mut hash: u64 = 0;

    for y in 0..8 {
        for x in 0..8 {
            let bit = ((small_img.get_pixel(x, y).data[0] > small_img.get_pixel(x + 1, y).data[0])
                as u64)
                << (x + y * 8);
            hash |= bit;
        }
    }

    Hash(hash)
}

pub fn distance(a: Hash, b: Hash) -> u32 {
    (a.0 ^ b.0).count_ones()
}

pub const IMAGE_MIMES: [&str; 11] = [
    "image/png",
    "image/jpeg",
    "image/gif",
    "image/webp",
    "image/x-portable-anymap",
    "image/tiff",
    "image/x-targa",
    "image/x-tga",
    "image/bmp",
    "image/vnd.microsoft.icon",
    "image/vnd.radiance",
];

pub fn get_image<C>(
    client: Client<C, Body>,
    link: String,
) -> impl Future<Item = (StatusCode, DynamicImage), Error = GetImageFail>
where
    C: 'static + Connect,
{
    let link2 = link.clone();
    let map_gif = |e| GetImageFail {
        link: link2,
        error: e,
    };
    loop_fn((client, link), move |(client, this_link): (_, String)| {
        let this_link = if this_link.starts_with('/') {
            format!("https://reddit.com{}", this_link)
        } else {
            this_link
        };

        result(
            Request::get(utf8_percent_encode(&this_link, QUERY_ENCODE_SET).collect::<String>())
                .header(header::ACCEPT, IMAGE_MIMES.join(","))
                .header(
                    header::USER_AGENT,
                    "Mozilla/5.0 (X11; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0",
                )
                .body(Body::empty()),
        )
        .map_err(Error::from)
        .and_then(|request| {
            client
                .request(request)
                .map_err(Error::from)
                .and_then(move |res| {
                    let status = res.status();
                    if status.is_success() {
                        match res.headers().get(header::CONTENT_TYPE) {
                            Some(ctype) => {
                                let val = ctype.to_str().map_err(Error::from)?;
                                if IMAGE_MIMES.iter().any(|t| *t == val) {
                                    Ok(Loop::Break(res))
                                } else {
                                    Err(format_err!("Got unsupported MIME type {}", val))
                                }
                            }
                            None => Ok(Loop::Break(res)),
                        }
                    } else if status.is_redirection() {
                        Ok(Loop::Continue((
                            client,
                            String::from(
                                res.headers()
                                    .get(header::LOCATION)
                                    .ok_or_else(|| format_err!("Redirected without location"))?
                                    .to_str()
                                    .map_err(Error::from)?,
                            ),
                        )))
                    } else {
                        Err(Error::from(StatusFail { status }))
                    }
                })
        })
    })
    .and_then(|resp| {
        let (parts, body) = resp.into_parts();
        (ok(parts.status), body.concat2().map_err(Error::from))
    })
    .and_then(move |(status, body)| (ok(status), load_from_memory(&body).map_err(Error::from)))
    .map_err(map_gif)
}
