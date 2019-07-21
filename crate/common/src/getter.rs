use super::*;

use future::{err, ok, result, Either};
use tokio::prelude::*;

macro_rules! fut_try {
    ($res:expr) => {
        match $res {
            Ok(r) => r,
            Err(e) => return Either::B(err(e)),
        }
    };
    ($res:expr, ) => {
        match $res {
            Ok(r) => r,
            Err(e) => return err(e),
        }
    };
    ($res:expr, $wrap:path) => {
        match $res {
            Ok(r) => r,
            Err(e) => return $wrap(err(e)),
        }
    };
}

macros::multi_either!(4);

lazy_static! {
    static ref REQW_CLIENT: reqwest::r#async::Client = reqwest::r#async::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap();
}

pub fn new_domain_re(domain: &str) -> Result<Regex, regex::Error> {
    Regex::new(&format!(
        r"(?i)^https?://(?:[a-z0-9-.]+\.)?{}(?:[/?#:]|$)",
        domain
    ))
}

pub fn is_link_imgur(link: &str) -> bool {
    lazy_static! {
        static ref IMGUR_LINK_RE: Regex = new_domain_re("imgur.com").unwrap();
    }

    IMGUR_LINK_RE.is_match(link)
}

pub fn is_link_gfycat(link: &str) -> bool {
    lazy_static! {
        static ref GFYCAT_LINK_RE: Regex = new_domain_re("gfycat.com").unwrap();
    }

    GFYCAT_LINK_RE.is_match(link)
}

lazy_static! {
    static ref WIKIPEDIA_FILE_RE: Regex =
               Regex::new(r"(?i)(?:^|\.)(?:wikipedia|wiktionary|wikiquote|wikibooks|wikisource|wikinews|wikiversity|wikispecies|mediawiki|wikidata|wikivoyage|wikimedia).org(?-i)/wiki/((?i:Image|File):[^#?]+)").unwrap();
}

pub fn is_wikipedia_file(link: &str) -> bool {
    WIKIPEDIA_FILE_RE.is_match(link)
}

pub fn is_link_special(link: &str) -> bool {
    is_link_imgur(link) || is_link_gfycat(link) || is_wikipedia_file(link)
}

pub fn is_link_important(link: &str) -> bool {
    is_link_imgur(link) || is_link_gfycat(link)
}

pub fn follow_link(url: Url) -> impl Future<Item = String, Error = UserError> + Send {
    if is_link_imgur(url.as_str()) {
        Box::new(follow_imgur(url)) as _
    } else if is_wikipedia_file(url.as_str()) {
        Box::new(follow_wikipedia(url)) as Box<dyn Future<Item = _, Error = _> + Send>
    } else if EXT_RE.is_match(url.as_str()) {
        Box::new(ok(url.into_string())) as _
    } else if is_link_gfycat(url.as_str()) {
        Box::new(follow_gfycat(url)) as _
    } else {
        Box::new(ok(url.into_string())) as _
    }
}

fn follow_gfycat(url: Url) -> impl Future<Item = String, Error = UserError> + Send {
    lazy_static! {
        static ref GFY_ID_SEL: Regex = Regex::new(r"^/([[:alpha:]]+)").unwrap();
    }

    #[derive(Deserialize)]
    struct GfyItem {
        #[serde(rename = "mobilePosterUrl")]
        mobile_poster_url: String,
    }

    #[derive(Deserialize)]
    struct Gfycats {
        #[serde(rename = "gfyItem")]
        gfy_item: GfyItem,
    }

    Either::A(
        REQW_CLIENT
            .get(&format!(
                "https://api.gfycat.com/v1/gfycats/{}",
                fut_try!(GFY_ID_SEL
                    .captures(url.path())
                    .and_then(|c| c.get(1))
                    .map(|m| m.as_str())
                    .ok_or_else(|| ue!("couldn't find Gfycat ID in link", Source::User)))
            ))
            .send()
            .map_err(map_ue!("couldn't reach Gfycat API"))
            .and_then(|resp| resp.error_for_status().map_err(error_for_status_ue))
            .and_then(|mut resp| {
                resp.json::<Gfycats>()
                    .map_err(map_ue!("invalid JSON from Gfycat API"))
            })
            .map(|gfycats| gfycats.gfy_item.mobile_poster_url),
    )
}

fn follow_imgur(mut url: Url) -> impl Future<Item = String, Error = UserError> + Send {
    lazy_static! {
        static ref IMGUR_SEL: Selector = Selector::parse("meta[property='og:image']").unwrap();
        static ref IMGUR_GIFV_RE: Regex = Regex::new(r"(?i)([^.]+)\.(?:gifv|webm|mp4)$").unwrap();
        static ref IMGUR_EMPTY_RE: Regex = Regex::new(r"^/\.[[:alnum:]]+\b").unwrap();
        static ref IMGUR_EXT_RE: Regex =
            Regex::new(r"(?i)[[:alnum:]]\.(?:jpg|png)[[:alnum:]]+").unwrap();
        static ref HOST_LIMIT_RE: Regex =
            Regex::new(r"^(?i).+?\.([a-z0-9-]+\.[a-z0-9-]+\.[a-z0-9-]+)$").unwrap();
    }

    let host = fut_try!(url.host_str().ok_or(ue!("No host in Imgur URL")));

    if let Some(caps) = HOST_LIMIT_RE.captures(host) {
        let new_host = caps.get(1).unwrap().as_str().to_string();
        fut_try!(url
            .set_host(Some(&new_host))
            .map_err(map_ue!("couldn't set new host")));
    }

    if EXT_RE.is_match(url.as_str()) {
        return Either::B(ok(url.into_string()));
    }

    let path = url.path();
    let link = url.to_string();

    let my_url = url.clone();

    let path_start = fut_try!(my_url
        .path_segments()
        .and_then(|mut ps| ps.next())
        .ok_or(ue!("base Imgur URL", Source::User)))
    .to_owned();

    if IMGUR_GIFV_RE.is_match(path) {
        Either::B(ok(IMGUR_GIFV_RE
            .replace(path, "https://i.imgur.com/$1.gif")
            .to_string()))
    } else if IMGUR_EXT_RE.is_match(path) || path_start == "download" {
        Either::B(ok(url.into_string()))
    } else if path_start == "a" || path_start == "gallery" {
        Either::A(
            REQW_CLIENT
                .get(&link)
                .send()
                .map_err(map_ue!("couldn't reach Imgur"))
                .and_then(move |resp| {
                    if resp.status() == StatusCode::NOT_FOUND && path_start == "gallery" {
                        Either::A(
                            REQW_CLIENT
                                .get(&link.replace("/gallery/", "/a/"))
                                .send()
                                .map_err(map_ue!("couldn't reach Imgur"))
                                .and_then(|resp| {
                                    resp.error_for_status().map_err(error_for_status_ue)
                                }),
                        )
                    } else {
                        Either::B(result(resp.error_for_status().map_err(error_for_status_ue)))
                    }
                })
                .and_then(|resp| {
                    resp.into_body()
                        .concat2()
                        .map_err(map_ue!("couldn't retrieve Imgur page"))
                })
                .and_then(|chunk| {
                    let doc_str = std::str::from_utf8(chunk.as_ref())
                        .map_err(map_ue!("invalid UTF-8 in Imgur page"))?;
                    let doc = Html::parse_document(&doc_str);

                    let og_image = doc
                        .select(&IMGUR_SEL)
                        .next()
                        .and_then(|el| el.value().attr("content"))
                        .ok_or_else(|| ue!("couldn't extract image from Imgur album"))?;

                    let mut image_url =
                        Url::parse(og_image).map_err(map_ue!("invalid image URL from Imgur"))?;
                    image_url.set_query(None); // Maybe take advantage of Imgur's downscaling?
                    if IMGUR_EMPTY_RE.is_match(image_url.path()) {
                        return Err(ue!("empty Imgur album"));
                    }

                    Ok(image_url.into_string())
                }),
        )
    } else {
        Either::B(ok(format!("https://i.imgur.com/{}.jpg", path_start)))
    }
}

fn follow_wikipedia(url: Url) -> impl Future<Item = String, Error = UserError> + Send {
    #[derive(Debug, Deserialize)]
    struct ImageInfo {
        mime: String,
        thumburl: String,
        url: String,
    }
    #[derive(Debug, Deserialize)]
    struct Page {
        imageinfo: Vec<ImageInfo>,
    }
    #[derive(Debug, Deserialize)]
    struct Query {
        pages: std::collections::HashMap<String, Page>,
    }
    #[derive(Debug, Deserialize)]
    struct APIQuery {
        query: Query,
    }

    let title = fut_try!(WIKIPEDIA_FILE_RE
        .captures(url.as_str())
        .and_then(|c| c.get(1))
        .map(|m| m.as_str())
        .ok_or(ue!("couldn't extract title")));

    let title = fut_try!(percent_decode(title.as_bytes())
        .decode_utf8()
        .map_err(map_ue!("couldn't decode title", Source::User)));

    let api_url = fut_try!(Url::parse_with_params(
        &format!(
            "https://{}/w/api.php",
            fut_try!(url.domain().ok_or(ue!("no domain in Wikipedia URL")))
        ),
        &[
            ("action", "query"),
            ("format", "json"),
            ("prop", "imageinfo"),
            ("iiprop", "url|mime"),
            ("iiurlwidth", "500"),
            ("titles", &title),
        ],
    )
    .map_err(map_ue!("couldn't create Wikipedia API URL", Source::User)));

    Either::A(
        REQW_CLIENT
            .get(api_url)
            .send()
            .map_err(map_ue!("couldn't query Wikipedia API"))
            .and_then(|mut resp| {
                resp.json::<APIQuery>()
                    .map_err(map_ue!("Wikipedia API returned problematic JSON"))
            })
            .and_then(|api_query| {
                let imageinfo = api_query
                    .query
                    .pages
                    .into_iter()
                    .next()
                    .ok_or(ue!("Wikipedia API returned no pages", Source::User))?
                    .1
                    .imageinfo
                    .into_iter()
                    .nth(0)
                    .ok_or(ue!("Wikipedia API returned no images", Source::User))?;

                Ok(if IMAGE_MIMES.contains(&imageinfo.mime.as_str()) {
                    imageinfo.url
                } else {
                    imageinfo.thumburl
                })
            }),
    )
}

pub fn get_hash(
    link: String,
) -> impl Future<Item = (Hash, String, GetKind), Error = UserError> + Send {
    lazy_static! {
        static ref EXT_REPLACE_RE: Regex = Regex::new(r"^(.+?)\.[[:alnum:]]+$").unwrap();
    }

    if link.len() > 2000 {
        return Either::B(err(ue!("URL too long", Source::User)));
    }

    let url = fut_try!(Url::parse(&link).map_err(map_ue!("not a valid URL", Source::User)));

    let scheme = url.scheme();
    if scheme != "http" && scheme != "https" {
        return Either::B(err(ue!("unsupported scheme in URL", Source::User)));
    }

    Either::A(follow_link(url).and_then(|mut link| {
        if let Some((hash, hash_dest, id)) = fut_try!(get_existing(&link)) {
            return Either::B(ok((hash, link, GetKind::Cache(hash_dest, id))));
        }

        Either::A(
            REQW_CLIENT
                .get(&utf8_percent_encode(&link, QUERY_ENCODE_SET).collect::<String>())
                .header(header::ACCEPT, IMAGE_MIMES.join(","))
                .header(
                    header::USER_AGENT,
                    "Mozilla/5.0 (X11; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0",
                )
                .send()
                .map_err(map_ue!("couldn't connect to image host"))
                .and_then(|resp| resp.error_for_status().map_err(error_for_status_ue))
                .and_then(|resp| {
                    let url = resp.url();
                    if url
                        .host_str()
                        .map(|host| host == "i.imgur.com")
                        .unwrap_or(false)
                        && url.path() == "/removed.png"
                    {
                        return err(ue!("removed from Imgur"));
                    }

                    if let Some(ct) = resp.headers().get(header::CONTENT_TYPE) {
                        let ct = fut_try!(ct
                            .to_str()
                            .map_err(map_ue!("non-ASCII Content-Type header")),);

                        if !IMAGE_MIMES.contains(&ct) {
                            return err(ue!(format!("unsupported Content-Type: {}", ct)));
                        }

                        if url
                            .host_str()
                            .map(|host| host == "i.imgur.com")
                            .unwrap_or(false)
                        {
                            link = EXT_REPLACE_RE
                                .replace(
                                    &link,
                                    format!("$1.{}", ct.split('/').nth(1).unwrap()).as_str(),
                                )
                                .to_owned().to_string();
                        }
                    }

                    ok((
                        link,
                        resp.headers().to_owned(),
                        resp.into_body()
                            .concat2()
                            .map_err(map_ue!("couldn't download image", Source::External)),
                    ))
                })
                .and_then(|(link, headers, fut)| (ok(link), ok(headers), fut))
                .and_then(
                    |(link, headers, image)| match hash_from_memory(image.as_ref()) {
                        Ok(hash) => ok((hash, link, GetKind::Request(headers))),
                        Err(e) => err(e),
                    },
                ),
        )
    }))
}

pub fn save_hash(
    link: String,
    hash_dest: HashDest,
) -> impl Future<Item = (Hash, HashDest, i64, bool), Error = UserError> + Send {
    get_hash(link).and_then(move |(hash, link, get_kind)| {
        let inner_result = move || {
            let poss_move_row = |hash: Hash,
                                 found_hash_dest: HashDest,
                                 id: i64|
             -> Result<(Hash, HashDest, i64, bool), UserError> {
                if hash_dest == found_hash_dest || hash_dest == HashDest::ImageCache {
                    Ok((hash, hash_dest, id, true))
                } else {
                    let mut client = DB_POOL.get().map_err(map_ue!())?;
                    let mut trans = client.transaction().map_err(map_ue!())?;
                    let rows = trans
                        .query(
                            "INSERT INTO images \
                             (link, hash, no_store, no_cache, expires, etag, \
                             must_revalidate, retrieved_on) \
                             VALUES (SELECT link, hash, no_store, no_cache, expires, etag,\
                             must_revalidate, retrieved_on FROM image_cache WHERE id = $1) \
                             RETURNING id",
                            &[&id],
                        )
                        .map_err(map_ue!())?;
                    trans.commit().map_err(map_ue!())?;

                    let mut trans = client.transaction().map_err(map_ue!())?;
                    trans
                        .query("DELETE FROM image_cache WHERE id = $1", &[&id])
                        .map_err(map_ue!())?;
                    trans.commit().map_err(map_ue!())?;

                    let id = rows
                        .get(0)
                        .and_then(|row| row.get("id"))
                        .unwrap_or_else(|| unreachable!());

                    Ok((hash, HashDest::Images, id, true))
                }
            };

            match get_kind {
                GetKind::Cache(hash_dest, id) => poss_move_row(hash, hash_dest, id),
                GetKind::Request(headers) => {
                    let now = chrono::offset::Utc::now().naive_utc();
                    let cc: Option<CacheControl> = headers
                        .get(header::CACHE_CONTROL)
                        .and_then(|hv| hv.to_str().ok())
                        .and_then(|s| cache_control::with_str(s).ok());
                    let cc = cc.as_ref();

                    let mut client = DB_POOL.get().map_err(map_ue!())?;
                    let mut trans = client.transaction().map_err(map_ue!())?;
                    let rows = trans
                        .query(
                            format!(
                                "INSERT INTO {} (link, hash, no_store, no_cache, expires, \
                                 etag, must_revalidate, retrieved_on) \
                                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
                                 ON CONFLICT DO NOTHING \
                                 RETURNING id",
                                hash_dest.table_name()
                            )
                            .as_str(),
                            &[
                                &link,
                                &hash,
                                &cc.map(|cc| cc.no_store),
                                &cc.map(|cc| cc.no_cache),
                                &cc.and_then(|cc| cc.max_age)
                                    .map(|n| NaiveDateTime::from_timestamp(n as i64, 0))
                                    .or_else(|| {
                                        headers
                                            .get(header::EXPIRES)
                                            .and_then(|hv| hv.to_str().ok())
                                            .and_then(|s| DateTime::parse_from_rfc2822(s).ok())
                                            .map(|dt| dt.naive_utc())
                                    }),
                                &headers.get(header::ETAG).and_then(|hv| hv.to_str().ok()),
                                &cc.map(|cc| cc.must_revalidate),
                                &now,
                            ],
                        )
                        .map_err(map_ue!())?;
                    trans.commit().map_err(map_ue!())?;

                    match rows.get(0) {
                        Some(row) => Ok((
                            hash,
                            hash_dest,
                            row.try_get("id").map_err(map_ue!())?,
                            false,
                        )),
                        None => get_existing(&link)?
                            .map(|(hash, hash_dest, id)| poss_move_row(hash, hash_dest, id))
                            .ok_or_else(|| ue!("conflict but no existing match"))?,
                    }
                }
            }
        };
        result(inner_result())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn follow() {
        assert_eq!(
            follow_imgur(Url::parse("http://www.i.imgur.com/3EqtHIK.jpg").unwrap())
                .wait()
                .unwrap(),
            "http://i.imgur.com/3EqtHIK.jpg"
        );
    }
}
