use super::*;

use future::{err, ok, result, Either};
use reqwest::{RedirectPolicy, StatusCode};
use serde_json::Value;
use std::time::Duration;
use tokio::prelude::*;

lazy_static! {
    static ref REQW_CLIENT: reqwest::r#async::Client = reqwest::r#async::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
}

pub fn new_domain_re(domain: &str) -> Result<Regex, regex::Error> {
    Regex::new(&format!(
        r"(?i)^https?://(?:[a-z0-9-.]+\.)?{}(?:[/?#:]|$)",
        domain.replace(".", r"\.")
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
               Regex::new(r"(?i)^(?:[^.]+\.)?(?:wikipedia|wiktionary|wikiquote|wikibooks|wikisource|wikinews|wikiversity|wikispecies|mediawiki|wikidata|wikivoyage|wikimedia).org(?-i)/wiki/((?i:Image|File):[^#?]+)").unwrap();
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
    .map(|link: String| utf8_percent_encode(link.as_str(), QUERY_ENCODE_SET).collect::<String>())
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

fn make_imgur_api_request(api_link: String) -> impl Future<Item = Value, Error = UserError> {
    REQW_CLIENT
        .get(&api_link)
        .header(
            header::AUTHORIZATION,
            format!("Client-ID {}", SECRETS.imgur.client_id),
        )
        .header("X-RapidAPI-Key", SECRETS.imgur.rapidapi_key.as_str())
        .send()
        .map_err(map_ue!("couldn't reach Imgur API"))
        .and_then(|resp| resp.error_for_status().map_err(error_for_status_ue))
        .and_then(|mut resp| {
            if fut_try!(resp
                .headers()
                .get("x-ratelimit-requests-remaining")
                .ok_or(ue!(
                    "header `x-ratelimit-requests-remaining` not sent",
                    Source::Internal
                ))
                .and_then(|hv| hv.to_str().map_err(map_ue!()))
                .and_then(|s| s.parse::<i64>().map_err(map_ue!())))
                < 10
            {
                Either::B(err(ue!("out of Imgur API requests", Source::Internal)))
            } else {
                Either::A(
                    resp.json::<Value>()
                        .map_err(map_ue!("Imgur API returned invalid JSON")),
                )
            }
        })
}

fn follow_imgur(mut url: Url) -> impl Future<Item = String, Error = UserError> + Send {
    lazy_static! {
        static ref ID_RE: Regex = Regex::new(r"^[[:alnum:]]+").unwrap();
        static ref GIFV_RE: Regex = Regex::new(r"\.(?:gifv|webm|mp4)($|[?#])").unwrap();
        static ref EMPTY_RE: Regex = Regex::new(r"^/\.[[:alnum:]]+\b").unwrap();
        static ref EXT_RE: Regex = Regex::new(r"(?i)[[:alnum:]]\.(?:jpg|png)[[:alnum:]]+").unwrap();
        static ref HOST_LIMIT_RE: Regex =
            Regex::new(r"^(?i).+?\.([a-z0-9-]+\.[a-z0-9-]+\.[a-z0-9-]+)$").unwrap();
        static ref REQW_CLIENT_NO_REDIR: reqwest::r#async::Client =
            reqwest::r#async::Client::builder()
                .timeout(Duration::from_secs(45))
                .redirect(RedirectPolicy::none())
                .build()
                .unwrap();
    }

    let host = fut_try!(url.host_str().ok_or(ue!("No host in Imgur URL")));

    if let Some(caps) = HOST_LIMIT_RE.captures(host) {
        let new_host = caps.get(1).unwrap().as_str().to_string();
        fut_try!(url
            .set_host(Some(&new_host))
            .map_err(map_ue!("couldn't set new host")));
    }

    let host = url.host_str().unwrap();

    if EXT_RE.is_match(url.as_str()) {
        return Either::B(ok(url.into_string()));
    }

    let path = url.path();

    let path_start = fut_try!(url
        .path_segments()
        .and_then(|mut ps| ps.next())
        .ok_or(ue!("base Imgur URL", Source::User)))
    .to_owned();

    if host == "i.imgur.com" && GIFV_RE.is_match(path) {
        Either::B(ok(GIFV_RE.replace(url.as_str(), ".gif$1").to_string()))
    } else if EXT_RE.is_match(path) || path_start == "download" {
        Either::B(ok(url.into_string()))
    } else if path_start == "a" {
        let id = url.path_segments().unwrap().next_back().unwrap();
        let api_link = format!("https://imgur-apiv3.p.rapidapi.com/3/album/{}/images", id);
        Either::A(
            Box::new(make_imgur_api_request(api_link).and_then(move |json| {
                Ok(GIFV_RE
                    .replace(
                        json["data"].get(0).ok_or(ue!("Imgur album is empty"))?["link"]
                            .as_str()
                            .ok_or(ue!("Imgur API returned unexpectedly-structured JSON"))?,
                        ".gif$1",
                    )
                    .to_string())
            })) as Box<dyn Future<Item = _, Error = _> + Send>,
        )
    } else if path_start == "gallery" {
        let id = url.path_segments().unwrap().next_back().unwrap().to_owned();
        let image_link = format!("https://i.imgur.com/{}.jpg", id);

        Either::A(Box::new(
            REQW_CLIENT_NO_REDIR
                .head(&image_link)
                .send()
                .map_err(map_ue!("couldn't reach Imgur image servers"))
                .and_then(move |resp| {
                    let resp_url = resp.url().as_str();
                    if resp.status() == StatusCode::FOUND {
                        let api_link =
                            format!("https://imgur-apiv3.p.rapidapi.com/3/gallery/album/{}", id);
                        Either::A(make_imgur_api_request(api_link).and_then(|json| {
                            let to = GIFV_RE
                                .replace(
                                    json["data"]["images"]
                                        .get(0)
                                        .ok_or(ue!("Imgur album is empty"))?["link"]
                                        .as_str()
                                        .ok_or(ue!(
                                            "Imgur API returned unexpectedly-structured JSON"
                                        ))?,
                                    ".gif$1",
                                )
                                .to_string();
                            Ok(to)
                        }))
                    } else {
                        Either::B(result(
                            resp.error_for_status_ref()
                                .map(|_| resp_url.to_string())
                                .map_err(error_for_status_ue),
                        ))
                    }
                }),
        ))
    } else {
        let id = fut_try!(url
            .path_segments()
            .unwrap()
            .next_back()
            .and_then(|seg| ID_RE.find(seg))
            .ok_or(ue!("Couldn't find Imgur ID")))
        .as_str();

        Either::B(ok(format!("https://i.imgur.com/{}.jpg", id)))
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
            .map_err(map_ue!("couldn't reach Wikipedia API"))
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

pub fn get_tld(url: &Url) -> &str {
    lazy_static! {
        static ref TLD_RE: Regex = Regex::new(r"([^.]+\.[^.]+)$").unwrap();
    }

    url.domain()
        .and_then(|s| TLD_RE.find(s))
        .map(|m| m.as_str())
        .unwrap_or_else(|| url.host_str().unwrap())
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

    let is_photobucket = get_tld(&url) == "photobucket.com";

    Either::A(follow_link(url).and_then(move |mut link| {
        get_existing(link.clone()).and_then(move |found| {
            if let Some((hash, hash_dest, id)) = found {
                return Either::B(ok((hash, link, GetKind::Cache(hash_dest, id))));
            }

            Either::A(
                REQW_CLIENT
                    .get(&link)
                    .header(header::ACCEPT, {
                        if is_photobucket {
                            &IMAGE_MIMES_NO_WEBP as &[&str]
                        } else {
                            &IMAGE_MIMES as &[&str]
                        }
                        .join(",")
                    })
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
                                let new_ext = ct.split('/').nth(1).unwrap();
                                let new_ext = if new_ext == "jpeg" { "jpg" } else { new_ext };
                                link = EXT_REPLACE_RE
                                    .replace(&link, format!("$1.{}", new_ext).as_str())
                                    .to_owned()
                                    .to_string();
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
        })
    }))
}

fn poss_move_row(
    hash: Hash,
    hash_dest: HashDest,
    found_hash_dest: HashDest,
    id: i64,
) -> impl Future<Item = (Hash, HashDest, i64, bool), Error = UserError> {
    if hash_dest == found_hash_dest || hash_dest == HashDest::ImageCache {
        Either::B(ok((hash, hash_dest, id, true)))
    } else {
        Either::A(PG_POOL.take().and_then(move |mut client| {
            client
                .build_transaction()
                .build(
                    client
                        .prepare(
                            "INSERT INTO images \
                             (link, hash, no_store, no_cache, expires, etag, \
                             must_revalidate, retrieved_on) \
                             VALUES (SELECT link, hash, no_store, no_cache, expires, etag,\
                             must_revalidate, retrieved_on FROM image_cache WHERE id = $1) \
                             RETURNING id",
                        )
                        .and_then(move |stmt| {
                            client
                                .query(&stmt, &[&id])
                                .into_future()
                                .map_err(|(e, _)| e)
                                .map(|(row, _)| row.unwrap().get::<_, i64>("id"))
                                .and_then(move |new_id| {
                                    client
                                        .prepare("DELETE FROM image_cache WHERE id = $1")
                                        .join(ok(new_id))
                                        .and_then(move |(stmt, new_id)| {
                                            (ok(new_id), client.execute(&stmt, &[&id]))
                                        })
                                })
                        }),
                )
                .map(move |(new_id, _modified)| (hash, HashDest::Images, new_id, true))
                .map_err(map_ue!())
        }))
    }
}

pub fn save_hash(
    link: String,
    hash_dest: HashDest,
) -> impl Future<Item = (Hash, HashDest, i64, bool), Error = UserError> + Send {
    get_hash(link).and_then(move |(hash, link, get_kind)| match get_kind {
        GetKind::Cache(found_hash_dest, id) => {
            Either::A(poss_move_row(hash, hash_dest, found_hash_dest, id))
        }
        GetKind::Request(headers) => {
            let now = chrono::offset::Utc::now().naive_utc();
            let cc: Option<CacheControl> = headers
                .get(header::CACHE_CONTROL)
                .and_then(|hv| hv.to_str().ok())
                .and_then(|s| cache_control::with_str(s).ok());
            Either::B(PG_POOL.take().and_then(move |mut client| {
                client.build_transaction().build(
                    client
                        .prepare(
                            format!(
                                "INSERT INTO {} (link, hash, no_store, no_cache, expires, \
                                 etag, must_revalidate, retrieved_on) \
                                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
                                 ON CONFLICT DO NOTHING \
                                 RETURNING id",
                                hash_dest.table_name()
                            )
                            .as_str(),
                        )
                        .and_then(move |stmt| {
                            let cc = cc.as_ref();

                            let query = client
                                .query(
                                    &stmt,
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
                                                    .and_then(|s| {
                                                        DateTime::parse_from_rfc2822(s).ok()
                                                    })
                                                    .map(|dt| dt.naive_utc())
                                            }),
                                        &headers.get(header::ETAG).and_then(|hv| hv.to_str().ok()),
                                        &cc.map(|cc| cc.must_revalidate),
                                        &now,
                                    ],
                                )
                                .into_future()
                                .map_err(|(e, _)| e);

                            (ok(link), query)
                        })
                        .map_err(map_ue!())
                        .and_then(move |(link, (row, _))| match row {
                            Some(row) => Either::B(ok((hash, hash_dest, row.get("id"), false))),
                            None => {
                                Either::A(get_existing(link).and_then(move |found| match found {
                                    Some((hash, found_hash_dest, id)) => Either::A(poss_move_row(
                                        hash,
                                        hash_dest,
                                        found_hash_dest,
                                        id,
                                    )),
                                    None => Either::B(err(ue!("conflict but no existing match"))),
                                }))
                            }
                        }),
                )
            }))
        }
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

    #[test]
    fn wikipedia_files() {
        assert!(is_wikipedia_file(
            "https://commons.wikimedia.org/wiki/File:Kalidas_1931_Songbook.JPG"
        ));
        assert!(!is_wikipedia_file(
            "http://en.www.wikipedia.org/wiki/File:Virtual-Boy-Set.png"
        ));
    }

    #[test]
    fn imgur_links() {
        assert!(is_link_imgur("https://i.imgur.com/3EqtHIK.jpg"));
        assert!(is_link_imgur("https://imgur.com/3EqtHIK"));
        assert!(is_link_imgur("http://imgur.com/3EqtHIK"));
        assert!(is_link_imgur("https://imgur.com"));
        assert!(!is_link_imgur("https://notimgur.com/3EqtHIK"));
        assert!(!is_link_imgur("http://www.valuatemysite.com/www.imgur.com"));
        assert!(is_link_imgur("https://sub-domain.imgur.com"));
        assert!(is_link_imgur("https://imgur.com?query=string"));
        assert!(is_link_imgur("HTTPS://IMGUR.COM/3EqtHIK"));
        assert!(is_link_imgur("https://imgur.com#fragment"));
        assert!(is_link_imgur("https://imgur.com:443"));
        assert!(!is_link_imgur("http://rir.li/http://i.imgur.com/oGqNH.jpg"));
    }
    #[test]
    fn gfycat_links() {
        assert!(is_link_gfycat(
            "https://gfycat.com/excellentclumsyjanenschia-dog"
        ));
        assert!(is_link_gfycat("https://gfycat.com"));
        assert!(is_link_gfycat("https://developers.gfycat.com/api/"));
        assert!(!is_link_gfycat(
            "https://notgfycat.com/excellentclumsyjanenschia-dog"
        ));
    }
}
