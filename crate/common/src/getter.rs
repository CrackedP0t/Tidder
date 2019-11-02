use super::*;

use reqwest::{RedirectPolicy, StatusCode};
use serde_json::Value;

pub fn new_domain_with_path_re(domain: &str) -> Result<Regex, regex::Error> {
    Regex::new(&format!(
        r"(?i)^https?://(?:[a-z0-9-.]+\.)?{}(?::\d+)?[/?#].+",
        domain.replace(".", r"\.")
    ))
}

pub fn is_link_imgur(link: &str) -> bool {
    lazy_static! {
        static ref IMGUR_LINK_RE: Regex = new_domain_with_path_re("imgur.com").unwrap();
    }

    IMGUR_LINK_RE.is_match(link)
}

pub fn is_link_gfycat(link: &str) -> bool {
    lazy_static! {
        static ref GFYCAT_LINK_RE: Regex = new_domain_with_path_re("gfycat.com").unwrap();
    }

    GFYCAT_LINK_RE.is_match(link)
}

pub fn is_link_gifsound(link: &str) -> bool {
    lazy_static! {
        static ref GIFSOUND_LINK_RE: Regex = new_domain_with_path_re("gifsound.com").unwrap();
    }

    GIFSOUND_LINK_RE.is_match(link)
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

pub async fn follow_link(url: Url) -> Result<String, UserError> {
    let link = if is_link_imgur(url.as_str()) {
        follow_imgur(url).await?
    } else if is_wikipedia_file(url.as_str()) {
        follow_wikipedia(url).await?
    } else if is_link_gifsound(url.as_str()) {
        follow_gifsound(url)?
    } else if EXT_RE.is_match(url.as_str()) {
        url.into_string()
    } else if is_link_gfycat(url.as_str()) {
        follow_gfycat(url).await?
    } else {
        url.into_string()
    };
    Ok(utf8_percent_encode(link.as_str(), QUERY_ENCODE_SET).collect::<String>())
}

fn follow_gifsound(url: Url) -> Result<String, UserError> {
    lazy_static! {
        static ref IMGUR_NO_SCHEME_RE: Regex = Regex::new(r"^(?:[a-z0-9-.]+\.)?imgur.com").unwrap();
    }
    for (key, value) in url.query_pairs() {
        if key == "gif" {
            return Ok(
                if value.starts_with("http://") || value.starts_with("https://") {
                    value.to_string()
                } else if IMGUR_NO_SCHEME_RE.is_match(&value) {
                    format!("https://{}", value)
                } else {
                    format!("http://{}", value)
                },
            );
        } else if key == "gifv" {
            return Ok(format!("https://i.imgur.com/{}.gif", value));
        } else if key == "mp4" || key == "webm" {
            if IMGUR_NO_SCHEME_RE.is_match(&value) {
                return Ok(format!("https://i.imgur.com/{}.gif", value));
            } else {
                return Err(ue_save!(
                    "Unsupported GifSound file",
                    "gifsound_unsupported",
                    Source::User
                ));
            }
        }
    }
    Err(ue_save!(
        "GifSound URL without GIF",
        "gifsound_no_gif",
        Source::User
    ))
}

async fn follow_gfycat(url: Url) -> Result<String, UserError> {
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

    let resp = REQW_CLIENT
        .get(&format!(
            "https://api.gfycat.com/v1/gfycats/{}",
            GFY_ID_SEL
                .captures(url.path())
                .and_then(|c| c.get(1))
                .map(|m| m.as_str())
                .ok_or_else(|| ue_save!(
                    "couldn't find Gfycat ID in link",
                    "gfycat_no_id",
                    Source::User
                ))?
        ))
        .send()
        .await
        .map_err(map_ue!("couldn't connect to GfyCat API"))?
        .error_for_status()
        .map_err(error_for_status_ue)?;

    Ok(resp
        .json::<Gfycats>()
        .map_err(map_ue_save!(
            "problematic JSON from Gfycat API",
            "gfycat_json_bad"
        ))
        .await?
        .gfy_item
        .mobile_poster_url)
}

async fn make_imgur_api_request(api_link: String) -> Result<Value, UserError> {
    lazy_static! {
        static ref API_CLIENT: reqwest::Client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .default_headers({
                let mut headers = COMMON_HEADERS.clone();
                headers.insert(
                    "X-RapidAPI-Host",
                    HeaderValue::from_static("imgur-apiv3.p.rapidapi.com"),
                );
                headers.insert(
                    "X-RapidAPI-Key",
                    HeaderValue::from_static(&SECRETS.imgur.rapidapi_key),
                );
                headers.insert(
                    header::AUTHORIZATION,
                    format!("Client-ID {}", SECRETS.imgur.client_id)
                        .parse()
                        .unwrap(),
                );
                headers
            })
            .build()
            .unwrap();
    }
    let resp = API_CLIENT
        .get(&api_link)
        .send()
        .map_err(map_ue!("couldn't reach Imgur API"))
        .await?;

    let status = resp.status();

    if !status.is_success() {
        let msg = if status == StatusCode::NOT_FOUND {
            format!("Imgur API call to {} returned status {}", api_link, status)
        } else {
            format!(
                "Imgur API call to {} returned status {}: {}",
                api_link,
                status,
                resp.json::<Value>().await?
            )
        };
        return Err(ue_save!(msg, format!("http_{}", status.as_str())));
    }

    if resp
        .headers()
        .get("x-ratelimit-requests-remaining")
        .ok_or(ue!(
            "header `x-ratelimit-requests-remaining` not sent",
            Source::Internal
        ))
        .and_then(|hv| hv.to_str().map_err(map_ue!()))
        .and_then(|s| s.parse::<i64>().map_err(map_ue!()))?
        < 10
    {
        Err(ue!("out of Imgur API requests", Source::Internal))
    } else {
        resp.json::<Value>()
            .map_err(map_ue_save!(
                "Imgur API returned invalid JSON",
                "imgur_json_bad"
            ))
            .await
    }
}

fn get_id(id: &str) -> Option<&str> {
    lazy_static! {
        static ref ID_RE: Regex = Regex::new(r"^[[:alnum:]]+").unwrap();
    }

    if id != "all" {
        ID_RE.find(id).map(|m| m.as_str())
    } else {
        None
    }
}

fn id_segment<'a>(segments: &'a [&str], loc: usize) -> Result<&'a str, UserError> {
    segments
        .get(loc)
        .and_then(|&seg| get_id(seg))
        .ok_or(ue_save!("couldn't find Imgur ID in URL", "imgur_no_id"))
}

fn last_id<'a, 'b: 'a, D>(
    segments: impl IntoIterator<Item = &'a &'b str, IntoIter = D>,
) -> Result<&'b str, UserError>
where
    D: DoubleEndedIterator,
    D: Iterator<Item = &'a &'b str>,
{
    segments
        .into_iter()
        .rev()
        .find_map(|&id| get_id(id))
        .ok_or(ue_save!("couldn't find Imgur ID in URL", "imgur_no_id"))
}

async fn follow_imgur(mut url: Url) -> Result<String, UserError> {
    lazy_static! {
        static ref GIFV_RE: Regex = Regex::new(r"\.(?:gifv|webm|mp4)($|[?#])").unwrap();
        static ref EMPTY_RE: Regex = Regex::new(r"^/\.[[:alnum:]]+\b").unwrap();
        static ref EXT_RE: Regex = Regex::new(r"(?i)[[:alnum:]]\.(?:jpg|png)[[:alnum:]]+").unwrap();
        static ref HOST_LIMIT_RE: Regex =
            Regex::new(r"^(?i).+?\.([a-z0-9-]+\.[a-z0-9-]+\.[a-z0-9-]+)$").unwrap();
        static ref REQW_CLIENT_NO_REDIR: reqwest::r#async::Client =
            reqwest::r#async::Client::builder()
                .timeout(Duration::from_secs(30))
                .redirect(RedirectPolicy::none())
                .build()
                .unwrap();
    }

    let host = url.host_str().ok_or(ue!("no host in Imgur URL"))?;

    if let Some(caps) = HOST_LIMIT_RE.captures(host) {
        let new_host = caps.get(1).unwrap().as_str().to_string();
        url.set_host(Some(&new_host))
            .map_err(map_ue!("couldn't set new host"))?;
    }

    let host = url.host_str().unwrap();

    if EXT_RE.is_match(url.as_str()) {
        return Ok(url.into_string());
    }

    let path = url.path();
    let segments = url
        .path_segments()
        .ok_or(ue!("base Imgur URL", Source::User))?
        .collect::<Vec<_>>();
    let path_start = *segments.first().ok_or(ue!("base Imgur URL"))?;

    if host == "i.imgur.com" && GIFV_RE.is_match(path) {
        Ok(GIFV_RE.replace(url.as_str(), ".gif$1").to_string())
    } else if EXT_RE.is_match(path) || path_start == "download" {
        Ok(url.into_string())
    } else if path_start == "a" {
        let id = id_segment(&segments, 1)?;
        let api_link = format!("https://imgur-apiv3.p.rapidapi.com/3/album/{}/images", id);
        let json = make_imgur_api_request(api_link).await?;
        Ok(GIFV_RE
            .replace(
                json["data"]
                    .get(0)
                    .ok_or(ue_save!("Imgur album is empty", "imgur_album_empty"))?["link"]
                    .as_str()
                    .ok_or(ue_save!(
                        "Imgur API returned unexpectedly-structured JSON",
                        "imgur_json_bad"
                    ))?,
                ".gif$1",
            )
            .to_string())
    } else if path_start == "gallery" {
        let id = id_segment(&segments, 1)?;
        // let image_link = format!("https://i.imgur.com/{}.jpg", id);

        // let resp = REQW_CLIENT_NO_REDIR
        //     .head(&image_link)
        //     .send()
        //     .map_err(map_ue!("couldn't reach Imgur image servers"))
        //     .await?;
        // let resp_url = resp.url().as_str();
        // if resp.status().is_success() {
        let api_link = format!("https://imgur-apiv3.p.rapidapi.com/3/gallery/album/{}", id);
        let json = make_imgur_api_request(api_link).await?;
        Ok(GIFV_RE
            .replace(
                json["data"]["images"]
                    .get(0)
                    .ok_or(ue_save!("Imgur album is empty", "imgur_album_empty"))?["link"]
                    .as_str()
                    .ok_or(ue_save!(
                        "Imgur API returned unexpectedly-structured JSON",
                        "imgur_json_bad"
                    ))?,
                ".gif$1",
            )
            .to_string())
    // } else {
    //     resp.error_for_status_ref()
    //         .map(|_| resp_url.to_string())
    //         .map_err(error_for_status_ue)
    // }
    } else {
        let id = last_id(&segments)?;

        Ok(format!("https://i.imgur.com/{}.jpg", id))
    }
}

async fn follow_wikipedia(url: Url) -> Result<String, UserError> {
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

    let title = WIKIPEDIA_FILE_RE
        .captures(url.as_str())
        .and_then(|c| c.get(1))
        .map(|m| m.as_str())
        .ok_or(ue!("couldn't extract title"))?;

    let title = percent_decode(title.as_bytes())
        .decode_utf8()
        .map_err(map_ue!("couldn't decode title", Source::User))?;

    let api_url = Url::parse_with_params(
        &format!(
            "https://{}/w/api.php",
            url.domain().ok_or(ue!("no domain in Wikipedia URL"))?
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
    .map_err(map_ue!("couldn't create Wikipedia API URL", Source::User))?;

    let resp = REQW_CLIENT
        .get(api_url.as_str())
        .send()
        .map_err(map_ue!("couldn't reach Wikipedia API"))
        .await?
        .error_for_status()
        .map_err(error_for_status_ue)?;

    let api_query = resp
        .json::<APIQuery>()
        .map_err(map_ue!("Wikipedia API returned problematic JSON"))
        .await?;

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

pub enum GetKind {
    Cache(HashDest, i64),
    Request(HeaderMap),
}

pub async fn get_hash(orig_link: &str) -> Result<(Hash, String, GetKind), UserError> {
    lazy_static! {
        static ref EXT_REPLACE_RE: Regex = Regex::new(r"^(.+?)\.[[:alnum:]]+$").unwrap();
    }

    if orig_link.len() > 2000 {
        return Err(ue!("URL too long", Source::User));
    }

    let url = Url::parse(orig_link).map_err(map_ue!("invalid URL", Source::User))?;

    let scheme = url.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(ue!("unsupported scheme in URL", Source::User));
    }

    let is_photobucket = get_tld(&url) == "photobucket.com";

    let mut link = follow_link(url).await?;

    let found = get_existing(&link).await?;

    if let Some((hash, hash_dest, id)) = found {
        return Ok((hash, link, GetKind::Cache(hash_dest, id)));
    }

    let resp = REQW_CLIENT
        .get(&link)
        .header(header::ACCEPT, {
            if is_photobucket {
                &IMAGE_MIMES_NO_WEBP as &[&str]
            } else {
                &IMAGE_MIMES as &[&str]
            }
            .join(",")
        })
        .header(header::USER_AGENT, USER_AGENT)
        .send()
        .map_err(map_ue!("couldn't connect to image host"))
        .await?
        .error_for_status()
        .map_err(error_for_status_ue)?;

    let url = resp.url();
    if url
        .host_str()
        .map(|host| host == "i.imgur.com")
        .unwrap_or(false)
        && url.path() == "/removed.png"
    {
        return Err(ue_save!("removed from Imgur", "imgur_removed"));
    }

    if let Some(ct) = resp.headers().get(header::CONTENT_TYPE) {
        let ct = ct
            .to_str()
            .map_err(map_ue!("non-ASCII Content-Type header"))?;

        if !IMAGE_MIMES.contains(&ct) {
            return Err(ue_save!(
                format!("unsupported Content-Type: {}", ct),
                "content_type_unsupported"
            ));
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

    let headers = resp.headers().to_owned();

    let image = &resp
        .bytes()
        .map_err(map_ue_save!("couldn't download image", "download_image"))
        .await?;

    let hash = match std::panic::catch_unwind(|| hash_from_memory(image)) {
        Ok(r) => r?,
        Err(e) => {
            error!("{} panicked!", orig_link);
            std::panic::resume_unwind(e)
        }
    };

    Ok((hash, link, GetKind::Request(headers)))
}

async fn poss_move_row(
    hash: Hash,
    hash_dest: HashDest,
    found_hash_dest: HashDest,
    id: i64,
) -> Result<(Hash, HashDest, i64, bool), UserError> {
    if hash_dest == found_hash_dest || hash_dest == HashDest::ImageCache {
        Ok((hash, hash_dest, id, true))
    } else {
        let mut client = PG_POOL.take().await?;
        let trans = client.transaction().await?;
        let stmt = trans
            .prepare(
                "INSERT INTO images \
                 (link, hash, no_store, no_cache, expires, etag, \
                 must_revalidate, retrieved_on) \
                 SELECT link, hash, no_store, no_cache, expires, etag, \
                 must_revalidate, retrieved_on FROM image_cache WHERE id = $1 \
                 RETURNING id",
            )
            .await?;

        let new_id = trans.query_one(&stmt, &[&id]).await?.get::<_, i64>("id");

        let stmt = trans
            .prepare("DELETE FROM image_cache WHERE id = $1")
            .await?;
        trans.execute(&stmt, &[&id]).await?;

        trans.commit().await?;

        Ok((hash, HashDest::Images, new_id, true))
    }
}

pub async fn save_hash(
    link: &str,
    hash_dest: HashDest,
) -> Result<(Hash, HashDest, i64, bool), UserError> {
    let (hash, link, get_kind) = get_hash(link).await?;
    match get_kind {
        GetKind::Cache(found_hash_dest, id) => {
            poss_move_row(hash, hash_dest, found_hash_dest, id).await
        }
        GetKind::Request(headers) => {
            let now = chrono::offset::Utc::now().naive_utc();
            let cc: Option<CacheControl> = headers
                .get(header::CACHE_CONTROL)
                .and_then(|hv| hv.to_str().ok())
                .and_then(|s| cache_control::with_str(s).ok());
            let cc = cc.as_ref();

            let mut client = PG_POOL.take().await?;
            let trans = client.transaction().await?;
            let stmt = trans
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
                .await?;

            let rows = trans
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
                                    .and_then(|s| DateTime::parse_from_rfc2822(s).ok())
                                    .map(|dt| dt.naive_utc())
                            }),
                        &headers.get(header::ETAG).and_then(|hv| hv.to_str().ok()),
                        &cc.map(|cc| cc.must_revalidate),
                        &now,
                    ],
                )
                .await?;

            trans.commit().await?;

            match rows.first() {
                Some(row) => Ok((hash, hash_dest, row.get("id"), false)),
                None => {
                    let found = get_existing(&link).await?;
                    match found {
                        Some((hash, found_hash_dest, id)) => {
                            poss_move_row(hash, hash_dest, found_hash_dest, id).await
                        }
                        None => Err(ue!("conflict but no existing match")),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn follow_async() {
        assert_eq!(
            follow_imgur(Url::parse("http://www.i.imgur.com/3EqtHIK.jpg").unwrap())
                .await
                .unwrap(),
            "https://i.imgur.com/3EqtHIK.jpg"
        );

        assert_eq!(
            follow_imgur(Url::parse("http://imgur.com/vyyUWmX,m8YtXvI,Fay1RGQ,DKFJDkI").unwrap())
                .await
                .unwrap(),
            "https://i.imgur.com/vyyUWmX.jpg"
        );
    }

    #[test]
    fn follow_sync() {
        assert_eq!(
            follow_gifsound(
                Url::parse("http://gifsound.com/?gifv=IRRzso8&v=HcuKxAvCSZ4&s=115").unwrap()
            )
            .unwrap(),
            "https://i.imgur.com/IRRzso8.gif"
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
        assert!(!is_link_imgur("https://imgur.com"));
        assert!(!is_link_imgur("https://notimgur.com/3EqtHIK"));
        assert!(!is_link_imgur("http://www.valuatemysite.com/www.imgur.com"));
        assert!(is_link_imgur("https://sub-domain.imgur.com/imageid"));
        assert!(is_link_imgur("https://imgur.com?query=string"));
        assert!(is_link_imgur("HTTPS://IMGUR.COM/3EqtHIK"));
        assert!(is_link_imgur("https://imgur.com#fragment"));
        assert!(is_link_imgur("https://imgur.com:443/imageid"));
        assert!(!is_link_imgur("http://rir.li/http://i.imgur.com/oGqNH.jpg"));
    }

    #[test]
    fn gfycat_links() {
        assert!(is_link_gfycat(
            "https://gfycat.com/excellentclumsyjanenschia-dog"
        ));
        assert!(!is_link_gfycat("https://gfycat.com"));
        assert!(is_link_gfycat("https://developers.gfycat.com/api/"));
        assert!(!is_link_gfycat(
            "https://notgfycat.com/excellentclumsyjanenschia-dog"
        ));
    }

    #[test]
    fn gifsound_links() {
        assert!(is_link_gifsound(
            "http://gifsound.com/?gif=i.imgur.com/IRRzso8.gif&v=HcuKxAvCSZ4&s=115"
        ));
        assert!(is_link_gifsound(
            "https://gifsound.com/?gifv=IRRzso8&v=HcuKxAvCSZ4&s=115"
        ));
    }
}
