use cache_control::CacheControl;
use chrono::{DateTime, NaiveDateTime};
use image::{imageops, load_from_memory, DynamicImage};
use lazy_static::lazy_static;
use log::LevelFilter;
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use regex::Regex;
use reqwest::{
    header::{self, HeaderMap},
    Response, StatusCode,
};
use scraper::{Html, Selector};
use serde::Deserialize;
use std::borrow::Cow;
use std::fmt::{self, Display};
use std::io::{BufReader, Read};
use std::string::ToString;
use tokio_postgres::{to_sql_checked, types, NoTls};
use url::{
    percent_encoding::{utf8_percent_encode, QUERY_ENCODE_SET},
    Url,
};

pub use failure::{self, format_err, Error};
pub use log::{error, info, warn};

lazy_static! {
    pub static ref EXT_RE: Regex =
        Regex::new(r"\W(?:png|jpe?g|gif|webp|p[bgpn]m|tiff?|bmp|ico|hdr)\b").unwrap();
}

// Log Error, returning empty
#[macro_export]
macro_rules! le {
    () => {
        |e| error!("{}", e)
    };
}

// Log Error as Info, returning empty
#[macro_export]
macro_rules! lei {
    () => {
        |e| info!("{}", e)
    };
}

pub mod user_error {
    use failure::Error;
    use reqwest::StatusCode;
    use serde::Serialize;
    use std::fmt::{self, Debug, Display, Formatter};

    #[derive(Debug, Serialize)]
    pub enum Source {
        Internal,
        External,
        User,
    }

    #[derive(Debug, Serialize)]
    pub struct UserError {
        pub user_msg: String,
        // #[serde(serialize_with = "UserError::serialize_status_code")]
        pub source: Source,
        #[serde(skip)]
        pub error: Error,
        #[serde(skip)]
        pub file: Option<&'static str>,
        #[serde(skip)]
        pub line: Option<u32>,
    }

    impl UserError {
        // #[allow(clippy::trivially_copy_pass_by_ref)]
        // pub fn serialize_status_code<S>(sc: &SC, ser: S) -> Result<S::Ok, S::Error>
        // where
        //     S: Serializer,
        // {
        //     ser.serialize_u16(sc.as_u16())
        // }
        pub fn new<M: ToString, E: Into<Error>>(user_msg: M, error: E) -> Self {
            Self {
                source: Source::External,
                user_msg: user_msg.to_string(),
                error: error.into(),
                file: None,
                line: None,
            }
        }
        pub fn new_source<M: ToString, E: Into<Error>>(
            user_msg: M,
            source: Source,
            error: E,
        ) -> Self {
            Self {
                source,
                user_msg: user_msg.to_string(),
                error: error.into(),
                file: None,
                line: None,
            }
        }
        pub fn new_msg<M: Display + Debug + Send + Sync + 'static>(user_msg: M) -> Self {
            Self {
                source: Source::External,
                user_msg: user_msg.to_string(),
                error: failure::err_msg(user_msg),
                file: None,
                line: None,
            }
        }
        pub fn new_msg_source<M: Display + Debug + Send + Sync + 'static>(
            user_msg: M,
            source: Source,
        ) -> Self {
            Self {
                source,
                user_msg: user_msg.to_string(),
                error: failure::err_msg(user_msg),
                file: None,
                line: None,
            }
        }
        pub fn from_std<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
            Self {
                source: Source::Internal,
                user_msg: "internal error".to_string(),
                error: error.into(),
                file: None,
                line: None,
            }
        }

        pub fn status_code(&self) -> StatusCode {
            match self.source {
                Source::Internal => StatusCode::INTERNAL_SERVER_ERROR,
                Source::External => StatusCode::OK,
                Source::User => StatusCode::BAD_REQUEST,
            }
        }
    }

    impl From<Error> for UserError {
        fn from(error: Error) -> Self {
            Self {
                source: Source::Internal,
                user_msg: "internal error".to_string(),
                error,
                file: None,
                line: None,
            }
        }
    }

    impl Display for UserError {
        fn fmt(&self, f: &mut Formatter) -> fmt::Result {
            Display::fmt(&self.error, f)
        }
    }

    #[macro_export]
    macro_rules! ue {
        ($msg:expr) => {
            UserError {
                file: Some(file!()),
                line: Some(line!()),
                ..UserError::new_msg($msg)
            }
        };
        ($msg:expr, $source:expr) => {
            UserError {
                file: Some(file!()),
                line: Some(line!()),
                ..UserError::new_msg_source($msg, $source)
            }
        };
    }

    #[macro_export]
    macro_rules! map_ue {
        () => {
            |e| UserError {
                file: Some(file!()),
                line: Some(line!()),
                ..UserError::from_std(e)
            }
        };
        ($msg:expr) => {
            |e| UserError {
                file: Some(file!()),
                line: Some(line!()),
                ..UserError::new($msg, Error::from(e))
            }
        };
        ($msg:expr, $source:expr) => {
            |e| UserError {
                file: Some(file!()),
                line: Some(line!()),
                ..UserError::new_source($msg, $source, Error::from(e))
            }
        };
    }
}

pub use user_error::*;

pub const DEFAULT_DISTANCE: i64 = 1;

#[derive(Deserialize, Debug)]
pub struct Submission {
    pub id_int: i64,
    pub id: String,
    pub author: Option<String>,
    pub created_utc: i64,
    pub is_self: bool,
    pub over_18: bool,
    pub permalink: String,
    pub score: i64,
    pub spoiler: Option<bool>,
    pub subreddit: String,
    pub title: String,
    pub url: String,
}

#[derive(Deserialize, Debug)]
pub struct Hit {
    #[serde(rename = "_source")]
    pub source: Submission,
}

#[derive(Deserialize, Debug)]
pub struct Hits {
    pub hits: Vec<Hit>,
}

#[derive(Deserialize, Debug)]
pub struct PushShiftSearch {
    pub hits: Hits,
}

pub fn save_post(
    pool: &r2d2::Pool<PostgresConnectionManager<NoTls>>,
    post: &Submission,
    image_id: i64,
) -> Result<bool, UserError> {
    lazy_static! {
        static ref ID_RE: Regex = Regex::new(r"/comments/([^/]+)/").unwrap();
    }

    let reddit_id = String::from(
        ID_RE
            .captures(&post.permalink)
            .and_then(|cap| cap.get(1))
            .ok_or_else(|| ue!("Couldn't find ID in permalink"))?
            .as_str(),
    );

    let mut client = pool.get().map_err(map_ue!())?;
    let mut trans = client.transaction().map_err(map_ue!())?;
    let modified = trans
        .execute(
            "INSERT INTO posts (reddit_id, link, permalink, author, created_utc, score, subreddit, title, nsfw, spoiler, image_id, reddit_id_int) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
             ON CONFLICT DO NOTHING",
            &[
                &reddit_id,
                &post.url,
                &post.permalink,
                &post.author,
                &(NaiveDateTime::from_timestamp(post.created_utc, 0)),
                &post.score,
                &post.subreddit,
                &post.title,
                &post.over_18,
                &post.spoiler.unwrap_or(false),
                &image_id,
                &i64::from_str_radix(&reddit_id, 36).map_err(map_ue!())?
            ],
        ).map_err(map_ue!())?;

    trans.commit().map_err(map_ue!())?;

    Ok(modified == 0)
}

#[derive(Debug)]
pub struct Hash(u64);

impl Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.0, f)
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
            let bit = ((small_img.get_pixel(x, y)[0] > small_img.get_pixel(x + 1, y)[0]) as u64)
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

pub fn hash_from_memory(image: &[u8]) -> Result<Hash, UserError> {
    Ok(dhash(
        // match format {
        //     Some(format) => load_from_memory_with_format(&file, format),
        // None =>
        load_from_memory(&image)
            // }
            .map_err(map_ue!("invalid image"))?,
    ))
}

#[derive(Copy, Debug, Clone, Eq, PartialEq)]
pub enum HashDest {
    Images,
    ImageCache,
}

impl HashDest {
    pub fn table_name(self) -> &'static str {
        match self {
            HashDest::Images => "images",
            HashDest::ImageCache => "image_cache",
        }
    }
}

lazy_static! {
    static ref DB_POOL: r2d2::Pool<PostgresConnectionManager<NoTls>> = r2d2::Pool::new(
        PostgresConnectionManager::new(SECRETS.postgres.connect.parse().unwrap(), NoTls)
    )
    .unwrap();
}

pub enum GetKind {
    Cache(HashDest, i64),
    Request(HeaderMap),
}

fn get_existing(link: &str) -> Result<Option<(Hash, HashDest, i64)>, UserError> {
    let mut client = DB_POOL.get().map_err(map_ue!())?;
    let mut trans = client.transaction().map_err(map_ue!())?;

    trans
        .query(
            "SELECT hash, id, 'images' as table_name FROM images WHERE link = $1 UNION SELECT hash, id, 'image_cache' as table_name FROM image_cache WHERE link = $1",
            &[&link],
        )
        .map_err(map_ue!())
        .map(|rows| {
            rows.get(0)
                .map(|row| (Hash(row.get::<_, i64>("hash") as u64), match row.get("table_name") {
                    "images" => HashDest::Images,
                    "image_cache" => HashDest::ImageCache,
                    _ => unreachable!()
                }, row.get("id")))
        })
}

lazy_static! {
    static ref REQW_CLIENT: reqwest::Client = reqwest::Client::builder()
        .timeout(Some(std::time::Duration::from_secs(10)))
        .build()
        .unwrap();
}

fn error_for_status_ue(e: reqwest::Error) -> UserError {
    let msg = match e.status() {
        None => Cow::Borrowed("recieved error status from image host"),
        Some(sc) => Cow::Owned(format!("recieved error status from image host: {}", sc)),
    };

    UserError::new(msg, e)
}

pub fn is_host_imgur(host: &str) -> bool {
    lazy_static! {
        static ref IMGUR_HOST_RE: Regex = Regex::new(r"(?:^|\.)imgur.com$").unwrap();
    }

    IMGUR_HOST_RE.is_match(host)
}

pub fn is_host_gfycat(host: &str) -> bool {
    lazy_static! {
        static ref GFYCAT_HOST_RE: Regex = Regex::new(r"(?:^|\.)gfycat.com$").unwrap();
    }

    GFYCAT_HOST_RE.is_match(host)
}

pub fn is_host_special(host: &str) -> bool {
    is_host_imgur(host) || is_host_gfycat(host)
}

pub fn follow_link(link: &str) -> Result<Option<String>, UserError> {
    if EXT_RE.is_match(&link) {
        return Ok(None);
    }

    let url = Url::parse(link).map_err(map_ue!("not a valid URL", Source::User))?;

    let host = url
        .host_str()
        .ok_or_else(|| ue!("no host in URL", Source::User))?;

    // Begin special-casing

    if url.path() == "/" {
        return Ok(None);
    }

    if is_host_imgur(host) {
        follow_imgur(&url).map(Some)
    } else if is_host_gfycat(host) {
        follow_gfycat(&url).map(Some)
    } else {
        Ok(None)
    }
}

pub fn follow_gfycat(url: &Url) -> Result<String, UserError> {
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

    Ok(REQW_CLIENT
        .get(&format!(
            "https://api.gfycat.com/v1/gfycats/{}",
            GFY_ID_SEL
                .captures(url.path())
                .and_then(|c| c.get(1))
                .map(|m| m.as_str())
                .ok_or_else(|| ue!("couldn't find Gfycat ID in link", Source::User))?
        ))
        .send()
        .map_err(map_ue!("couldn't reach Gfycat API"))?
        .error_for_status()
        .map_err(error_for_status_ue)?
        .json::<Gfycats>()
        .map_err(map_ue!("invalid JSON from Gfycat API"))?
        .gfy_item
        .mobile_poster_url)
}

pub fn follow_imgur(url: &Url) -> Result<String, UserError> {
    lazy_static! {
        static ref IMGUR_SEL: Selector = Selector::parse("meta[property='og:image']").unwrap();
        static ref IMGUR_GIFV_RE: Regex = Regex::new(r"([^.]+)\.(?:gifv|webm)$").unwrap();
        static ref IMGUR_EMPTY_RE: Regex = Regex::new(r"^/\.[[:alnum:]]+\b").unwrap();
        static ref IMGUR_EXT_RE: Regex =
            Regex::new(r"[[:alnum:]]\.(?:jpg|png)[[:alnum:]]+").unwrap();
    }

    let path = url.path();
    let link = url.as_str();
    let path_start = url.path_segments().and_then(|mut ps| ps.next()).ok_or(ue!("base Imgur URL", Source::User))?;

    if IMGUR_GIFV_RE.is_match(path) {
        Ok(IMGUR_GIFV_RE
            .replace(path, "https://i.imgur.com/$1.gif")
            .to_string())
    } else if IMGUR_EXT_RE.is_match(path) || path_start == "download" {
        Ok(url.to_string())
    } else {
        let mut resp = REQW_CLIENT
            .get(link)
            .send()
            .and_then(|resp| {
                if resp.status() == StatusCode::NOT_FOUND && path_start == "gallery" {
                    REQW_CLIENT
                        .get(&link.replace("/gallery/", "/a/"))
                        .send()
                        .and_then(Response::error_for_status)
                } else {
                    resp.error_for_status()
                }
            })
            .map_err(error_for_status_ue)?;

        let mut doc_string = String::new();

        resp.read_to_string(&mut doc_string).map_err(map_ue!())?;

        let doc = Html::parse_document(&doc_string);
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
    }
}

pub fn get_hash(link: &str) -> Result<(Hash, Cow<str>, GetKind), UserError> {
    if link.len() > 2000 {
        return Err(ue!("URL too long", Source::User));
    }
    let url = Url::parse(link).map_err(map_ue!("not a valid URL", Source::User))?;

    let scheme = url.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(ue!("unsupported scheme in URL", Source::User));
    }

    let link = follow_link(link)?
        .map(Cow::Owned)
        .unwrap_or_else(|| Cow::Borrowed(link));

    if let Some((hash, hash_dest, id)) = get_existing(&link)? {
        return Ok((hash, link, GetKind::Cache(hash_dest, id)));
    }

    let mut resp = REQW_CLIENT
        .get(&utf8_percent_encode(&link, QUERY_ENCODE_SET).collect::<String>())
        .header(header::ACCEPT, IMAGE_MIMES.join(","))
        .header(
            header::USER_AGENT,
            "Mozilla/5.0 (X11; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0",
        )
        .send()
        .map_err(map_ue!("couldn't connect to image host"))?;

    resp.error_for_status_ref().map_err(error_for_status_ue)?;

    let url = resp.url();
    if url
        .host_str()
        .map(|host| host == "i.imgur.com")
        .unwrap_or(false)
        && url.path() == "/removed.png"
    {
        return Err(ue!("removed from Imgur"));
    }

    if let Some(ct) = resp.headers().get(header::CONTENT_TYPE) {
        let ct = ct
            .to_str()
            .map_err(map_ue!("non-ASCII Content-Type header"))?;
        if !IMAGE_MIMES.contains(&ct) {
            return Err(ue!(format!("unsupported Content-Type: {}", ct)));
        }
    }

    let mut image = Vec::<u8>::with_capacity(
        resp.headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|hv| hv.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(2048),
    );

    BufReader::new(resp.by_ref())
        .read_to_end(&mut image)
        .map_err(map_ue!())?;

    Ok((
        hash_from_memory(&image)?,
        link,
        GetKind::Request(resp.headers().to_owned()),
    ))
}

pub fn save_hash(
    link: &str,
    hash_dest: HashDest,
) -> Result<(Hash, HashDest, i64, bool), UserError> {
    let (hash, link, get_kind) = get_hash(link)?;

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
                .query("INSERT INTO images \
                        (link, hash, no_store, no_cache, expires, etag, must_revalidate, retrieved_on) VALUES (SELECT link, hash, no_store, no_cache, expires, etag, must_revalidate, retrieved_on FROM image_cache WHERE id = $1) RETURNING id", &[&id])
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
}

pub fn setup_logging() {
    fern::Dispatch::new()
        .format(|out, message, record| {
            let level = record.level();
            out.finish(format_args!(
                "{}[{}{}] {}",
                chrono::Local::now().format("[%Y-%m-%d %H:%M:%S]"),
                if level != LevelFilter::Info && level != LevelFilter::Warn {
                    match record.file() {
                        Some(file) => format!(
                            ":{}{}",
                            file,
                            match record.line() {
                                Some(line) => format!("#{}", line),
                                None => "".to_string(),
                            }
                        ),
                        None => "".to_string(),
                    }
                } else {
                    "".to_string()
                },
                record.level(),
                message
            ))
        })
        .level(LevelFilter::Warn)
        .level_for("site", LevelFilter::Info)
        .level_for("watcher", LevelFilter::Info)
        .level_for("hasher", LevelFilter::Info)
        .level_for("ingest", LevelFilter::Info)
        .level_for("common", LevelFilter::Info)
        .chain(std::io::stderr())
        .chain(
            fern::log_file(format!(
                "output_{}.log",
                chrono::Local::now().format("%Y-%m-%d_%H:%M:%S")
            ))
            .unwrap(),
        )
        .apply()
        .unwrap();
}

pub mod secrets {
    use failure::Error;
    use serde::Deserialize;
    use std::io::Read;

    #[derive(Debug, Deserialize)]
    pub struct Imgur {
        pub client_id: String,
        pub client_secret: String,
    }
    #[derive(Debug, Deserialize)]
    pub struct Postgres {
        pub connect: String,
    }
    #[derive(Debug, Deserialize)]
    pub struct Secrets {
        pub imgur: Imgur,
        pub postgres: Postgres,
    }

    pub fn load() -> Result<Secrets, Error> {
        let mut s = String::new();
        std::fs::File::open("../secrets/secrets.toml")
            .map_err(Error::from)?
            .read_to_string(&mut s)
            .map_err(Error::from)?;
        toml::from_str::<Secrets>(&s).map_err(Error::from)
    }
}

lazy_static! {
    pub static ref SECRETS: secrets::Secrets = secrets::load().unwrap();
}
