use askama::Template;
use bytes::Buf;
use common::*;
use fallible_iterator::FallibleIterator;
use http::{Response, StatusCode};
use hyper::{self, Body, HeaderMap};
use lazy_static::lazy_static;
use multipart::server::Multipart;
use postgres::NoTls;
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Read;
use std::str::FromStr;
use std::vec::Vec;
use url::Url;
use warp::path::{full, FullPath};
use warp::{
    body::{self, FullBody},
    get2,
    header::headers_cloned,
    path, post2, query, reply, Filter, Reply,
};

#[derive(Deserialize)]
struct SearchQuery {
    imagelink: Option<String>,
    distance: Option<String>,
    nsfw: Option<String>,
}

pub mod filters {
    pub fn plural_es<N>(n: &N) -> Result<&'static str, askama::Error>
    where
        N: From<u8> + PartialEq<N>,
    {
        Ok(if *n == 1u8.into() { "" } else { "es" })
    }

    pub fn plural_s<N>(n: &N) -> Result<&'static str, askama::Error>
    where
        N: From<u8> + PartialEq<N>,
    {
        Ok(if *n == 1u8.into() { "" } else { "s" })
    }

    pub fn tern<D: std::fmt::Display>(cond: &bool, yes: D, no: D) -> Result<D, askama::Error> {
        Ok(if *cond { yes } else { no })
    }
}

#[derive(Clone, PartialEq)]
enum NSFWOption {
    Only,
    Allow,
    Never,
}

impl FromStr for NSFWOption {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use NSFWOption::*;
        match s {
            "only" => Ok(Only),
            "" | "allow" => Ok(Allow),
            "never" => Ok(Never),
            _ => Err(format_err!("Invalid NSFW option: {}", s)),
        }
    }
}

impl Default for NSFWOption {
    fn default() -> Self {
        NSFWOption::Allow
    }
}

struct Match {
    author: String,
    created_utc: chrono::NaiveDateTime,
    distance: i64,
    link: String,
    permalink: String,
    score: i64,
    subreddit: String,
    title: String,
}

#[derive(Template)]
#[template(path = "findings.html")]
struct Findings {
    matches: Vec<Match>,
}

#[derive(Clone)]
struct Form {
    link: String,
    distance: i64,
    nsfw: NSFWOption,
    upload: bool,
}

impl Default for Form {
    fn default() -> Self {
        Form {
            link: "".to_string(),
            distance: 1,
            nsfw: NSFWOption::Allow,
            upload: false,
        }
    }
}

#[derive(Template)]
#[template(path = "search.html")]
struct Search {
    form: Form,
    findings: Result<Option<Result<Findings, Error>>, Error>,
}

impl Default for Search {
    fn default() -> Search {
        Search {
            form: Form::default(),
            findings: Ok(None),
        }
    }
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

fn make_findings(hash: Hash, distance: i64, nsfw: NSFWOption) -> Result<Findings, Error> {
    let mut conn = POOL.get().map_err(Error::from)?;

    let rows = conn.query_iter(
        format_args!("SELECT hash <-> $1 as distance, posts.link, permalink, score, author, created_utc, subreddit, title FROM posts INNER JOIN images ON hash <@ ($1, $2) AND image_id = images.id {}ORDER BY distance ASC, created_utc ASC", match nsfw {
            NSFWOption::Only => " AND nsfw = true ",
            NSFWOption::Allow => "",
            NSFWOption::Never => " AND nsfw = false "
        }).to_string().as_str(),
        &[&hash, &distance],
    ).map_err(Error::from)?;

    Ok(Findings {
        matches: rows
            .map(move |row| {
                Ok(Match {
                    permalink: format!("https://reddit.com{}", row.get::<_, &str>("permalink")),
                    distance: row.get("distance"),
                    score: row.get("score"),
                    author: row.get("author"),
                    link: row.get("link"),
                    created_utc: row.get("created_utc"),
                    subreddit: row.get("subreddit"),
                    title: row.get("title"),
                })
            })
            .collect()?,
    })
}

fn get_search(qs: SearchQuery) -> Result<Search, Error> {
    let have_link = qs.imagelink.as_ref().map(|s| s != "").unwrap_or(false);

    let mut form = Form::default();
    let mut error = None;

    if let Some(s) = qs.imagelink {
        if &s != "" {
            error = Url::parse(&s).map_err(Error::from).err();
            form.link = s;
        }
    }
    if let Some(s) = qs.distance {
        if &s != "" {
            match s.parse::<i64>() {
                Ok(d) => form.distance = d,
                Err(e) => error = Some(Error::from(e)),
            }
        }
    }
    if let Some(s) = qs.nsfw {
        match NSFWOption::from_str(&s) {
            Ok(n) => form.nsfw = n,
            Err(e) => error = Some(e),
        }
    }

    Ok(Search {
        form: form.clone(),
        findings: match error {
            None => Ok(if have_link {
                let (hash, _image_id, _exists) = get_hash(form.link).map_err(Error::from)?;
                Some(make_findings(hash, form.distance, form.nsfw))
            } else {
                None
            }),
            Some(e) => Err(e),
        },
    })
}

fn post_search(headers: HeaderMap, body: FullBody) -> Result<Search, Error> {
    lazy_static! {
        static ref BOUNDARY_RE: Regex = Regex::new(r"boundary=(.+)").unwrap();
    }

    let mut mp = Multipart::with_body(
        body.reader(),
        BOUNDARY_RE
            .captures(
                headers
                    .get("Content-Type")
                    .ok_or(format_err!("No Content-Type header supplied"))?
                    .to_str()
                    .map_err(Error::from)?,
            )
            .and_then(|captures| captures.get(1))
            .map(|capture| capture.as_str())
            .ok_or(format_err!("No boundary in Content-Type"))?,
    );

    let mut map: HashMap<String, Vec<u8>> = HashMap::new();

    while let Ok(Some(mut field)) = mp.read_entry() {
        let mut data = Vec::new();
        field.data.read_to_end(&mut data).map_err(Error::from)?;
        map.insert(field.headers.name.to_string(), data);
    }

    let mut form = Form::default();
    let mut error = None;

    form.upload = map.contains_key("imagefile");
    let hash = if let Some(b) = map.get("imagefile") {
        match hash_from_memory(b) {
            Ok(hash) => Some(hash),
            Err(e) => {
                error = Some(e.into());
                None
            }
        }
    } else {
        None
    };
    if let Some(b) = map.get("distance") {
        let s = std::str::from_utf8(&b).map_err(Error::from)?;
        if s != "" {
            match s.parse::<i64>() {
                Ok(d) => form.distance = d,
                Err(e) => error = Some(Error::from(e)),
            }
        }
    }
    if let Some(b) = map.get("nsfw") {
        let s = std::str::from_utf8(&b).map_err(Error::from)?;
        match NSFWOption::from_str(s) {
            Ok(n) => form.nsfw = n,
            Err(e) => error = Some(e),
        }
    }

    Ok(Search {
        form: form.clone(),
        findings: match error {
            None => Ok(hash.map(|hash| make_findings(hash, form.distance, form.nsfw))),
            Some(e) => Err(e),
        },
    })
}

fn get_response(qs: SearchQuery) -> Response<Body> {
    let error;
    let out = match get_search(qs) {
        Ok(search) => {
            error = false;
            search.render().unwrap()
        }
        Err(e) => {
            error = true;
            format_args!("<h1>Error 500: Internal Server Error</h1><h2>{}</h2>", e).to_string()
        }
    };

    Response::builder()
        .status(if error { 500 } else { 200 })
        .header("Content-Type", "text/html")
        .body(Body::from(out))
        .unwrap()
}

fn post_response(headers: HeaderMap, body: FullBody) -> Response<Body> {
    let error;
    let out = match post_search(headers, body) {
        Ok(search) => {
            error = false;
            search.render().unwrap()
        }
        Err(e) => {
            error = true;
            format_args!("<h1>Error 500: Internal Server Error</h1><h2>{}</h2>", e).to_string()
        }
    };

    Response::builder()
        .status(if error { 500 } else { 200 })
        .header("Content-Type", "text/html")
        .body(Body::from(out))
        .unwrap()
}

fn run_server() {
    setup_logging();
    let router = path("search")
        .and(
            get2()
                .and(query::<SearchQuery>().map(get_response))
                .or(post2()
                    .and(headers_cloned())
                    .and(body::concat())
                    .map(post_response)),
        )
        .or(full().map(reply_not_found));

    warp::serve(router).run(([127, 0, 0, 1], 7878));
}

pub fn main() {
    run_server();
}
