use bytes::Buf;
use common::*;
use fallible_iterator::FallibleIterator;
use http::{Response, StatusCode};
use hyper::{self, Body, HeaderMap};
use lazy_static::{lazy_static, LazyStatic};
use multipart::server::Multipart;
use postgres::NoTls;
use r2d2_postgres::{r2d2, PostgresConnectionManager};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Read;
use std::str::FromStr;
use std::vec::Vec;
use tera::Tera;
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

pub mod utils {
    use std::collections::HashMap;
    use tera::{to_value, try_get_value, Error, Result, Value};

    pub fn pluralize(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
        let num = try_get_value!("pluralize", "value", f64, value);

        let plural = match args.get("plural") {
            Some(val) => try_get_value!("pluralize", "plural", String, val),
            None => "s".to_string(),
        };

        let singular = match args.get("singular") {
            Some(val) => try_get_value!("pluralize", "singular", String, val),
            None => "".to_string(),
        };

        // English uses plural when it isn't one
        if (num.abs() - 1.).abs() > ::std::f64::EPSILON {
            Ok(to_value(&plural).unwrap())
        } else {
            Ok(to_value(&singular).unwrap())
        }
    }

    pub fn tern(cond: &Value, args: &HashMap<String, Value>) -> Result<Value> {
        let cond = cond.as_bool().ok_or(Error::msg("Expected bool"))?;
        let yes = args
            .get("yes")
            .ok_or(Error::msg("Argument 'yes' missing"))?
            .clone();
        let no = args
            .get("no")
            .ok_or(Error::msg("Argument 'no' missing"))?
            .clone();
        Ok(if cond { yes } else { no })
    }

    pub fn null(arg: Option<&Value>, _args: &[Value]) -> Result<bool> {
        arg.ok_or(Error::msg("Tester `null` was called on an undefined variable")).map(|v| v.is_null())
    }
}

#[derive(Clone, PartialEq, Serialize, Copy)]
#[serde(rename_all="lowercase")]
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

// impl ToStr for NSFWOption {

// }

impl Default for NSFWOption {
    fn default() -> Self {
        NSFWOption::Allow
    }
}

#[derive(Serialize)]
struct Match {
    author: Option<String>,
    created_utc: chrono::NaiveDateTime,
    distance: i64,
    link: String,
    permalink: String,
    score: i64,
    subreddit: String,
    title: String,
}

#[derive(Serialize)]
struct Findings {
    matches: Vec<Match>,
}

#[derive(Clone, Serialize)]
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

#[derive(Serialize)]
struct Search {
    form: Form,
    findings: Option<Findings>,
    error: Option<String>,
}

impl Default for Search {
    fn default() -> Search {
        Search {
            form: Form::default(),
            findings: None,
            error: None,
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
    static ref TERA: Tera = match Tera::new("site/templates/*") {
        Ok(mut t) => {
            t.register_filter("tern", utils::tern);
            t.register_filter("plural", utils::pluralize);
            t.register_tester("null", utils::null);
            t
        }
        Err(e) => {
            println!("Parsing error(s): {}", e);
            std::process::exit(1);
        }
    };
}

fn make_findings(hash: Hash, distance: i64, nsfw: NSFWOption) -> Result<Findings, Error> {
    let matches = POOL.get().map_err(Error::from).and_then(|mut conn| {
        conn.query_iter(
            format_args!(
                "SELECT hash <-> $1 as distance, posts.link, permalink, \
                 score, author, created_utc, subreddit, title \
                 FROM posts INNER JOIN images \
                 ON hash <@ ($1, $2) \
                 AND image_id = images.id \
                 {} \
                 ORDER BY distance ASC, created_utc ASC",
                match nsfw {
                    NSFWOption::Only => "AND nsfw = true",
                    NSFWOption::Allow => "",
                    NSFWOption::Never => "AND nsfw = false",
                }
            )
            .to_string()
            .as_str(),
            &[&hash, &distance],
        )
        .map_err(Error::from)
        .and_then(|rows| {
            rows.map(move |row| {
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
            .collect()
            .map_err(Error::from)
        })
    });

    matches.map(|matches| Findings { matches })
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

    let findings = if error.is_some() || !have_link {
        None
    } else {
        let distance = form.distance;
        let nsfw = form.nsfw;
        match get_hash(&form.link)
            .map_err(Error::from)
            .and_then(|(hash, _image_id, _exists)| make_findings(hash, distance, nsfw))
        {
            Ok(findings) => Some(findings),
            Err(e) => {
                error = Some(e);
                None
            }
        }
    };

    Ok(Search {
        form: form.clone(),
        error: error.map(|e| e.to_string()),
        findings,
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

    let findings = if error.is_some() {
        None
    } else {
        let distance = form.distance;
        let nsfw = form.nsfw;
        match hash.map(|hash| make_findings(hash, distance, nsfw))
        {
            None => None,
            Some(Ok(findings)) => Some(findings),
            Some(Err(e)) => {
                error = Some(e);
                None
            }
        }
    };

    Ok(Search {
        form: form.clone(),
        error: error.map(|e| e.to_string()),
        findings,
    })
}

fn get_response(qs: SearchQuery) -> Response<Body> {
    let out = get_search(qs)
        .map_err(|e| e.to_string())
        .and_then(|search| {
            TERA.render_value("search.html", &search).map_err(|e| {
                println!("{:?}", e);
                e.to_string()
            })
        })
        .map_err(|e| {
            format_args!("<h1>Error 500: Internal Server Error</h1><h2>{}</h2>", e).to_string()
        });

    Response::builder()
        .status(if out.is_err() { 500 } else { 200 })
        .header("Content-Type", "text/html")
        .body(Body::from(out.unwrap_or_else(|s| s)))
        .unwrap()
}

fn post_response(headers: HeaderMap, body: FullBody) -> Response<Body> {
    let error;
    let out = match post_search(headers, body) {
        Ok(search) => {
            error = false;
            TERA.render_value("search.html", &search).unwrap()
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
    TERA::initialize(&TERA);
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
