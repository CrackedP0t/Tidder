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
    subreddits: Option<String>,
    authors: Option<String>,
}

pub mod utils {
    use std::collections::HashMap;
    use tera::{to_value, try_get_value, Error, Result, Value};

    pub fn pluralize(value: &Value, args: &HashMap<String, Value>) -> Result<Value> {
        let num = try_get_value!("pluralize", "value", f64, value);

        let plural = match args.get("plural") {
            Some(val) => try_get_value!("pluralize", "plural", String, val),
            None => String::from("s"),
        };

        let singular = match args.get("singular") {
            Some(val) => try_get_value!("pluralize", "singular", String, val),
            None => String::from(""),
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
        arg.ok_or(Error::msg(
            "Tester `null` was called on an undefined variable",
        ))
        .map(|v| v.is_null())
    }
}

#[derive(Clone, PartialEq, Serialize, Copy)]
#[serde(rename_all = "lowercase")]
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
    distance: String,
    nsfw: String,
    subreddits: String,
    authors: String,
}

impl Default for Form {
    fn default() -> Form {
        Form {
            link: string!(""),
            distance: string!("1"),
            nsfw: string!("allow"),
            subreddits: string!(""),
            authors: string!(""),
        }
    }
}

#[derive(Serialize)]
struct Search {
    form: Form,
    default_form: Form,
    findings: Option<Findings>,
    error: Option<String>,
    upload: bool,
}

impl Default for Search {
    fn default() -> Search {
        Search {
            form: Form::default(),
            default_form: Form::default(),
            findings: None,
            error: None,
            upload: false,
        }
    }
}

struct Params {
    distance: i64,
    nsfw: NSFWOption,
    subreddits: Vec<String>,
    authors: Vec<String>,
}

impl Params {
    pub fn from_form(form: &Form) -> Result<Params, Error> {
        Ok(Params {
            distance: if form.distance.is_empty() {
                1
            } else {
                form.distance.parse().map_err(Error::from)?
            },
            nsfw: form.nsfw.parse().map_err(Error::from)?,
            subreddits: form.subreddits.split_whitespace().map(String::from).collect(),
            authors: form.authors.split_whitespace().map(String::from).collect(),
        })
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

fn make_findings(hash: Hash, params: Params) -> Result<Findings, Error> {
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
                match params.nsfw {
                    NSFWOption::Only => "AND nsfw = true",
                    NSFWOption::Allow => "",
                    NSFWOption::Never => "AND nsfw = false",
                }
            )
            .to_string()
            .as_str(),
            &[&hash, &params.distance],
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
    let imagelink = qs.imagelink.clone();

    let default_form = Form::default();
    let form = Form {
        distance: qs.distance.unwrap_or(default_form.distance),
        nsfw: qs.nsfw.unwrap_or(default_form.nsfw),
        subreddits: qs.subreddits.unwrap_or(default_form.subreddits),
        authors: qs.authors.unwrap_or(default_form.authors),
        link: qs.imagelink.unwrap_or(default_form.link),
    };

    let findings = imagelink
        .and_then(|link| {
            if &link != "" {
                Some(Url::parse(&link).map_err(Error::from).and_then(|_| {
                    let (hash, _image_id, _exists) = get_hash(&link)?;
                    make_findings(hash, Params::from_form(&form)?)
                }))
            } else {
                None
            }
        })
        .unwrap_or_else(|| {
            Ok(Findings {
                matches: Vec::new(),
            })
        });

    let (findings, error) = match findings {
        Ok(findings) => (Some(findings), None),
        Err(error) => (None, Some(error)),
    };

    Ok(Search {
        form: form.clone(),
        error: error.map(|e| e.to_string()),
        findings,
        upload: false,
        ..Default::default()
    })
}

fn post_search(headers: HeaderMap, body: FullBody) -> Result<Search, Error> {
    fn utf8_to_string(utf8: &Vec<u8>) -> String {
        String::from_utf8_lossy(utf8.as_slice()).to_string()
    }

    lazy_static! {
        static ref BOUNDARY_RE: Regex = Regex::new(r"boundary=(.+)").unwrap();
    }

    let output = headers
        .get("Content-Type")
        .ok_or(format_err!("No Content-Type header supplied"))
        .and_then(|header_value| {
            let boundary = BOUNDARY_RE
                .captures(header_value.to_str().map_err(Error::from)?)
                .and_then(|captures| captures.get(1))
                .map(|capture| capture.as_str())
                .ok_or(format_err!("No boundary in Content-Type"))?;

            let mut mp = Multipart::with_body(body.reader(), boundary);
            let mut map: HashMap<String, Vec<u8>> = HashMap::new();

            while let Ok(Some(mut field)) = mp.read_entry() {
                let mut data = Vec::new();
                field.data.read_to_end(&mut data).map_err(Error::from)?;
                map.insert(field.headers.name.to_string(), data);
            }

            let default_form = Form::default();
            let form = Form {
                distance: map
                    .get("distance")
                    .map(utf8_to_string)
                    .unwrap_or(default_form.distance),
                nsfw: map
                    .get("nsfw")
                    .map(utf8_to_string)
                    .unwrap_or(default_form.nsfw),
                subreddits: map
                    .get("subreddits")
                    .map(utf8_to_string)
                    .unwrap_or(default_form.subreddits),
                authors: map
                    .get("authors")
                    .map(utf8_to_string)
                    .unwrap_or(default_form.authors),
                ..Default::default()
            };

            let hash = map
                .get("imagefile")
                .map(|bytes| hash_from_memory(bytes))
                .transpose()?;

            let params = Params::from_form(&form)?;

            hash.map(|hash| make_findings(hash, params))
                .unwrap_or_else(|| {
                    Ok(Findings {
                        matches: Vec::new(),
                    })
                })
                .map(|findings| (form, findings))
        });

    let (form, findings, error) = match output {
        Ok((form, findings)) => (form, Some(findings), None),
        Err(error) => (Form::default(), None, Some(error)),
    };

    Ok(Search {
        form: form,
        error: error.map(|e| e.to_string()),
        findings,
        upload: true,
        ..Default::default()
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
