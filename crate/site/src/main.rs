#![type_length_limit = "5802293"]

use bytes::Buf;
use common::format;
use common::*;
use futures::prelude::*;
use http::StatusCode;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::vec::Vec;
use tera::{Context, Tera};
use url::Url;
use warp::filters::*;
use warp::multipart::FormData;
use warp::Filter;

#[derive(Deserialize)]
struct SearchQuery {
    imagelink: Option<String>,
    distance: Option<String>,
    nsfw: Option<String>,
    subreddits: Option<String>,
    authors: Option<String>,
}

#[allow(clippy::implicit_hasher)]
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
        if (num.abs() - 1.).abs() > std::f64::EPSILON {
            Ok(to_value(&plural).unwrap())
        } else {
            Ok(to_value(&singular).unwrap())
        }
    }

    pub fn tern(cond: &Value, args: &HashMap<String, Value>) -> Result<Value> {
        let cond = cond.as_bool().ok_or_else(|| Error::msg("Expected bool"))?;
        let yes = args
            .get("yes")
            .ok_or_else(|| Error::msg("Argument 'yes' missing"))?
            .clone();
        let no = args
            .get("no")
            .ok_or_else(|| Error::msg("Argument 'no' missing"))?
            .clone();
        Ok(if cond { yes } else { no })
    }

    pub fn null(arg: Option<&Value>, _args: &[Value]) -> Result<bool> {
        arg.ok_or_else(|| Error::msg("Tester `null` was called on an undefined variable"))
            .map(Value::is_null)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
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

impl Default for NSFWOption {
    fn default() -> Self {
        NSFWOption::Allow
    }
}

#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
struct Findings {
    matches: Vec<Match>,
}

#[derive(Clone, Debug, Serialize)]
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
            link: "".to_string(),
            distance: "1".to_string(),
            nsfw: "allow".to_string(),
            subreddits: "".to_string(),
            authors: "".to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
struct Search {
    form: Form,
    default_form: Form,
    findings: Option<Findings>,
    error: Option<UserError>,
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

#[derive(Debug)]
struct Params {
    distance: i64,
    nsfw: NSFWOption,
    subreddits: Vec<String>,
    authors: Vec<String>,
}

impl Params {
    pub fn from_form(form: &Form) -> Result<Params, UserError> {
        Ok(Params {
            distance: if form.distance.is_empty() {
                1
            } else {
                form.distance
                    .parse()
                    .map_err(map_ue!("invalid distance parameter", Source::User))?
            },
            nsfw: form
                .nsfw
                .parse()
                .map_err(map_ue!("invalid nsfw parameter", Source::User))?,
            subreddits: form
                .subreddits
                .split_whitespace()
                .map(str::to_lowercase)
                .collect(),
            authors: form
                .authors
                .split_whitespace()
                .map(str::to_lowercase)
                .collect(),
        })
    }
}

fn create_tera() -> Tera {
    match Tera::new(concat!(env!("CARGO_MANIFEST_DIR"), "/templates/*")) {
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
    }
}

static TERA: Lazy<Tera> = Lazy::new(create_tera);

async fn make_findings(hash: Hash, params: Params) -> Result<Findings, UserError> {
    macro_rules! tosql {
        ($v:expr) => {
            (&$v as &(dyn tokio_postgres::types::ToSql + Sync))
        };
    }

    let client = PG_POOL.take().await?;

    let (s_query, a_query, args) = if params.subreddits.is_empty() && params.authors.is_empty() {
        ("", "", vec![tosql!(hash), tosql!(params.distance)])
    } else if params.authors.is_empty() {
        (
            "AND LOWER(subreddit) = ANY($3)",
            "",
            vec![
                tosql!(hash),
                tosql!(params.distance),
                tosql!(params.subreddits),
            ],
        )
    } else if params.subreddits.is_empty() {
        (
            "",
            "AND LOWER(author) = ANY($3)",
            vec![
                tosql!(hash),
                tosql!(params.distance),
                tosql!(params.authors),
            ],
        )
    } else {
        (
            "AND LOWER(subreddit) = ANY($3)",
            "AND LOWER(author) = ANY($4)",
            vec![
                tosql!(hash),
                tosql!(params.distance),
                tosql!(params.subreddits),
                tosql!(params.authors),
            ],
        )
    };

    let rows = client
        .query(
            format!(
                "SELECT hash <-> $1 as distance, images.link, permalink, \
                 score, author, created_utc, subreddit, title \
                 FROM posts INNER JOIN images \
                 ON hash <@ ($1, $2) \
                 AND image_id = images.id \
                 {} \
                 {} \
                 {} \
                 ORDER BY distance ASC, created_utc ASC",
                match params.nsfw {
                    NSFWOption::Only => "AND nsfw = true",
                    NSFWOption::Allow => "",
                    NSFWOption::Never => "AND nsfw = false",
                },
                s_query,
                a_query
            )
            .as_str(),
            &args,
        )
        .await?;

    Ok(Findings {
        matches: rows
            .iter()
            .map(move |row| Match {
                permalink: format!("https://reddit.com{}", row.get::<_, &str>("permalink")),
                distance: row.get("distance"),
                score: row.get("score"),
                author: row.get("author"),
                link: row.get("link"),
                created_utc: row.get("created_utc"),
                subreddit: row.get("subreddit"),
                title: row.get("title"),
            })
            .collect(),
    })
}

async fn get_search(qs: SearchQuery) -> Search {
    let imagelink = qs.imagelink.clone();

    let default_form = Form::default();
    let form = Form {
        distance: qs.distance.unwrap_or(default_form.distance),
        nsfw: qs.nsfw.unwrap_or(default_form.nsfw),
        subreddits: qs.subreddits.unwrap_or(default_form.subreddits),
        authors: qs.authors.unwrap_or(default_form.authors),
        link: qs.imagelink.unwrap_or(default_form.link),
    };

    let err_form = form.clone();

    let findings =
        match imagelink {
            None => Ok(None),
            Some(link) => {
                if &link != "" {
                    match Url::parse(&link).map_err(map_ue!("invalid URL")) {
                        Ok(_url) => match Params::from_form(&form) {
                            Ok(params) => {
                                save_hash(&link, HashDest::ImageCache)
                                    .and_then(|hash_saved| {
                                        async move {
                                            make_findings(hash_saved.hash, params).await.map(Some)
                                        }
                                    })
                                    .await
                            }
                            Err(e) => Err(e),
                        },
                        Err(e) => Err(e),
                    }
                } else {
                    Ok(None)
                }
            }
        };

    match findings {
        Ok(findings) => Search {
            form,
            error: None,
            findings,
            upload: false,
            ..Default::default()
        },
        Err(error) => Search {
            form: err_form,
            error: Some(error),
            findings: None,
            upload: false,
            ..Default::default()
        },
    }
}

async fn post_search(mut form: FormData) -> Search {
    #[allow(clippy::ptr_arg)]
    fn utf8_to_string(utf8: &Vec<u8>) -> String {
        String::from_utf8_lossy(utf8.as_slice()).to_string()
    }

    let do_findings = move || {
        async move {
            let mut map: HashMap<String, Vec<u8>> = HashMap::new();

            while let Some(mut part) = form.try_next().await? {
                let name = part.name().to_string();
                let mut data = Vec::<u8>::new();

                while let Some(b) = part.data().await {
                    let b = b?;
                    data.extend(b.bytes());
                }

                map.insert(name, data);
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

            Ok(match hash {
                None => (form, None),
                Some(hash) => (form, Some(make_findings(hash, params).await?)),
            })
        }
    };

    let output = do_findings().await;

    let (form, findings, error) = match output {
        Ok((form, findings)) => (form, findings, None),
        Err(error) => (Form::default(), None, Some(error)),
    };

    Search {
        form,
        error,
        findings,
        upload: true,
        ..Default::default()
    }
}

async fn get_response(query: SearchQuery) -> impl warp::Reply {
    let search = get_search(query).await;

    #[cfg(debug_assertions)]
    let tera = create_tera();

    #[cfg(not(debug_assertions))]
    let tera = TERA.force();

    let out =
        Context::from_serialize(&search).and_then(|context| tera.render("search.html", &context));

    let (page, status) = match out {
        Ok(page) => (
            page,
            search
                .error
                .map(|ue| {
                    warn!("{}", ue.error);
                    ue.status_code()
                })
                .unwrap_or(StatusCode::OK),
        ),
        Err(_) => (
            "<h1>Error 500: Internal Server Error</h1>".to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    };

    warp::reply::with_status(warp::reply::html(page), status)
}

async fn post_response(form: FormData) -> impl warp::Reply {
    let search = post_search(form).await;

    #[cfg(debug_assertions)]
    let tera = create_tera();

    #[cfg(not(debug_assertions))]
    let tera = TERA.force();

    let out =
        Context::from_serialize(&search).and_then(|context| tera.render("search.html", &context));

    let (page, status) = match out {
        Ok(page) => (
            page,
            search
                .error
                .map(|ue| {
                    warn!("{}", ue.error);
                    ue.status_code()
                })
                .unwrap_or(StatusCode::OK),
        ),
        Err(_) => (
            "<h1>Error 500: Internal Server Error</h1>".to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    };

    warp::reply::with_status(warp::reply::html(page), status)
}

#[tokio::main]
async fn main() {
    setup_logging!();
    Lazy::force(&TERA);

    let router =
        warp::path::end().and(
            method::get()
                .and(query::query::<SearchQuery>().and_then(|query| {
                    async { Ok::<_, warp::Rejection>(get_response(query).await) }
                }))
                .or(method::post().and(multipart::form()).and_then(|form| {
                    async move { Ok::<_, warp::Rejection>(post_response(form).await) }
                })),
        );

    let ip = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1".to_string());

    println!("Serving on http://{}:7878", ip);

    warp::serve(router)
        .run((ip.parse::<std::net::IpAddr>().unwrap(), 7878))
        .await;
}
