use bytes::Buf;
use common::format;
use common::*;
use futures::prelude::*;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error as _;
use std::time::Instant;
use std::vec::Vec;
use tera::Context;
use tokio_postgres::error::{DbError, SqlState};
use url::Url;
use warp::multipart::FormData;

#[derive(Deserialize)]
pub struct SearchQuery {
    imagelink: Option<String>,
    distance: Option<String>,
    nsfw: Option<String>,
    subreddits: Option<String>,
    authors: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum NSFWOption {
    Only,
    Allow,
    Never,
}

impl std::str::FromStr for NSFWOption {
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
    preview: String,
    permalink: String,
    score: i64,
    subreddit: String,
    title: String,
}

#[derive(Debug, Serialize)]
struct Findings {
    took: String,
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
    max_distance: u8,
}

impl Default for Search {
    fn default() -> Search {
        Search {
            form: Form::default(),
            default_form: Form::default(),
            findings: None,
            error: None,
            upload: false,
            max_distance: CONFIG.max_distance,
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
            distance: {
                let distance = if form.distance.is_empty() {
                    1
                } else {
                    form.distance
                        .parse()
                        .map_err(map_ue!("invalid distance parameter", Source::User))?
                };

                if distance > CONFIG.max_distance {
                    return Err(ue!("distance too large", Source::User));
                }

                distance as i64
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

async fn make_findings(hash: Hash, params: Params) -> Result<Findings, UserError> {
    macro_rules! tosql {
        ($v:expr) => {
            (&$v as &(dyn tokio_postgres::types::ToSql + Sync))
        };
    }

    let client = PG_POOL.get().await?;

    let (s_query, a_query, args) = if params.subreddits.is_empty() && params.authors.is_empty() {
        (
            "",
            "",
            vec![
                tosql!(hash),
                tosql!(params.distance),
                tosql!(CONFIG.max_results),
            ],
        )
    } else if params.authors.is_empty() {
        (
            "AND LOWER(subreddit) = ANY($4)",
            "",
            vec![
                tosql!(hash),
                tosql!(params.distance),
                tosql!(CONFIG.max_results),
                tosql!(params.subreddits),
            ],
        )
    } else if params.subreddits.is_empty() {
        (
            "",
            "AND LOWER(author) = ANY($4)",
            vec![
                tosql!(hash),
                tosql!(params.distance),
                tosql!(CONFIG.max_results),
                tosql!(params.authors),
            ],
        )
    } else {
        (
            "AND LOWER(subreddit) = ANY($4)",
            "AND LOWER(author) = ANY($5)",
            vec![
                tosql!(hash),
                tosql!(params.distance),
                tosql!(CONFIG.max_results),
                tosql!(params.subreddits),
                tosql!(params.authors),
            ],
        )
    };

    let search_start = Instant::now();

    let rows = client
        .query(
            format!(
                "SELECT hash <-> $1 as distance, preview, images.link as link, permalink, \
                 score, author, created_utc, subreddit, title \
                 FROM posts INNER JOIN images \
                 ON hash <@ ($1, $2) \
                 AND image_id = images.id \
                 {} \
                 {} \
                 {} \
                 ORDER BY distance ASC, created_utc ASC LIMIT $3",
                match params.nsfw {
                    NSFWOption::Only => "AND nsfw = true",
                    NSFWOption::Allow => "",
                    NSFWOption::Never => "AND nsfw = false",
                },
                s_query,
                a_query,
            )
            .as_str(),
            &args,
        )
        .await
        .map_err(|e| {
            if let Some(dberror) = e.source().and_then(|e| e.downcast_ref::<DbError>()) {
                if *dberror.code() == SqlState::QUERY_CANCELED
                    && dberror.message() == "canceling statement due to statement timeout"
                {
                    ue!("query took too long", Source::User)
                } else {
                    e.into()
                }
            } else {
                e.into()
            }
        })?;

    let search_took = search_start.elapsed();

    Ok(Findings {
        took: format!(
            "{}.{:03}",
            search_took.as_secs(),
            search_took.subsec_millis()
        ),
        matches: rows
            .iter()
            .map(move |row| {
                let link: String = row.get("link");
                let preview = row
                    .get::<_, Option<String>>("preview")
                    .map(|p| Submission::unescape(&p))
                    .unwrap_or_else(|| link.clone());

                Match {
                    permalink: format!("https://reddit.com{}", row.get::<_, &str>("permalink")),
                    distance: row.get("distance"),
                    score: row.get("score"),
                    author: row.get("author"),
                    link,
                    preview,
                    created_utc: row.get("created_utc"),
                    subreddit: row.get("subreddit"),
                    title: row.get("title"),
                }
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

    let findings = match imagelink {
        None => Ok(None),
        Some(link) => {
            if &link != "" {
                match Url::parse(&link).map_err(map_ue!("invalid URL")) {
                    Ok(_url) => match Params::from_form(&form) {
                        Ok(params) => {
                            save_hash(&link, HashDest::ImageCache)
                                .and_then(|hash_saved| async move {
                                    make_findings(hash_saved.hash, params).await.map(Some)
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

    let do_findings = move || async move {
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

pub async fn get_response(query: SearchQuery) -> impl warp::Reply {
    let search = get_search(query).await;

    let tera = super::get_tera!();

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
        Err(e) => {
            error!("{}", e);
            (
                "<h1>Error 500: Internal Server Error</h1>".to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    };

    warp::reply::with_status(warp::reply::html(page), status)
}

pub async fn post_response(form: FormData) -> impl warp::Reply {
    let search = post_search(form).await;

    let tera = super::get_tera!();

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
