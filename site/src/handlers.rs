use askama::Template;
use hyper::{Body, Response};
use serde::Deserialize;
use url::Url;

pub mod search {
    use super::*;

    #[derive(Deserialize, StateData, StaticResponseExtender)]
    pub struct QSE {
        imageurl: Option<String>,
    }

    struct Findings {}

    struct Sent {
        url: String,
        findings: Result<Findings, String>,
    }

    #[derive(Template)]
    #[template(path = "search.html")]
    struct Search {
        sent: Option<Sent>,
    }

    pub fn get(mut state: State) -> (State, impl IntoResponse) {
        let query = QSE::take_from(&mut state);

        let tpl = match query.imageurl {
            Some(url) => {
                let valid = Url::parse(&url);
                Search {
                    sent: Some(Sent {
                        url,
                        findings: match valid {
                            Ok(_) => Ok(Findings {}),
                            Err(e) => Err(format!("{}", e)),
                        },
                    }),
                }
            }
            None => Search { sent: None },
        };

        (state, tpl)
    }
}

pub mod error {
    use super::*;

    pub fn extend(_state: &mut State, resp: &mut Response<Body>) {
        let err_string = format!(
            "<h1>Error {}: {}</h1>",
            resp.status().as_str(),
            resp.status().canonical_reason().unwrap_or("Unknown error")
        );
        let err_body = Body::from(err_string);
        let body_ref = resp.body_mut();

        *body_ref = err_body;
    }
}
