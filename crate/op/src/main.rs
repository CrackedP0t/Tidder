use clap::clap_app;
use common::*;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

enum Op {
    Post(String),
    Hash(Vec<String>),
}

#[derive(Debug)]
struct StrError(String);

impl StrError {
    fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl Display for StrError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Error for StrError {}

fn post(id: String) -> Result<(), Box<dyn Error>> {
    use reqwest::{header::USER_AGENT, Client};
    use serde_json::Value;

    let client = Client::new();

    let mut auth_resp = client
        .post("https://www.reddit.com/api/v1/access_token")
        .basic_auth(
            &SECRETS.reddit.client_id,
            Some(&SECRETS.reddit.client_secret),
        )
        .query(&[
            ("grant_type", "password"),
            ("username", &SECRETS.reddit.username),
            ("password", &SECRETS.reddit.password),
        ])
        .send()?
        .error_for_status()?;

    let json = auth_resp.json::<Value>()?;

    let access_token = json["access_token"]
        .as_str()
        .ok_or_else(|| StrError::new("Access token not found"))?;

    let link = format!("https://oauth.reddit.com/by_id/t3_{}", id);

    let mut resp = client
        .get(&link)
        .query(&[("raw_json", "1")])
        .header(USER_AGENT, "Tidder 0.0.1")
        .bearer_auth(access_token)
        .send()?;

    resp.error_for_status_ref()?;

    let post = &resp.json::<Value>()?["data"]["children"][0]["data"];

    println!("{:#}", post);

    Ok(())
}

fn hash(links: Vec<String>) -> Result<(), Box<dyn Error>> {
    use futures::{
        future::{ok, Future},
        stream::{iter_ok, Stream},
    };

    tokio::run(
        iter_ok::<_, ()>(links.into_iter())
            .fold(None, |last, arg| {
                get_hash(arg.clone()).then(move |res| {
                    let (hash, link, _get_kind) = match res {
                        Ok(res) => res,
                        Err(e) => {
                            println!("{} failed: {}", arg, e);
                            return ok(last);
                        }
                    };
                    let mut out = format!("{}: {}", link, hash);
                    if let Some(last) = last {
                        out = format!("{} ({})", out, distance(hash, last));
                    }
                    println!("{}", out);

                    ok(Some(hash))
                })
            })
            .map(|_| ()),
    );
    Ok(())
}

fn get_op() -> Result<Op, Box<dyn Error>> {
    let matches = clap_app!(op =>
        (@subcommand post =>
            (@arg ID: +required "Reddit's ID for the post")
        )
        (@subcommand hash =>
            (@arg LINKS: +required ... "The links you wish to hash")
        )
    )
    .get_matches();

    let (op_name, op_matches) = matches.subcommand();
    let op_matches = op_matches.ok_or_else(|| StrError::new("No subcommand provided"))?;

    let op = match op_name {
        "post" => Op::Post(op_matches.value_of("ID").unwrap().to_string()),
        "hash" => Op::Hash(
            op_matches
                .values_of("LINKS")
                .unwrap()
                .map(|l| l.to_owned())
                .collect(),
        ),
        unknown => {
            return Err(Box::new(StrError::new(format!(
                "Unknown subcommand '{}'",
                unknown
            ))))
        }
    };

    Ok(op)
}

fn main() -> Result<(), Box<dyn Error>> {
    let op = get_op()?;

    match op {
        Op::Post(id) => post(id),
        Op::Hash(links) => hash(links),
    }
}
