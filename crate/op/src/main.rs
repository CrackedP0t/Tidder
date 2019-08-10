use clap::clap_app;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

enum Op {
    Post(String),
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

fn post(id: String) -> Result<(), Box<Error>> {
    use reqwest::{header::USER_AGENT, Client};
    use serde_json::Value;

    let client = Client::new();

    let mut auth_resp = client
        .post("https://www.reddit.com/api/v1/access_token")
        .basic_auth("***REMOVED***", Some("***REMOVED***"))
        .query(&[("grant_type", "password"), ("username", "***REMOVED***"), ("password", "***REMOVED***")])
        .send()?
        .error_for_status()?;

    let json = auth_resp.json::<Value>()?;

    let access_token = json["access_token"].as_str().ok_or_else(|| StrError::new("Access token not found"))?;

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

fn get_op() -> Result<Op, Box<Error>> {
    let matches = clap_app!(op =>
        (@subcommand post =>
         (@arg ID: +required "Reddit's ID for the post")
        )
    )
    .get_matches();

    let (op_name, op_matches) = matches.subcommand();
    let op_matches = op_matches.ok_or_else(|| StrError::new("No subcommand provided"))?;

    let op = match op_name {
        "post" => Op::Post(op_matches.value_of("ID").unwrap().to_string()),
        unknown => {
            return Err(Box::new(StrError::new(format!(
                "Unknown subcommand '{}'",
                unknown
            ))))
        }
    };

    Ok(op)
}

fn main() -> Result<(), Box<Error>> {
    let op = get_op()?;

    match op {
        Op::Post(id) => post(id),
    }
}
