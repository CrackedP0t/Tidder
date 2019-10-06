#![feature(async_closure)]

use clap::clap_app;
use common::*;
use failure::Error;
use futures::prelude::*;

enum Op {
    Post(String),
    Hash(Vec<String>),
}

async fn post(id: String) -> Result<(), Error> {
    use reqwest::{header::USER_AGENT, Client};
    use serde_json::Value;

    let client = Client::new();

    let auth_resp = client
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
        .send().await?
        .error_for_status()?;

    let json = auth_resp.json::<Value>().await?;

    let access_token = json["access_token"]
        .as_str()
        .ok_or_else(|| format_err!("Access token not found"))?;

    let link = format!("https://oauth.reddit.com/by_id/t3_{}", id);

    let resp = client
        .get(&link)
        .query(&[("raw_json", "1")])
        .header(USER_AGENT, "Tidder 0.0.1")
        .bearer_auth(access_token)
        .send().await?
        .error_for_status()?;

    let post = &resp.json::<Value>().await?["data"]["children"][0]["data"];

    println!("{:#}", post);

    Ok(())
}

async fn hash(links: Vec<String>) -> Result<(), Error> {
    futures::stream::iter(links.into_iter())
        .fold(None, async move |last, arg| -> Option<Hash> {
                let res = get_hash(arg.clone()).await;

                let (hash, link, _get_kind) = match res {
                    Ok(res) => res,
                    Err(e) => {
                        println!("{} failed: {:?}", arg, e);
                        return last;
                    }
                };

                let mut out = format!("{}: {}", link, hash);
                if let Some(last) = last {
                    out = format!("{} ({})", out, distance(hash, last));
                }
                println!("{}", out);

                Some(hash)
        }).await;

    Ok(())
}

fn get_op() -> Result<Op, Error> {
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
    let op_matches = op_matches.ok_or_else(|| format_err!("No subcommand provided"))?;

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
            return Err(format_err!(
                "Unknown subcommand '{}'",
                unknown
            ))
        }
    };

    Ok(op)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let op = get_op()?;

    match op {
        Op::Post(id) => post(id).await,
        Op::Hash(links) => hash(links).await,
    }
}
