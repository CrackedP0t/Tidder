#![feature(async_closure)]

use clap::clap_app;
use common::*;
use futures::prelude::*;
use reqwest::{header::USER_AGENT, Client};
use serde_json::Value;
use serde::de::Deserialize;

async fn post(id: &str) -> Result<(), UserError> {
    let client = Client::new();

    let auth_resp = client
        .post("https://www.reddit.com/api/v1/access_token")
        .basic_auth(
            &SECRETS.reddit.client_id,
            Some(&SECRETS.reddit.client_secret),
        )
        .form(&[
            ("grant_type", "password"),
            ("username", &SECRETS.reddit.username),
            ("password", &SECRETS.reddit.password),
        ])
        .send()
        .await?;

    let status = auth_resp.status();
    let json = auth_resp.json::<Value>().await?;

    if status.is_success() {
        let access_token = json["access_token"]
            .as_str()
            .ok_or_else(|| ue!("Access token not found"))?;

        let link = format!("https://oauth.reddit.com/by_id/t3_{}", id);

        let resp = client
            .get(&link)
            .query(&[("raw_json", "1")])
            .header(USER_AGENT, "Tidder 0.0.1")
            .bearer_auth(access_token)
            .send()
            .await?
            .error_for_status()?;

        let post = &resp.json::<Value>().await?["data"]["children"][0]["data"];

        println!("{:#}", post);

        Ok(())
    } else {
        println!("{:#}", json);

        Err(ue!(format!("Authentication HTTP error: {}", status)))
    }
}

async fn save(id: &str) -> Result<(), UserError> {
    let client = Client::new();

    let auth_resp = client
        .post("https://www.reddit.com/api/v1/access_token")
        .basic_auth(
            &SECRETS.reddit.client_id,
            Some(&SECRETS.reddit.client_secret),
        )
        .form(&[
            ("grant_type", "password"),
            ("username", &SECRETS.reddit.username),
            ("password", &SECRETS.reddit.password),
        ])
        .send()
        .await?;

    let status = auth_resp.status();
    let json = auth_resp.json::<Value>().await?;

    if status.is_success() {
        let access_token = json["access_token"]
            .as_str()
            .ok_or_else(|| ue!("Access token not found"))?;

        let link = format!("https://oauth.reddit.com/by_id/t3_{}", id);

        let resp = client
            .get(&link)
            .query(&[("raw_json", "1")])
            .header(USER_AGENT, "Tidder 0.0.1")
            .bearer_auth(access_token)
            .send()
            .await?
            .error_for_status()?;

        let post = Submission::deserialize(&resp.json::<Value>().await?["data"]["children"][0]["data"])?.finalize()?;

        let (_hash, _hash_dest, image_id, _exists) = save_hash(&post.url, HashDest::Images).await?;

        save_post(&post, Ok(image_id)).await?;
        Ok(())
    } else {
        println!("{:#}", json);

        Err(ue!(format!("Authentication HTTP error: {}", status)))
    }
}

async fn hash(links: &[&str]) -> Result<(), UserError> {
    futures::stream::iter(links.iter())
        .fold(None, async move |last, arg| -> Option<Hash> {
            let res = get_hash(&arg).await;

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
        })
        .await;

    Ok(())
}


#[tokio::main]
async fn main() -> Result<(), UserError> {
    let matches = clap_app!(op =>
        (@subcommand post =>
            (@arg ID: +required "Reddit's ID for the post")
        )
        (@subcommand hash =>
            (@arg LINKS: +required ... "The links you wish to hash")
        )
        (@subcommand save =>
            (@arg ID: +required ... "Reddit's ID for the post you wish to save")
        )
    )
    .get_matches();

    let (op_name, op_matches) = matches.subcommand();
    let op_matches = op_matches.ok_or_else(|| ue!("No subcommand provided"))?;

   match op_name {
        "post" => post(op_matches.value_of("ID").unwrap()).await,
        "hash" => hash(
            &op_matches
                .values_of("LINKS")
                .unwrap()
                .collect::<Vec<_>>(),
        ).await,
        "save" => save(op_matches.value_of("ID").unwrap()).await,
        unknown => Err(ue!(format!("Unknown subcommand '{}'", unknown))),
    }
}
