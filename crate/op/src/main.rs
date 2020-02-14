use clap::clap_app;
use common::format;
use common::*;
use futures::prelude::*;
use reqwest::{header::USER_AGENT, Client};
use serde::Deserialize;
use serde_json::Value;
use std::io::Write;

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

        println!(
            "{:#}",
            resp.json::<Value>().await?["data"]["children"][0]["data"]
        );

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

        let post =
            Submission::deserialize(&resp.json::<Value>().await?["data"]["children"][0]["data"])?
                .finalize()?;

        let hash_saved = save_hash(&post.url, HashDest::Images).await?;

        post.save(Ok(hash_saved.id)).await?;
        Ok(())
    } else {
        println!("{:#}", json);

        Err(ue!(format!("Authentication HTTP error: {}", status)))
    }
}

async fn hash(links: &[&str]) -> Result<(), UserError> {
    futures::stream::iter(links.iter())
        .fold(None, move |last, arg| async move {
            let HashGotten { hash, end_link, .. } = match get_hash(&arg).await {
                Ok(res) => res,
                Err(e) => {
                    warn!("{} failed: {:?}", arg, e);
                    return last;
                }
            };

            let mut out = format!("{}: {}", end_link, hash);
            if let Some(last) = last {
                out = format!("{} ({})", out, distance(hash, last));
            }
            println!("{}", out);

            Some(hash)
        })
        .await;

    Ok(())
}

async fn search(link: &str, distance: Option<i64>) -> Result<(), UserError> {
    const DEFAULT_DISTANCE: i64 = 2;

    let distance = distance.unwrap_or(DEFAULT_DISTANCE);

    let resp = reqwest::get(link).await?.error_for_status()?;
    let image = resp.bytes().await?;
    let hash = hash_from_memory(&image)?;

    let found = PG_POOL
        .get()
        .await?
        .query(
            "SELECT hash <-> $1 as distance, images.link, permalink, \
             score, author, created_utc, subreddit, title \
             FROM posts INNER JOIN images \
             ON hash <@ ($1, $2) \
             AND image_id = images.id \
             ORDER BY distance ASC, created_utc ASC",
            &[&hash, &distance],
        )
        .await?;

    for row in found {
        println!(
            "{} | {} | {} | {} | {} | /r/{} | {} | {}",
            row.get::<_, i64>("distance"),
            row.get::<_, chrono::NaiveDateTime>("created_utc"),
            row.get::<_, i64>("score"),
            row.get::<_, &str>("link"),
            row.get::<_, &str>("permalink"),
            row.get::<_, &str>("subreddit"),
            row.get::<_, &str>("author"),
            row.get::<_, &str>("title")
        );
    }

    Ok(())
}

async fn rank() -> Result<(), UserError> {
    let rows = PG_POOL
        .get()
        .await?
        .query(
            "SELECT COUNT(*) AS num,
             (SELECT link FROM images AS images2 WHERE images.hash <@ (images2.hash, 0) LIMIT 1) AS link
             FROM images GROUP BY hash ORDER BY num DESC LIMIT 100", &[]).await?;

    let commons = CommonImages {
        as_of: chrono::offset::Utc::now(),
        common_images: rows
            .iter()
            .map(|row| CommonImage {
                num: row.get::<_, i64>("num") as u64,
                link: row.get("link"),
            })
            .collect::<Vec<_>>(),
    };

    std::fs::File::create(std::env::var("HOME")? + "/stats/top100.ron")?
        .write_all(ron::ser::to_string_pretty(&commons, Default::default())?.as_bytes())?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), UserError> {
    setup_logging!();

    let matches = clap_app!(op =>
        (@subcommand hash =>
         (@arg LINKS: +required ... "The links you wish to hash")
        )
        (@subcommand post =>
         (@arg ID: +required "Reddit's ID for the post")
        )
        (@subcommand rank => )
        (@subcommand save =>
         (@arg ID: +required ... "Reddit's ID for the post you wish to save")
        )
        (@subcommand search =>
         (@arg LINK: +required ... "The link to the image you wish to search for")
         (@arg distance: -d --distance +takes_value "The max distance you'll accept")
        )
    )
    .get_matches();

    let (op_name, op_matches) = matches.subcommand();
    let op_matches = op_matches.ok_or_else(|| ue!("No subcommand provided"))?;

    match op_name {
        "hash" => hash(&op_matches.values_of("LINKS").unwrap().collect::<Vec<_>>()).await,
        "post" => post(op_matches.value_of("ID").unwrap()).await,
        "rank" => rank().await,
        "save" => save(op_matches.value_of("ID").unwrap()).await,
        "search" => {
            search(
                op_matches.value_of("LINK").unwrap(),
                op_matches
                    .value_of("distance")
                    .map(|d| d.parse())
                    .transpose()?,
            )
            .await
        }
        unknown => Err(ue!(format!("Unknown subcommand '{}'", unknown))),
    }
}
