use common::*;

use bytes::BytesMut;
use futures::prelude::*;
use once_cell::sync::Lazy;
use regex::Regex;

const BASE_STREAM_URL: &str = "https://stream.pushshift.io?type=submissions";

const NEWLINE_CODE: u8 = 10;

async fn process_event(data: &[u8]) -> Result<Option<i64>, UserError> {
    static PARSER: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"id: (\d+)\nevent: (\w+)\ndata: (.+)$").unwrap());

    let text = std::str::from_utf8(data)?;

    let captures = PARSER
        .captures(text)
        .ok_or(ue!(format!("Unexpected event format: `{}`", text)))?;

    let (id, event, data, extra) = (
        captures.get(1).unwrap().as_str().parse().unwrap(),
        captures.get(2).unwrap().as_str(),
        captures.get(3).unwrap().as_str(),
    );

    match event {
        "rs" => {
            let post = serde_json::from_str::<Submission>(data)?;

            println!("{}", post.title);

            Ok(Some(id))
        },
        "keepalive" => Ok(None),
        other => {
            Err(ue!(format!("Unexpected event `{}`", other)))
        }
    }
 }

async fn stream(mut last_id: Option<i64>) -> Result<(), (Option<i64>, UserError)> {
    let client = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .build()
        .map_err(|e| (last_id, e.into()))?;

    let req_url = match last_id {
        None => BASE_STREAM_URL.to_string(),
        Some(last_id) => format!("{}&{}", BASE_STREAM_URL, last_id),
    };

    let mut bytes_stream = client
        .get(&req_url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| (last_id, e.into()))?
        .bytes_stream();

    let mut current_data = BytesMut::new();

    loop {
        let bytes = bytes_stream
            .try_next()
            .await
            .map_err(|e| (last_id, e.into()))?
            .unwrap();

        let boundary = bytes
            .windows(2)
            .position(|window| window[0] == NEWLINE_CODE && window[1] == NEWLINE_CODE);

        match boundary {
            None => current_data.extend_from_slice(&bytes),
            Some(index) => {
                current_data.extend_from_slice(&bytes.slice(0..index));

                last_id = process_event(&current_data)
                    .await
                    .map_err(|e| (last_id, e))?
                    .or(last_id);

                current_data.clear();
                current_data.extend_from_slice(&bytes.slice(index + 2..bytes.len()));
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), UserError> {
    stream(None).await.map(drop).map_err(|(_i, ue)| ue)
}
