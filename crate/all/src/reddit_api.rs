use serde::Deserialize;
use common::*;

#[derive(Deserialize)]
pub struct Child {
    pub data: Submission
}

#[derive(Deserialize)]
pub struct Data {
    pub children: Vec<Child>,
    pub after: String,
    pub dist: u32,
    pub modhash: String
}

#[derive(Deserialize)]
pub struct SubredditListing {
    pub data: Data,
}
