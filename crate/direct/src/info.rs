use serde::Deserialize;

#[derive(Deserialize)]
pub struct Child {
    pub data: common::Submission
}

#[derive(Deserialize)]
pub struct Data {
    pub children: Vec<Child>
}

#[derive(Deserialize)]
pub struct Info {
    pub data: Data
}
