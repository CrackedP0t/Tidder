use super::*;

#[derive(Deserialize, Debug)]
pub struct Submission {
    #[serde(default)]
    pub id_int: i64,
    pub id: String,
    pub author: String,
    #[serde(deserialize_with = "de_sub::created_utc")]
    pub created_utc: NaiveDateTime,
    #[serde(default, deserialize_with = "de_sub::crosspost_parent")]
    pub crosspost_parent: Option<i64>,
    pub is_self: bool,
    pub over_18: bool,
    pub permalink: String,
    pub promoted: Option<bool>,
    pub score: i64,
    pub spoiler: Option<bool>,
    pub subreddit: String,
    pub title: String,
    pub thumbnail: Option<String>,
    pub thumbnail_width: Option<i32>,
    pub thumbnail_height: Option<i32>,
    #[serde(default)]
    pub updated: Option<NaiveDateTime>,
    pub url: String,
}

impl Submission {
    pub fn finalize(mut self) -> Result<Self, UserError> {
        self.id_int = i64::from_str_radix(&self.id, 36).map_err(|e| {
            UserError::new_source(
                format!("Couldn't parse number from ID '{}'", self.id),
                Source::Internal,
                e,
            )
        })?;

        Ok(self)
    }

    pub async fn save(
        &self,
        image_id: Result<i64, Option<Cow<'static, str>>>,
    ) -> Result<bool, UserError> {
        lazy_static! {
            static ref ID_RE: Regex = Regex::new(r"/comments/([^/]+)/").unwrap();
        }

        let reddit_id = String::from(
            ID_RE
                .captures(&self.permalink)
                .and_then(|cap| cap.get(1))
                .ok_or_else(|| ue!("Couldn't find ID in permalink"))?
                .as_str(),
        );

        let mut client = PG_POOL.take().await?;
        let trans = client.transaction().await?;

        let modified = match image_id {
            Ok(image_id) => {
                trans
                    .execute(
                        "INSERT INTO posts \
                         (reddit_id, link, permalink, author, \
                         created_utc, score, subreddit, title, nsfw, \
                         spoiler, image_id, reddit_id_int, \
                         thumbnail, thumbnail_width, thumbnail_height, \
                         crosspost_parent) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, \
                         $8, $9, $10, $11, $12, $13, $14, $15, $16) \
                         ON CONFLICT DO NOTHING",
                        &[
                            &reddit_id,
                            &self.url,
                            &self.permalink,
                            &self.author,
                            &self.created_utc,
                            &self.score,
                            &self.subreddit,
                            &self.title,
                            &self.over_18,
                            &self.spoiler.unwrap_or(false),
                            &image_id,
                            &i64::from_str_radix(&reddit_id, 36).unwrap(),
                            &self.thumbnail,
                            &self.thumbnail_width,
                            &self.thumbnail_height,
                            &self.crosspost_parent,
                        ],
                    )
                    .await?
            }
            Err(save_error) => {
                trans
                    .execute(
                        "INSERT INTO posts \
                         (reddit_id, link, permalink, author, \
                         created_utc, score, subreddit, title, nsfw, \
                         spoiler, reddit_id_int, thumbnail, \
                         thumbnail_width, thumbnail_height, save_error, \
                         crosspost_parent) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, \
                         $8, $9, $10, $11, $12, $13, $14, $15, $16) \
                         ON CONFLICT DO NOTHING",
                        &[
                            &reddit_id,
                            &self.url,
                            &self.permalink,
                            &self.author,
                            &self.created_utc,
                            &self.score,
                            &self.subreddit,
                            &self.title,
                            &self.over_18,
                            &self.spoiler.unwrap_or(false),
                            &i64::from_str_radix(&reddit_id, 36).unwrap(),
                            &self.thumbnail,
                            &self.thumbnail_width,
                            &self.thumbnail_height,
                            &save_error,
                            &self.crosspost_parent,
                        ],
                    )
                    .await?
            }
        };

        trans.commit().await?;

        Ok(modified > 0)
    }
}
