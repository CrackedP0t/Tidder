use super::*;
use url::Url;

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
    #[serde(default)]
    pub is_video: bool,
    pub over_18: bool,
    pub permalink: String,
    #[serde(default, deserialize_with = "de_sub::preview")]
    pub preview: Option<String>,
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
    pub fn choose_url(&self) -> Result<Url, UserError> {
        if self.is_video {
            return Url::parse(
                &self
                    .preview
                    .as_ref()
                    .ok_or_else(|| ue_save!("is_video but no preview", "video_no_preview"))?,
            )
            .map_err(map_ue_save!("invalid URL", "url_invalid"));
        }

        let post_url = Url::parse(&self.url).map_err(map_ue_save!("invalid URL", "url_invalid"))?;

        if let Some("v.redd.it") = post_url.host_str() {
            Url::parse(
                self.preview
                    .as_ref()
                    .ok_or_else(|| ue_save!("v.redd.it but no preview", "v_redd_it_no_preview"))?,
            )
            .map_err(map_ue_save!("invalid URL", "url_invalid"))
        } else {
            Ok(post_url)
        }
    }

    pub fn unescape(s: &str) -> String {
        s.replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&amp;", "&")
    }

    pub fn finalize(mut self) -> Result<Self, UserError> {
        self.url = Self::unescape(&self.url);
        self.title = Self::unescape(&self.title);
        self.preview = self.preview.map(|p| Self::unescape(&p));

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
        static ID_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"/comments/([^/]+)/").unwrap());

        let reddit_id = String::from(
            ID_RE
                .captures(&self.permalink)
                .and_then(|cap| cap.get(1))
                .ok_or_else(|| ue!("Couldn't find ID in permalink"))?
                .as_str(),
        );

        let client = PG_POOL.get().await?;

        let modified = match image_id {
            Ok(image_id) => {
                let stmt = client
                    .prepare(
                        "INSERT INTO posts \
                         (reddit_id, link, permalink, author, \
                         created_utc, score, subreddit, title, nsfw, \
                         spoiler, image_id, is_video, preview, reddit_id_int, \
                         thumbnail, thumbnail_width, thumbnail_height, \
                         crosspost_parent) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, \
                         $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18) \
                         ON CONFLICT DO NOTHING",
                    )
                    .await?;
                client
                    .execute(
                        &stmt,
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
                            &self.is_video,
                            &self.preview,
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
                let stmt = client
                    .prepare(
                        "INSERT INTO posts \
                         (reddit_id, link, permalink, author, \
                         created_utc, score, subreddit, title, nsfw, \
                         spoiler, reddit_id_int, thumbnail, \
                         thumbnail_width, thumbnail_height, save_error, \
                         crosspost_parent, is_video, preview) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, \
                         $10, $11, $12, $13, $14, $15, $16, $17, $18) \
                         ON CONFLICT DO NOTHING",
                    )
                    .await?;
                client
                    .execute(
                        &stmt,
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
                            &self.is_video,
                            &self.preview,
                        ],
                    )
                    .await?
            }
        };

        Ok(modified > 0)
    }
}

mod de_sub {
    use super::*;
    use serde::de::{self, Deserializer, Unexpected, Visitor};
    use std::fmt::{self, Formatter};

    pub fn created_utc<'de, D>(des: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CreatedUTC;
        impl<'de> Visitor<'de> for CreatedUTC {
            type Value = NaiveDateTime;

            fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
                write!(formatter, "a number, possibly inside a string")
            }

            fn visit_u64<E>(self, secs: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_i64(secs as i64)
            }

            fn visit_i64<E>(self, secs: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(NaiveDateTime::from_timestamp(secs, 0))
            }

            fn visit_str<E>(self, secs: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let secs = secs
                    .parse()
                    .map_err(|_e| E::invalid_value(Unexpected::Str(secs), &self))?;
                self.visit_i64(secs)
            }

            fn visit_f64<E>(self, secs: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_i64(secs as i64)
            }
        }

        des.deserialize_any(CreatedUTC)
    }

    pub fn crosspost_parent<'de, D>(des: D) -> Result<Option<i64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CrosspostParent;
        impl<'de> Visitor<'de> for CrosspostParent {
            type Value = Option<i64>;

            fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
                write!(formatter, "t3_<id>")
            }

            fn visit_some<D>(self, des: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                des.deserialize_str(self)
            }

            fn visit_str<E>(self, name: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                static T3_RE: Lazy<Regex> =
                    Lazy::new(|| Regex::new("^t3_([[:alnum:]]+)$").unwrap());

                T3_RE
                    .captures(name)
                    .and_then(|cs| cs.get(1))
                    .and_then(|id| i64::from_str_radix(id.as_str(), 36).ok())
                    .ok_or_else(|| E::invalid_value(Unexpected::Str(name), &self))
                    .map(Some)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(None)
            }
        }

        des.deserialize_option(CrosspostParent)
    }

    pub fn preview<'de, D>(des: D) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Resolution {
            url: String,
        }

        #[derive(Deserialize)]
        struct Image {
            source: Resolution,
        }

        #[derive(Deserialize)]
        struct Preview {
            images: [Image; 1],
        }

        Ok(Option::<Preview>::deserialize(des)?.map(|p| {
            let [i] = p.images;
            i.source.url
        }))
    }
}
