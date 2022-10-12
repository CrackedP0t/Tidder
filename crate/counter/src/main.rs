use anyhow::Error;
use chrono::prelude::*;
use futures::TryStreamExt;
use sqlx::postgres::PgPool;
use sqlx::query;
use tokio::fs::{remove_file, OpenOptions};
use tokio::prelude::*;

const PATH: &str = "months.csv";

#[tokio::main]
async fn main() -> Result<(), Error> {
    dotenv::dotenv()?;

    let pool = PgPool::new(&std::env::var("DATABASE_URL")?).await?;

    let q = query!(
        "SELECT DATE_TRUNC('month', created_utc) AS month, COUNT(*) FROM posts GROUP BY month;"
    )
    .fetch(&pool);

    if std::path::Path::new(PATH).exists() {
        remove_file(PATH).await?;
    }

    q.try_for_each(|r| async move {
        let mut out_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(PATH)
            .await?;

        let date = r.month.unwrap().date();
        out_file
            .write_all(
                format!("{}-{},{}\n", date.year(), date.month(), r.count.unwrap()).as_bytes(),
            )
            .await?;
        Ok(())
    })
    .await?;

    Ok(())
}
