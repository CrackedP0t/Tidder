use common::*;
use fallible_iterator::FallibleIterator;
use postgres::{error::DbError, Client, NoTls};

fn main() {
    // for arg in std::env::args().skip(1) {
    //     let (hash, link, _get_kind) = get_hash(&arg).unwrap();
    //     println!("{}: {}", link, hash);
    // }

    let mut i_client = Client::connect(&SECRETS.postgres.connect, NoTls).unwrap();

    let mut o_client = Client::connect(&SECRETS.postgres.connect, NoTls).unwrap();

    let u_statement = o_client
        .prepare("UPDATE images SET link = $1 WHERE id = $2")
        .unwrap();

    let e_statement = o_client
        .prepare(
            "UPDATE posts SET image_id = (SELECT id FROM images WHERE link = $1) \
             WHERE image_id = (SELECT id FROM images WHERE link = $2)",
        )
        .unwrap();

    let d_statement = o_client
        .prepare("DELETE FROM images WHERE link = $1")
        .unwrap();

    let mut rows = i_client
        .query_iter(
            r"SELECT id, link FROM images WHERE link ~ 'https?://imgur.com/[[:alnum:]_]+$'",
            &[],
        )
        .unwrap();

    while let Some(row) = rows.next().unwrap() {
        let link: String = row.get("link");
        let i_link = match follow_imgur(&link) {
            Ok(Some(link)) => link,
            Ok(None) => {
                println!("No follow found for {}", link);
                continue;
            }
            Err(e) => {
                println!("{}", e);
                continue;
            }
        };

        println!("Changing {} to {}", link, i_link);

        let mut trans = o_client.transaction().unwrap();
        if let Err(e) = trans.execute(&u_statement, &[&i_link, &row.get::<_, i64>("id")]) {
            if let Some(cause) = e.into_source() {
                let cause = cause.downcast::<DbError>().unwrap();
                if cause.constraint() == Some("images_link_key") {
                    drop(trans);
                    let mut trans = o_client.transaction().unwrap();
                    trans.execute(&e_statement, &[&i_link, &link]).unwrap();
                    trans.commit().unwrap();

                    let mut trans = o_client.transaction().unwrap();
                    trans.execute(&d_statement, &[&link]).unwrap();
                    trans.commit().unwrap();
                } else {
                    panic!("Update error");
                }
            } else {
                panic!("Update error");
            }
        } else {
            trans.commit().unwrap();
        }
    }
}
