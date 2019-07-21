use common::*;
use futures::{
    future::{ok, Future},
    stream::{iter_ok, Stream},
};

fn main() {
    let args = std::env::args().skip(1).map(|arg| arg.to_string()).collect::<Vec<String>>();
    tokio::run(
        iter_ok::<_, ()>(args.into_iter()).fold(None, |last, arg| {
            get_hash(arg.clone()).then(move |res| {
                let (hash, link, _get_kind) = match res {
                    Ok(res) => res,
                    Err(e) => {
                        println!("{} failed: {}", arg, e);
                        return ok(last);
                    }
                };
                let mut out = format!("{}: {}", link, hash);
                if let Some(last) = last {
                    out = format!("{} ({})", out, distance(hash, last));
                }
                println!("{}", out);

                ok(Some(hash))
            })
        }).map(|_| ())
    );
}
