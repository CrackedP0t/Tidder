use common::*;
use futures::future::Future;

fn main() {
    std::env::args().skip(1).fold(None, |last, arg| {
        let (hash, link, _get_kind) = match get_hash(arg.clone()).wait() {
            Ok(res) => res,
            Err(e) => {
                println!("{} failed: {}", arg, e);
                return last;
            }
        };
        let mut out = format!("{}: {}", link, hash);
        if let Some(last) = last {
            out = format!("{} ({})", out, distance(hash, last));
        }
        println!("{}", out);

        Some(hash)
    });
}
