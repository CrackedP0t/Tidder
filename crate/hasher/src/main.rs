use common::*;

fn main() {
    std::env::args().skip(1).fold(None, |last, arg| {
        let (hash, link, _get_kind) = get_hash(&arg).unwrap();
        let mut out = format!("{}: {}", link, hash);
        if let Some(last) = last {
            out = format!("{} ({})", out, distance(hash, last));
        }
        println!("{}", out);

        Some(hash)
    });
}
