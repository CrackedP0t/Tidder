use common::*;

fn main() {
    for arg in std::env::args().skip(1) {
        let (hash, link, _get_kind) = get_hash(&arg).unwrap();
        println!("{}: {}", link, hash);
    }
}
