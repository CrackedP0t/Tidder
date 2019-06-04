use common::*;

fn main() {
    for arg in std::env::args().skip(1) {
        println!("{}", get_hash(&arg).unwrap().0);
    }
}
