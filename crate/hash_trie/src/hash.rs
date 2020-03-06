pub fn get_bit(hash: u64, pos: u8) -> u8 {
    ((hash & (1 << pos)) != 0) as u8
}

pub struct HashBits {
    hash: u64,
    pos: u8
}

impl HashBits {
    pub fn new(hash: u64) -> Self {
        Self {
            hash,
            pos: 0
        }
    }

    pub fn new_at(hash: u64, pos: u8) -> Self {
        Self {
            hash,
            pos
        }
    }
}

impl Iterator for HashBits {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos > 63 {
            None
        } else {
            let ret = get_bit(self.hash, self.pos);
            self.pos += 1;
            Some(ret)
        }
    }
}

pub fn distance(a: u64, b: u64) -> u8 {
    (a ^ b).count_ones() as u8
}
