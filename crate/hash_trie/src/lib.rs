use std::fs::OpenOptions;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

mod hash;
use hash::*;

#[derive(Debug, Default, PartialEq)]
pub struct Node {
    zero: Option<usize>,
    one: Option<usize>,
}

#[derive(Debug, Default)]
pub struct HashTrie {
    haystack: Vec<Node>,
}

impl HashTrie {
    pub fn new() -> Self {
        Self {
            haystack: vec![Node::default()],
        }
    }

    pub fn insert(&mut self, hash: u64) -> bool {
        let (start_pos, mut index) = self.search(hash);

        if start_pos == 63 {
            return true;
        }

        for bit in HashBits::new_at(hash, start_pos) {
            let new_node = Node::default();
            let new_index = self.haystack.len();
            self.haystack.push(new_node);

            if bit == 0 {
                self.haystack[index].zero = Some(new_index);
            } else if bit == 1 {
                self.haystack[index].one = Some(new_index);
            }

            index = new_index;
        }

        false
    }

    fn search(&self, needle: u64) -> (u8, usize) {
        let mut current_node = &self.haystack[0];

        let mut next_index = 0;

        for (pos, bit) in HashBits::new(needle).enumerate() {
            next_index = if let (0, Some(index)) = (bit, current_node.zero) {
                index
            } else if let (1, Some(index)) = (bit, current_node.one) {
                index
            } else {
                return (pos as u8, next_index);
            };

            current_node = &self.haystack[next_index];
        }

        (63, next_index)
    }

    pub fn similar(&self, needle: u64, max_distance: u8) -> Similar {
        Similar::new(self, needle, max_distance)
    }

    pub fn read_in(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;

        let len = file.metadata()?.len();

        let mut file = BufReader::new(file);

        let mut new = HashTrie { haystack: Vec::new() };

        for _i in 0..len / 16 {
            let mut zero_bytes = [0, 0, 0, 0, 0, 0, 0, 0];
            let mut one_bytes = [0, 0, 0, 0, 0, 0, 0, 0];

            file.read_exact(&mut zero_bytes)?;
            file.read_exact(&mut one_bytes)?;

            let zero = u64::from_le_bytes(zero_bytes);
            let one = u64::from_le_bytes(one_bytes);

            new.haystack.push(Node {
                zero: if zero == 0 { None } else { Some(zero as usize) },
                one: if one == 0 { None } else { Some(one as usize) },
            });
        }

        Ok(new)
    }

    pub fn write_out(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let mut file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(path)?,
        );

        for node in self.haystack.iter() {
            file.write_all(&node.zero.map(|z| z as u64).unwrap_or(0).to_le_bytes())?;
            file.write_all(&node.one.map(|o| o as u64).unwrap_or(0).to_le_bytes())?;
        }

        file.flush()
    }

    pub fn hashes(&self) -> HashIter {
        HashIter::new(self)
    }
}

impl std::iter::FromIterator<u64> for HashTrie {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = u64>,
    {
        let mut new = Self::new();

        for hash in iter {
            new.insert(hash);
        }

        new
    }
}

struct SimilarBranch<'a> {
    hash: u64,
    pos: u8,
    distance: u8,
    node: &'a Node,
}

pub struct Similar<'a> {
    trie: &'a HashTrie,
    needle: u64,
    max_distance: u8,
    branches: Vec<SimilarBranch<'a>>,
}

impl<'a> Similar<'a> {
    fn new(trie: &'a HashTrie, needle: u64, max_distance: u8) -> Self {
        Self {
            trie,
            needle,
            max_distance,
            branches: vec![SimilarBranch {
                hash: 0,
                pos: 0,
                distance: 0,
                node: &trie.haystack[0],
            }],
        }
    }
}

impl<'a> Iterator for Similar<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(SimilarBranch {
            mut hash,
            mut distance,
            pos: start_pos,
            mut node,
        }) = self.branches.pop()
        {
            for pos in start_pos..=64 {
                let index = match (node.zero, node.one) {
                    (None, None) => {
                        debug_assert_eq!(pos, 64);
                        return Some(hash);
                    }
                    (Some(index), None) => {
                        if get_bit(self.needle, pos) == 0 {
                            index
                        } else {
                            distance += 1;
                            if distance <= self.max_distance {
                                index
                            } else {
                                break;
                            }
                        }
                    }
                    (None, Some(index)) => {
                        hash |= 1 << pos;

                        if get_bit(self.needle, pos) == 1 {
                            index
                        } else {
                            distance += 1;
                            if distance <= self.max_distance {
                                index
                            } else {
                                break;
                            }
                        }
                    }
                    (Some(zero_index), Some(one_index)) => {
                        let needle_bit = get_bit(self.needle, pos);

                        if needle_bit == 1 || distance < self.max_distance {
                            let branch_distance = if needle_bit == 1 {
                                distance
                            } else {
                                distance + 1
                            };

                            self.branches.push(SimilarBranch {
                                hash: hash | 1 << pos,
                                pos: pos + 1,
                                distance: branch_distance,
                                node: &self.trie.haystack[one_index],
                            });
                        }

                        if needle_bit == 0 {
                            zero_index
                        } else {
                            distance += 1;
                            if distance <= self.max_distance {
                                zero_index
                            } else {
                                break;
                            }
                        }
                    }
                };
                debug_assert_ne!(pos, 64);
                node = &self.trie.haystack[index];
            }
        }

        None
    }
}

pub struct HashIter<'a> {
    trie: &'a HashTrie,
    branches: Vec<(u64, u8, &'a Node)>,
}

impl<'a> HashIter<'a> {
    fn new(trie: &'a HashTrie) -> Self {
        Self {
            trie,
            branches: vec![(0, 0, &trie.haystack[0])],
        }
    }
}

impl<'a> Iterator for HashIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((mut hash, start_pos, mut node)) = self.branches.pop() {
            for pos in start_pos..64 {
                let index = match (node.zero, node.one) {
                    (None, None) => unreachable!(),
                    (Some(index), None) => index,
                    (None, Some(index)) => {
                        hash |= 1 << pos;
                        index
                    }
                    (Some(zero_index), Some(one_index)) => {
                        self.branches.push((
                            hash | 1 << pos,
                            pos + 1,
                            &self.trie.haystack[one_index],
                        ));
                        zero_index
                    }
                };
                debug_assert_ne!(pos, 64);
                node = &self.trie.haystack[index];
            }

            Some(hash)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::prelude::*;

    #[test]
    fn inout() {
        let mut input = vec![1, 54, 0, std::u64::MAX, 766];

        let trie: HashTrie = input.iter().copied().collect();
        let mut output = trie.hashes().collect::<Vec<_>>();

        input.sort();
        output.sort();

        assert_eq!(input, output);
    }

    #[test]
    fn random_inout() {
        let mut rng = thread_rng();

        let mut input: Vec<_> = std::iter::repeat_with(|| rng.gen()).take(1000).collect();

        let trie: HashTrie = input.iter().copied().collect();
        let mut output: Vec<_> = trie.hashes().collect();

        input.sort();
        output.sort();

        assert_eq!(input, output);
    }

    #[test]
    fn similar() {
        let input = [
            0b1001, 0b0100, 0b0010, 0b0101, 0b0110, 0b0001, 0b0000, 0b1111, 0b0011,
        ];

        let trie: HashTrie = input.iter().copied().collect();

        let needle = 0b0010;
        let max_distance = 1;
        let mut should_match = vec![0b0000, 0b0011, 0b0010, 0b0110];
        should_match.sort();

        let mut matches: Vec<_> = trie.similar(needle, max_distance).collect();
        matches.sort();

        assert_eq!(should_match, matches);
    }

    #[test]
    fn save() {
        let mut rng = thread_rng();

        let mut input: Vec<u64> = std::iter::repeat_with(|| rng.gen()).take(20000).collect();

        let in_trie: HashTrie = input.iter().copied().collect();

        in_trie.write_out("/tmp/test.hashtrie").unwrap();

        let out_trie = HashTrie::read_in("/tmp/test.hashtrie").unwrap();

        assert_eq!(in_trie.haystack, out_trie.haystack);
    }
}
