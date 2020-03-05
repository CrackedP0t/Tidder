mod hash;
use hash::*;

#[derive(Debug, Default)]
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

    pub fn search(&self, needle: u64) -> (u8, usize) {
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
            for pos in start_pos..=64 {
                let index = match (node.zero, node.one) {
                    (None, None) => {
                        debug_assert_eq!(pos, 64);
                        return Some(hash);
                    }
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

            unreachable!()
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
    fn works() {
        let mut input = vec![1, 54, 0, std::u64::MAX, 766];

        let trie: HashTrie = input.iter().copied().collect();
        let mut output = trie.hashes().collect::<Vec<_>>();

        input.sort();
        output.sort();

        assert_eq!(input, output);
    }

    #[test]
    fn random_fill() {
        let mut rng = thread_rng();

        let mut input: Vec<_> = std::iter::repeat_with(|| rng.gen()).take(1000).collect();

        let trie: HashTrie = input.iter().copied().collect();
        let mut output: Vec<_> = trie.hashes().collect();

        input.sort();
        output.sort();

        assert_eq!(input, output);
    }
}
