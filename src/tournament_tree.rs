/* TODO: is there a more idiomatic way to express this? Maybe there is some standard trait for a comparable/orderable key which can be produced from data and saved within the tree */
pub trait Mergeable {
    type Data: ?Sized;

    fn peek_timestamp(&mut self) -> u64; // TODO: make this any "copy, sortable key?"
    fn pop(&mut self) -> Option<&Self::Data>;
}
pub struct Tree<T: Mergeable> {
    needs_updating: bool,
    winning_value_index: usize,
    nodes: Vec<u16>,
    values: Vec<u64>,
    input_streams: Vec<T>, // each input stream is held in memory next to its last popped data
}
impl<T: Mergeable> Tree<T> {
    // TODO: rather than taking an explict vector, maybe take anything iterable? Might need to solicit some help from the rust users forum
    pub fn new(input_streams: Vec<T>) -> Tree<T> {
        let n_leaf_nodes = if input_streams.len() == 1 {
            1
        } else {
            input_streams.len().next_power_of_two()
        };

        let values = vec![std::u64::MAX; n_leaf_nodes];
        let nodes = vec![(n_leaf_nodes - 1) as u16; n_leaf_nodes];
        let mut tree = Tree::<T> {
            needs_updating: true,
            winning_value_index: 0,
            nodes,
            values,
            input_streams: Vec::<T>::with_capacity(input_streams.len()),
        };

        // iterate through input streams and build the tree
        let mut streams = input_streams.into_iter();
        // - call pop for each stream and add it as a leaf node
        // - walk to the parent and do compares to determine the winner
        for i in 0..n_leaf_nodes as usize {
            if let Some(mut stream) = streams.next() {
                let value = stream.peek_timestamp();
                tree.values[i] = value;
                tree.input_streams.push(stream);
            }

            if i % 2 != 0 {
                // compute the winner and propagate it up the tree
                let winning_value_index = if tree.values[i] < tree.values[i - 1] {
                    i
                } else {
                    i - 1
                };
                let parent = (tree.nodes.len() >> 1) + (i >> 1);
                tree.nodes[parent] = winning_value_index as u16;
            }
        }

        // fill in each subsequent level of the tournament tree one at a time
        let mut level_start = tree.nodes.len() >> 2; // we've already filled in the first parent level. start at the second
        while level_start > 0 {
            for i in level_start..(2 * level_start) {
                let left_child = tree.nodes[2 * i];
                let right_child = tree.nodes[2 * i + 1];
                tree.nodes[i] =
                    if tree.values[left_child as usize] < tree.values[right_child as usize] {
                        left_child
                    } else {
                        right_child
                    };
                //println!("updated {} = {}", i, tree.nodes[i]);

                //println!("updating {} -> {}, {}", level_start, level_start >> 1, i);
            }
            level_start >>= 1; // move on to populating the next parent
        }
        if tree.nodes.len() > 1 {
            tree.winning_value_index = tree.nodes[1] as usize;
        }
        //println!("BEGIN tree: {:?} {:?}", &tree.nodes[1..], &tree.values[..]);
        tree.needs_updating = false;
        tree
    }

    // TODO: make this faster
    fn update_winner(&mut self, changed_value_index: u16) {
        //println!("BEFORE tree: {:?} {:?}", &self.nodes[1..], &self.values[..]);
        //let mut parent = self.nodes.len() - 1 - (changed_value_index + 1 >> 1) as usize;        //let mut parent = self.nodes.len() - 1 - ((self.nodes.len()>>1) - ((changed_value_index as usize) >> 1));
        if self.nodes.len() > 1 {
            let parent = (self.nodes.len() >> 1) + (changed_value_index >> 1) as usize;
            // the index that was changed was our previous winner
            let mut winning_value = self.values[changed_value_index as usize];
            let mut winning_value_index = changed_value_index;
            let sibling_value = self.values[winning_value_index as usize ^ 1];

            //println!("winning {} sibling {}", winning_value, sibling_value);
            if sibling_value < winning_value {
                winning_value = sibling_value;
                winning_value_index ^= 1;
            } //println!("MIDDLE tree: {:?} {:?}", &self.nodes[1..], &self.values[..]);

            if parent > 1 {
                self.nodes[parent] = winning_value_index;

                let mut changed_index = parent;
                while changed_index > 3 {
                    let parent = changed_index >> 1;
                    let sibling_value_index = self.nodes[changed_index ^ 1];
                    let sibling_value = self.values[sibling_value_index as usize];

                    // only need to update winning_value_index or winning_value if they have changed
                    if sibling_value < winning_value {
                        winning_value_index = sibling_value_index;
                        winning_value = sibling_value;
                    }

                    self.nodes[parent] = winning_value_index;
                    changed_index = parent;
                }
                let sibling_value_index = self.nodes[changed_index ^ 1];
                let sibling_value = self.values[sibling_value_index as usize];
                if winning_value > sibling_value {
                    winning_value_index = sibling_value_index;
                }
            }

            self.winning_value_index = winning_value_index as usize;
        } //println!("AFTER tree: {:?} {:?}", &self.nodes[1..], &self.values[..]);
    }

    pub fn pop(&mut self) -> std::option::Option<&<T>::Data> {
        if self.needs_updating {
            let winner_stream_index = self.winning_value_index;
            self.values[winner_stream_index] =
                self.input_streams[winner_stream_index].peek_timestamp();
            self.update_winner(winner_stream_index as u16);
        }

        if self.values[self.winning_value_index] == std::u64::MAX {
            None
        } else {
            let winner_stream_index = self.winning_value_index;
            self.needs_updating = true; // from here on out, we will always need to call peek on the last
            self.input_streams[winner_stream_index].pop()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct InputStream<T: Iterator<Item = u64>> {
        iterator: std::iter::Peekable<T>,
        current_value: Option<u64>,
    }
    impl<T: Iterator<Item = u64>> InputStream<T> {
        fn new(iterator: T) -> InputStream<T> {
            InputStream {
                iterator: iterator.peekable(),
                current_value: None,
            }
        }
    }
    // TODO: rather than taking an "InputStream", just take an iterator which returns tuples of
    // (Value,Data) as input to tournament_tree::Tree::new?
    impl<T: Iterator<Item = u64>> Mergeable for InputStream<T> {
        type Data = <T>::Item;

        // returns an tuple of (timestamp,data) or None
        fn pop(&mut self) -> Option<&<T>::Item> {
            self.current_value = self.iterator.next();
            self.current_value.as_ref()
        }

        fn peek_timestamp(&mut self) -> u64 {
            //println!("peeking {:?}", self.iterator.peek());
            *self.iterator.peek().unwrap_or(&std::u64::MAX)
        }
    }
    #[test]
    fn produces_all_output() {
        let inputs = vec![InputStream::new(vec![1, 1, 2, 6, 8, 8, 9].into_iter())];

        let mut tree = Tree::new(inputs);
        let expected_outputs = vec![1, 1, 2, 6, 8, 8, 9];
        for expected in expected_outputs {
            if let Some(popped) = tree.pop() {
                assert_eq!(*popped, expected);
            } else {
                panic!("Tree returned empty. Expected {}", expected);
            }
        }
        assert!(tree.pop().is_none(), "Tree should be empty but isn't");
    }
    #[test]
    fn produces_all_ordered_output_2() {
        let inputs = vec![
            InputStream::new(vec![2, 4, 5, 7].into_iter()),
            InputStream::new(vec![1, 1, 2, 6, 8, 8, 9].into_iter()),
        ];

        let mut tree = Tree::new(inputs);
        let expected_outputs = vec![1, 1, 2, 2, 4, 5, 6, 7, 8, 8, 9];
        for expected in expected_outputs {
            if let Some(popped) = tree.pop() {
                println!("popped {}", popped);
                assert_eq!(*popped, expected);
            } else {
                panic!("Tree returned empty. Expected {}", expected);
            }
        }
        assert!(tree.pop().is_none(), "Tree should be empty but isn't");
    }
    #[test]
    fn produces_all_ordered_output_3() {
        let inputs = vec![
            InputStream::new(vec![4, 5, 7].into_iter()),
            InputStream::new(vec![2, 3, 5, 7].into_iter()),
            InputStream::new(vec![1, 1, 2, 6, 8, 8, 9].into_iter()),
        ];

        let mut tree = Tree::new(inputs);
        let expected_outputs = vec![1, 1, 2, 2, 3, 4, 5, 5, 6, 7, 7, 8, 8, 9];
        for expected in expected_outputs {
            if let Some(popped) = tree.pop() {
                println!("popped {}", popped);
                assert_eq!(*popped, expected);
            } else {
                panic!("Tree returned empty. Expected {}", expected);
            }
        }
        assert!(tree.pop().is_none(), "Tree should be empty but isn't");
    }
    #[test]
    fn produces_all_ordered_output_5() {
        let inputs = vec![
            InputStream::new(vec![4, 5, 7].into_iter()),
            InputStream::new(vec![4, 5, 7].into_iter()),
            InputStream::new(vec![2, 3, 5, 7].into_iter()),
            InputStream::new(vec![4, 5, 7].into_iter()),
            InputStream::new(vec![1, 1, 2, 6, 8, 8, 9].into_iter()),
        ];

        let mut tree = Tree::new(inputs);
        let expected_outputs = vec![1, 1, 2, 2, 3, 4, 4, 4, 5, 5, 5, 5, 6, 7, 7, 7, 7, 8, 8, 9];
        for expected in expected_outputs {
            if let Some(popped) = tree.pop() {
                println!("popped {}", popped);
                assert_eq!(*popped, expected);
            } else {
                panic!("Tree returned empty. Expected {}", expected);
            }
        }
        assert!(tree.pop().is_none(), "Tree should be empty but isn't");
    }
}
