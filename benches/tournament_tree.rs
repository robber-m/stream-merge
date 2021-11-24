use criterion::{black_box, BenchmarkId, Criterion};
use itertools::Itertools;
use stream_merge;

pub struct InputStream<T: Iterator<Item = u64>> {
    iterator: std::iter::Peekable<T>,
    current_value: Option<u64>,
}
impl<T: Iterator<Item = u64>> InputStream<T> {
    pub fn new(iterator: T) -> InputStream<T> {
        InputStream {
            iterator: iterator.peekable(),
            current_value: None,
        }
    }
} // TODO: rather than taking an "InputStream", just take an iterator which returns tuples of
  // (Value,Data) as input to tournament_tree::Tree::new?
impl<T: Iterator<Item = u64>> stream_merge::tournament_tree::Mergeable for InputStream<T> {
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

const STEP: usize = 1; // change this to 3, for example, to run every 3rd power of two
const BENCHMARK_N_INPUTS: [usize; 13] = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096];
const N_STEPS: usize = BENCHMARK_N_INPUTS.len();

pub fn identical_inputs(c: &mut Criterion) {
    let mut group = c.benchmark_group("Merge K Identical Streams");
    group.throughput(criterion::Throughput::Elements(1));
    for n_streams in BENCHMARK_N_INPUTS.iter().step_by(STEP).take(N_STEPS) {
        group.bench_with_input(
            BenchmarkId::new("TournamentTree", n_streams),
            n_streams,
            |b, n_streams| {
                let mut inputs = Vec::<
                    InputStream<
                        <std::ops::Range<u64> as IntoIterator>::IntoIter,
                    >,
                >::with_capacity(*n_streams);
                for _ in 0..*n_streams {
                    inputs.push(InputStream::new((0..std::u64::MAX).into_iter()));
                }
                let mut tree = stream_merge::tournament_tree::Tree::new(inputs);
                b.iter(|| {
                    let _popped = black_box(tree.pop())
                        .expect("I thought this iterator would yield values for forever");
                    //println!("popped {}", _popped/1000 );
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("Kmerge", n_streams),
            n_streams,
            |b, n_streams| {
                let mut inputs =
                    Vec::<<std::ops::Range<u64> as IntoIterator>::IntoIter>::with_capacity(
                        *n_streams,
                    );
                for _ in 0..*n_streams {
                    inputs.push((0..std::u64::MAX).into_iter());
                }
                let mut merged = inputs.into_iter().kmerge();
                b.iter(|| {
                    let _popped = black_box(merged.next())
                        .expect("I thought this iterator would yield values for forever");
                    //println!("popped {}", _popped/1000 );
                })
            },
        );
    }
    group.finish();
}
