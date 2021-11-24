use criterion::{criterion_group, criterion_main};
mod merge_pcaps;
mod tournament_tree;

criterion_group!(merge, tournament_tree::identical_inputs);
criterion_group!(
    stream_decompress_and_merge_pcaps,
    merge_pcaps::stream_and_decompress_throughput
);
criterion_main!(/*merge,*/ stream_decompress_and_merge_pcaps);
