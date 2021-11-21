# Work in progress - crate stream-merge
Merge together and efficiently time-sort compressed .pcap files stored in AWS S3 object storage (or locally) to stdout for pipelined processing.
High performance and parallel implementation for > 10 Gbps playback throughput with large numbers of files (~4k).

## `merge_pcaps`
A utility executable for merging a collection of .pcap files to `stdout` in time-sequence.

Note: All input .pcap files must be time-ordered (common case).

  - Stream thousands of huge (bigger than can be downloaded to your system) compressed .pcap files from AWS S3 to another machine,
    which orchestrates parallel downloading and file prioritization, parallel decompression, and time-ordering of packets.
  - Scalable high performance. Architected for efficient use of resources (compute and memory, even with large numbers of files)
    and a target > 1.2GB/s output throughput (a modest target which this implementation can outperform
    with Zstd-compressed .pcap.zst files stored in S3).

    In my specific target usage of this utility, > 4,000 .pcap.gz files (hundreds of gigabytes) stored in
    S3 need to be decompressed and merged together to `stdout`. An interesting note about my environment,
    however, is that the .pcap files I merge together are segmented into some number of groupings of hour-long
    recordings of packet data. The tool is general purpose, fast and scalable (unlike mergecap), but it is also especially
    designed to efficiently handle large numbers of input files, some of which will not be needed until late in the playback.
  - Extensible decompression infrastructure. Currently supports Gzip and Zstd, but additional formats could likely be added
    very quickly and simply (reach out!).
