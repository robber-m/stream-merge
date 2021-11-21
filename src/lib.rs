pub mod pcap;
pub mod s3;
pub mod tournament_tree;
mod util;

use async_channel::bounded;
use async_compression::futures::bufread::{GzipDecoder, ZstdDecoder};
use bytes::Bytes;
use futures::io::AsyncRead;
use futures::stream::{StreamExt, TryStreamExt};
use tracing::{Instrument, Level};
use util::TakeThenBuffered;

fn download_s3_object_chunks_in_parallel(
    path: &str,
) -> impl futures::AsyncBufRead + std::marker::Unpin {
    // 128kb * 4_simultaneous_downloads == 128kB * 4000 files == 512MB for one chunk per-file. If decompression rate requires > 1 parallel chunk to achieve goal throughput,
    // let's say 4 to start are needed, our expected max memory footprint will be 512MB + (512MB/12) * 4 == 512MB + 170.6666MB == 682.6666MB when 1/12th of the files are "active" at a time.
    let object_chunks = s3::ObjectChunks::new(path, 1024 * 128).unwrap().boxed(); /* TODO: proper error on failure */
    // TODO: confirm that s3 object downloads actually happen in parallel. If they're just concurrent, might need to add a spawn() somewhere?
    let parallel_downloader = TakeThenBuffered::new(object_chunks, 1, 4);
    parallel_downloader.into_async_read()
}

#[tracing::instrument]
pub fn stream_and_decode_pcap_packets(
    path: String,
) -> impl futures::stream::Stream<Item = (u64, Bytes)> {
    // Load the file with the provided path from S3 or the local file system based on the presence or absence of s3:// at the beginning
    // of the file name. Wrap the file loader (which implements AsyncRead) in a ZstdDecoder or GzipDecoder if path ends with .zst or .gz.
    // Continually batch all available packets into a vector using ready_chunks(), then forward them to the receiver in the 1-deep async
    // channel created below. NOTE: the purpose of the 1-deep channel is to allow for parallelism and cross-thread communication between
    // the thread/task which decompresses the file and parses out a stream of packets with the thread/task responsible for merging the packets
    // together. Because there is only one merging thread, it is critical for throughput that our design allows for parallel merging w/r/t file decompression.
    // NOTE: by using a one-deep channel holding all ready chunks associated w/ the stream, we guarantee that no further downloading,
    // decompression, or file reading will occur until the first packet has been processed for the file and the next has been requested.
    let (sender, receiver) = bounded(1);

    smol::spawn(async move {
        async fn decode_pcap_packets_to_channel<T: AsyncRead + std::marker::Unpin>(
            reader: T,
            channel: async_channel::Sender<Vec<(u64, Bytes)>>,
        ) {
            /* TODO: create a "tracing" span tree associated with this file. would be cool to see this tree build all the way up to the merge function in the single-threaded case */
            // TODO: is this better than stream.forward()?
            let mut packet_stream = crate::pcap::Packets::new(1024 * 64, reader)
                .await
                .unwrap() /* TODO: nice error indicating what the issue is and bail */
                .map(std::result::Result::unwrap)
                .ready_chunks(2048); // batch as many packets as are available (up to 2048) into a single vector.. Maybe use a 1 or 2 deep channel somehow? while next_packet = ready!(packet_stream.next()) { packets.push(next_packet) }; send(ready_packets).. maybe could collect_ready()
                // TODO: validate the choice of 2048 here or choose a number which makes more sense? is a smaller number like 1024 any better?? trade off between parallelism and memory usage
            while let Some(packets) = packet_stream.next().instrument(tracing::trace_span!("NextPacket")).await {
                tracing::event!(Level::TRACE, ts = packets[0].0);
                channel.send(packets).await.unwrap();
            }
            channel.close();
        }

        // TODO: ask the rust user's forum for ideas about how to remove redundancy and simplify this code
        //       perhaps implement a .decompressed() function  on an enum type to return a decompressed stream?
        if path.starts_with("s3://") {
            let s3_object_stream = download_s3_object_chunks_in_parallel(&path);
            if path.ends_with(".zst") {
                // TODO: consider implementing some sort of from() function for the enum to unify this code?
                decode_pcap_packets_to_channel(ZstdDecoder::new(s3_object_stream), sender).await
            } else if path.ends_with(".gz") {
                decode_pcap_packets_to_channel(GzipDecoder::new(s3_object_stream), sender).await
            } else /* if path.ends_with(".pcap") */ {
                // uncompressed
                decode_pcap_packets_to_channel(s3_object_stream, sender).await
            }
        } else { // local file loader. TODO: consider switching to use io_uring w/ Tokio for this?
            // TODO: proper error handling if file doesn't exist
            let file = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
            let loader = smol::io::BufReader::with_capacity( 1024 * 128, smol::Unblock::with_capacity(1024 * 128, file));
            if path.ends_with(".zst") {
                decode_pcap_packets_to_channel(ZstdDecoder::new(loader), sender).await;
            } else if path.ends_with(".gz") {
                decode_pcap_packets_to_channel(GzipDecoder::new(loader), sender).await;
            } else /* if path.ends_with(".pcap") */ {
                // uncompressed
                decode_pcap_packets_to_channel(loader, sender).await;
            }
        }
    })
    .detach();

    receiver.map(futures::stream::iter).flatten() // hide the vector-batching we used to minimize atomic operations w/ inter-thread communication
}
