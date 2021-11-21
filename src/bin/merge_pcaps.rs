use bytes::Bytes;
use stream_merge::tournament_tree;
use std::io::{BufWriter, Write};

use hex_literal::hex;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

use std::path::PathBuf;
use structopt::StructOpt;

struct PacketStream<T: Iterator<Item = (u64, Bytes)>> {
    iterator: std::iter::Peekable<T>,
    current_value: Option<(u64, Bytes)>,
}
impl<T: Iterator<Item = (u64, Bytes)>> PacketStream<T> {
    pub fn new(iterator: T) -> PacketStream<T> {
        PacketStream {
            iterator: iterator.peekable(),
            current_value: None,
        }
    }
}

// TODO: rather than taking a "PacketStream", just take an iterator which returns tuples of
// (Value,Data) as packet to tournament_tree::Tree::new!
impl<T: Iterator<Item = (u64, Bytes)>> tournament_tree::Mergeable for PacketStream<T> {
    type Data = <T>::Item;

    // returns an tuple of (timestamp,data) or None
    fn pop(&mut self) -> Option<&<T>::Item> {
        self.current_value = self.iterator.next();
        self.current_value.as_ref()
    }

    fn peek_timestamp(&mut self) -> u64 {
        //println!("peeking {:?}", self.iterator.peek());
        if let Some((ts, _bytes)) = self.iterator.peek() {
            *ts
        } else {
            std::u64::MAX
        }
    }
}

#[derive(StructOpt)]
#[structopt(
    version = "1.0",
    author = "Bobby McShane <mcshane.bobby@gmail.com>",
    about = "Merge PCAP files [s3:/]/path/to/files*.pcap[.gz|.zst] files together in time-sequence from AWS S3 or a local filesystem"
)]
struct Args {
    /// pcap files to merge
    #[structopt(required = true, min_values = 1, parse(from_os_str))]
    pcaps: Vec<PathBuf>,
}

fn main() {
    // TODO: tracing feature gate?
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        /* TODO: use proper tracing library output formatting for testing:
         * https://docs.rs/tracing-subscriber/0.3.1/tracing_subscriber/fmt/writer/struct.TestWriter.html */
        .with_writer(std::io::stderr)
        .init();

    let packet_streams = Args::from_args()
        .pcaps
        .into_iter()
        .map(|path| {
            PacketStream::new(smol::stream::block_on(
                stream_merge::stream_and_decode_pcap_packets(
                    path.into_os_string().into_string().unwrap(),
                ),
            ))
        })
        .collect();

    {
        // TODO: pull the tournament tree module into the stream-merge crate directly
        let mut merger = tournament_tree::Tree::new(packet_streams);
        let stdout = std::io::stdout();
        // TODO: consider changing the stdout PIPE SIZE to be the max configured for the system
        // then configuring the buffer accordingly
        let mut writer = BufWriter::with_capacity(1024 * 1024 * 2, stdout.lock());
        // pcap header with nanosecond-precision timestamping
        const PCAP_HDR_NSEC: &[u8] = &hex!(
            "4D 3C B2 A1 02 00 04 00 00 00 00 00 00 00 00 00
    00 00 04 00 01 00 00 00"
        );
        writer.write_all(PCAP_HDR_NSEC).unwrap();
        // TODO: should some of these be spans?
        tracing::event!(tracing::Level::TRACE, "Wrote PCAP header");
        while let Some((ts, packet)) = merger.pop() {
            writer.write_all(&packet).unwrap();
            tracing::event!(tracing::Level::TRACE, text = "Wrote packet", ts);
            //coz::progress!("wrote packet");
        }
        writer.flush().unwrap();
        tracing::event!(tracing::Level::TRACE, "Merge complete. No more packets.");
    }
}
