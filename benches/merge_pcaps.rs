use assert_cmd::prelude::*;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::io::prelude::*;

#[derive(Copy, Clone)]
enum CompressionFormat {
    Uncompressed,
    Zstd, /* deaults: frame_size : 128Kb, compression_level : -12 */
    Gzip,
}

enum StorageLocation<'tmpdir> {
    Local { directory: &'tmpdir std::path::Path },
}

unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    ::std::slice::from_raw_parts((p as *const T) as *const u8, ::std::mem::size_of::<T>())
}

#[repr(C)]
pub struct PacketHeader {
    pub seconds: u32,
    pub nanoseconds: u32,
    pub caplen: u32,
    pub len: u32,
}

fn write_fake_packet<W: std::io::Write>(mut file: W) -> usize {
    // all bytes are 7 for now. TODO: randomize these bytes for more realistic compresssion
    let packet_bytes = [7u8; 153];

    // TODO: somehow use the property testing or mocking framework for choosing the "time step" between packets that is >= 0
    let header = PacketHeader {
        seconds: 11 as u32,
        nanoseconds: 99 as u32,
        caplen: packet_bytes.len() as u32,
        len: packet_bytes.len() as u32,
    };
    unsafe { file.write_all(any_as_u8_slice(&header)).unwrap() };
    file.write_all(&packet_bytes).unwrap();

    std::mem::size_of::<PacketHeader>() + packet_bytes.len()
}

fn create_corpus(
    total_uncompressed_size: usize, // in bytes
    n_files: u64,
    n_runtime_threads: u64,
    compression_format: CompressionFormat,
    storage_location: StorageLocation,
    /* TODO: consider letting users control the packet size */
) -> Vec<std::path::PathBuf> {
    let mut paths = Vec::with_capacity(n_files as usize);
    for i in 1..=n_files {
        let (mut file, mut path) = match storage_location {
            StorageLocation::Local { directory } => {
                let file_path = directory.join(std::format!("file_{}_of_{}.pcap", i, n_files));
                (
                    std::io::BufWriter::new(std::fs::File::create(&file_path).unwrap()),
                    file_path,
                )
            }
        };

        const PCAP_HDR_NSEC: &[u8] = &hex_literal::hex!(
            "4D 3C B2 A1 02 00 04 00 00 00 00 00 00 00 00 00
    00 00 04 00 01 00 00 00"
        );
        file.write_all(PCAP_HDR_NSEC).unwrap();

        let mut n_bytes_written = PCAP_HDR_NSEC.len();
        while n_bytes_written < total_uncompressed_size / n_files as usize {
            n_bytes_written += write_fake_packet(&mut file);
        }

        drop(file); // flush

        // compress
        match compression_format {
            CompressionFormat::Uncompressed => {
                // nothing to do
            }
            CompressionFormat::Gzip => {
                // replace w/ gzip-compressed .pcap.gz
                std::process::Command::new("gzip")
                    .arg(&path)
                    .status()
                    .expect("failed to gzip compress pcap");
                path = path.with_extension("pcap.gz");
            }
            CompressionFormat::Zstd => {
                // TODO: compress w/ zstd
            }
        }
        paths.push(path);
    }
    paths
}

pub fn stream_and_decompress_throughput(c: &mut Criterion) {
    for compression_format in &[CompressionFormat::Gzip] {
        // TODO: generate a corpus, then run the entire merge operation to /dev/null
        let tmp_dir = tempfile::Builder::new()
            .prefix("pcap_benchmark_corpus")
            .tempdir()
            .unwrap();

        const GB: usize = 1024 * 1024 * 1024;
        const TOTAL_CORPUS_SIZE_GB: usize = 2;
        let corpus = create_corpus(
            TOTAL_CORPUS_SIZE_GB * GB,
            20,
            3,
            *compression_format,
            StorageLocation::Local {
                directory: tmp_dir.path(),
            },
        );

        // TODO: loop over different num_files and corpus size options
        let mut group = c.benchmark_group(std::format!(
            "Merge 60-file {}GB Corpus",
            TOTAL_CORPUS_SIZE_GB
        ));
        group.throughput(criterion::Throughput::Bytes(
            (TOTAL_CORPUS_SIZE_GB * GB) as u64,
        ));
        group.sample_size(10);
        // group.sampling_mode(criterion::SamplingMode::Flat); /* use flat sampling mode due to long test runtime */
        for binary in &["merge_pcaps", "mergecap"] {
            group.bench_with_input(
                criterion::BenchmarkId::from_parameter(binary),
                binary,
                |b, binary| {
                    b.iter(|| match *binary {
                        "merge_pcaps" => {
                            let mut cmd = std::process::Command::cargo_bin(binary).unwrap();
                            cmd.stdout(std::process::Stdio::null());
                            cmd.stderr(std::process::Stdio::inherit());
                            cmd.args(corpus.iter());
                            cmd.assert().success();
                        }
                        "mergecap" => {
                            let mut cmd = std::process::Command::new("mergecap");
                            cmd.stdout(std::process::Stdio::null());
                            cmd.stderr(std::process::Stdio::inherit());
                            cmd.arg("-F").arg("nseclibpcap").arg("-w").arg("-");
                            cmd.args(corpus.iter());
                            cmd.assert().success();
                        }
                        _ => {}
                    });
                },
            );
        }
    }
}
