use assert_cmd::prelude::*;
use criterion::Criterion;
use std::io::prelude::*;

#[derive(Copy, Clone)]
enum CompressionFormat {
    Uncompressed,
    Zstd, /* deaults: frame_size : 128Kb, compression_level : -12 */
    Gzip,
}

impl std::fmt::Display for CompressionFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let text = match *self {
            CompressionFormat::Uncompressed => "Uncompressed",
            CompressionFormat::Gzip => "Gzip",
            CompressionFormat::Zstd => "Zstd",
        };
        write!(f, "{}", text)
    }
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

struct Corpus(Vec<std::path::PathBuf>);

impl Corpus {
    fn new(config: &CorpusConfiguration) -> Corpus {
        const GB: usize = 1024 * 1024 * 1024;
        let mut paths = Vec::with_capacity(config.n_files as usize);
        for i in 1..=config.n_files {
            let (mut file, path) = match config.storage_location {
                StorageLocation::Local { directory } => {
                    let file_path =
                        directory.join(std::format!("file_{}_of_{}.pcap", i, config.n_files));
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
            while n_bytes_written < ((config.total_size_gb * GB) / config.n_files as usize) {
                n_bytes_written += write_fake_packet(&mut file); /* TODO: pick a random step for the packet timestamp? */
            }

            drop(file); // flush

            // compress
            let path = match config.compression_format {
                CompressionFormat::Uncompressed => {
                    // nothing to do
                    path
                }
                CompressionFormat::Gzip => {
                    // replace w/ gzip-compressed .pcap.gz
                    std::process::Command::new("gzip")
                        .arg(&path)
                        .status()
                        .expect("failed to gzip compress pcap");
                    path.with_extension("pcap.gz")
                }
                CompressionFormat::Zstd => {
                    // TODO: compress w/ zstd
                    path.with_extension("pcap.zst")
                }
            };
            paths.push(path);
        }
        Corpus(paths)
    }
}

// TODO: consider letting users control the packet size?
// TODO better description: create a benchmark corpus locally or at s3, with the desired compression format, pcap sizes, etc..
struct CorpusConfiguration<'dir> {
    total_size_gb: usize,
    n_files: u16,
    storage_location: StorageLocation<'dir>,
    compression_format: CompressionFormat,
}

pub fn stream_and_decompress_throughput(c: &mut Criterion) {
    const TEST_BINARIES: [&str; 2] = ["merge_pcaps", "mergecap"];
    let mut group = c.benchmark_group("Merged Pcap Output Throughput");
    const GB: usize = 1024 * 1024 * 1024;
    // TODO: loop over different n_files and corpus size options
    for total_corpus_size_gb in &[1, 2] {
        for n_files in &[1, 2, 8 /*1, 2, 4, 8, 16, 32*/] {
            for n_runtime_threads in &[1, 2, 7 /*, 5, 6*/] {
                for compression_format in &[CompressionFormat::Gzip] {
                    /*  TODO is the tmp dir automatically deleted at the end of the loop iteration (i.e. on drop)?
                    I think so, which means the corpus gets regenerated and discarded with each benchmark. Not a big deal though */
                    let tmp_dir = tempfile::Builder::new()
                        .prefix("pcap_benchmark_corpus")
                        .tempdir()
                        .unwrap();

                    // generate a corpus matching the benchmark parameters, then run the entire merge operation to /dev/null
                    let corpus_config = CorpusConfiguration {
                        total_size_gb: *total_corpus_size_gb,
                        n_files: *n_files,
                        storage_location: StorageLocation::Local {
                            directory: tmp_dir.path(),
                        },
                        compression_format: *compression_format,
                    };

                    let corpus = Corpus::new(&corpus_config);

                    /* TODO: create and interact w/ the corpus via the Corpus type. Might feature commands like compress(Zstd, -12) or copy_to_s3 or something */

                    group.throughput(criterion::Throughput::Bytes(
                        (total_corpus_size_gb * GB) as u64,
                    ));
                    group.sample_size(10);
                    // group.sampling_mode(criterion::SamplingMode::Flat); /* use flat sampling mode due to long test runtime */
                    let test_binaries =
                        std::array::IntoIter::new(TEST_BINARIES).filter(|binary| match *binary {
                            "merge_pcaps" => true,
                            "mergecap" => {
                                // note: mergecap is single-threaded and am only comparing gzip compression performance (i.e. how I use the tool today)
                                if *n_runtime_threads > 1 {
                                    false
                                } else {
                                    match *compression_format {
                                        CompressionFormat::Uncompressed
                                        | CompressionFormat::Gzip => true,
                                        _ => false,
                                    }
                                }
                            }
                            _ => std::unreachable!(), // all binaries should be accounted for. consider using a different approach for this matching. maybe an enum?
                        });
                    // TODO: consider also varying tests over aws instance types??

                    for binary in test_binaries {
                        // TODO: describe output format somewhere. Uncompressed {} GB Output/Split Across {} Files/S3/
                        group.bench_with_input(
                            criterion::BenchmarkId::new(
                                std::format!(
                                    "{} GB/{} {}/{} {}/{}",
                                    *total_corpus_size_gb,
                                    *n_files,
                                    if *n_files > 1 { "Files" } else { "File" },
                                    *n_runtime_threads,
                                    if *n_runtime_threads > 1 {
                                        "Threads"
                                    } else {
                                        "Thread"
                                    },
                                    *compression_format
                                ),
                                binary,
                            ),
                            binary,
                            |b, binary| {
                                b.iter(|| match binary {
                                    "merge_pcaps" => {
                                        let mut cmd =
                                            std::process::Command::cargo_bin(binary).unwrap();
                                        cmd.env("SMOL_THREADS", n_runtime_threads.to_string()); // TODO: experiment with getting rid of the smol runtime and just using tokio. At least re-do perf experiments now that I understand tokio vs smol better.
                                        cmd.stdout(std::process::Stdio::null());
                                        cmd.stderr(std::process::Stdio::inherit());
                                        cmd.args(corpus.0.iter());
                                        cmd.assert().success();
                                    }
                                    "mergecap" => {
                                        let mut cmd = std::process::Command::new("mergecap");
                                        cmd.stdout(std::process::Stdio::null());
                                        cmd.stderr(std::process::Stdio::inherit());
                                        cmd.arg("-F").arg("nseclibpcap").arg("-w").arg("-");
                                        cmd.args(corpus.0.iter());
                                        cmd.assert().success();
                                    }
                                    _ => {}
                                });
                            },
                        );
                    }
                }
            }
        }
    }
}
