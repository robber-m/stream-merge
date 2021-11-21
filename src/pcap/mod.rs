//! Functionality related to the .pcap file format
//!
//! Asynchronously parse uncompressed pcap bytes as a `futures::stream::Stream<Item=(u64, Bytes)>` of `(timestamp, packet)` tuples.
//!

use anyhow::Result;
use bytes::buf::BufMut;
use bytes::{Bytes, BytesMut};
use futures::io::{AsyncRead, AsyncReadExt};
use futures::stream::Stream;
use futures::task::Poll;
use nom::{self, IResult};
use pcap_parser::pcap::{
    parse_pcap_frame, parse_pcap_frame_be, parse_pcap_header, LegacyPcapBlock,
};
use pcap_parser::PcapError;
use std::pin::Pin;
use std::task::Context;

#[pin_project::pin_project(project = PacketsProj)]
/// [AsyncRead] combinator type for parsing pcap files into a [Stream] of timestamped [Bytes] for each packet present in the file.
///
/// Wrapped types implementing [AsyncRead] are expected to yield uncompressed data in .pcap form with packet timestamps which never decrease (i.e. the file is already time-ordered). If the file is detected to be unordered or corrupt,
/// an error will be returned and TODO: define and test error return behavior for corrupt or unordered files.
pub struct Packets<R> {
    ts_usec_multiplier: u16, // we will always emit nanosecond-precision values. ts_usec_multiplier will
    // be 1000 for all microsecond-precision pcap files
    #[pin]
    reader: R,
    buffer: BytesMut,
    reader_exhausted: bool,
    parse: LegacyParseFn,
}

type LegacyParseFn = fn(&[u8]) -> IResult<&[u8], LegacyPcapBlock, PcapError>;

impl<R> Packets<R>
where
    R: AsyncRead + std::marker::Unpin,
{
    /// Given an internal buffer `capacity` and an [AsyncRead] reader which yields bytes in uncompressed .pcap format, validate
    /// the pcap file header and, on success, construct a [`Packets<R>`].
    pub async fn new(capacity: usize, mut reader: R) -> Result<Packets<R>, PcapError> {
        let mut header_bytes = [0; 24];
        let mut n_header_bytes_read = 0;
        let is_bigendian: bool;
        let ts_usec_multiplier;
        loop {
            n_header_bytes_read += reader
                .read(&mut header_bytes[n_header_bytes_read..])
                .await
                .or(Err(PcapError::ReadError))?;
            // TODO: handle getting less data than a pcap header??
            let (_, header) = match parse_pcap_header(&header_bytes) {
                Ok((r, h)) => Ok((r, h)),
                Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => Err(e),
                Err(_) => continue, //incomplete. TODO: bail if we continue to fail our read
            }?;
            ts_usec_multiplier = if header.is_nanosecond_precision() {
                1
            } else {
                1000
            };
            is_bigendian = header.is_bigendian();
            break;
        }
        let parse = if is_bigendian {
            parse_pcap_frame_be
        } else {
            parse_pcap_frame
        };
        Ok(Packets {
            ts_usec_multiplier,
            reader,
            buffer: BytesMut::with_capacity(capacity),
            reader_exhausted: false,
            parse,
        })
    }
}

impl<R: AsyncRead> Stream for Packets<R>
where
    R: AsyncRead,
{
    type Item = Result<(u64, Bytes), nom::Err<PcapError>>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.buffer.is_empty() && self.reader_exhausted {
            return Poll::Ready(None); // EOF
        }

        loop {
            match (self.as_mut().parse)(&self.buffer) {
                Ok((rem, packet)) => {
                    // TODO: write the nanosecond timestamp into the data??
                    let nanosecond_ts = packet.ts_sec as u64 * 1000000000
                        + packet.ts_usec as u64 * self.ts_usec_multiplier as u64;
                    let packet_n_bytes = self.buffer.len() - rem.len();
                    return Poll::Ready(Some(Ok((
                        nanosecond_ts,
                        self.as_mut()
                            .project()
                            .buffer
                            .split_to(packet_n_bytes)
                            .freeze(),
                    ))));
                }
                Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
                    return Poll::Ready(Some(Err(nom::Err::Error(e))))
                }
                Err(_) => {
                    // incomplete. get some more data from our underlying reader
                    let PacketsProj {
                        ts_usec_multiplier: _,
                        reader,
                        buffer,
                        reader_exhausted: _,
                        parse: _,
                    } = self.as_mut().project();

                    let to_read = unsafe {
                        &mut *(buffer.bytes_mut() as *mut [std::mem::MaybeUninit<u8>]
                            as *mut [u8])
                    };
                    if let Poll::Ready(Ok(n_bytes_read)) = reader.poll_read(cx, to_read) {
                        // got more data! loop around to see whether we now have a complete packet
                        if n_bytes_read == 0 {
                            return Poll::Ready(None);
                        }
                        unsafe {
                            self.as_mut().project().buffer.advance_mut(n_bytes_read);
                        }
                    } else {
                        return Poll::Pending; // our poll_read call will have scheduled our next wakeup for us
                    }
                }
            }
        }
    }
}
