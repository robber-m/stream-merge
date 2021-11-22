// TODO: generate N identical pcaps and confirm that ordering and content are correct in the output pcap. Maybe could compare to mergecap as a reference implementation?
use assert_cmd::prelude::*;

use std::process::Command;
use tempfile::NamedTempFile;

use hex_literal::hex;
use std::io::prelude::*;

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

#[test]
fn compare_to_mergecap() -> Result<(), Box<dyn std::error::Error>> {
    let input_pcaps: Vec<_> = (0..13u8)
        .map(|i| {
            let mut file = NamedTempFile::new().unwrap();
            const PCAP_HDR_NSEC: &[u8] = &hex!(
                "4D 3C B2 A1 02 00 04 00 00 00 00 00 00 00 00 00
    00 00 04 00 01 00 00 00"
            );
            file.write_all(PCAP_HDR_NSEC).unwrap();
            // TODO: use the property testing framework to decide between nanosecond or microsecond/millisecond? resolution inputs
            // TODO: somehow use the property testing framework for choosing the size/contents of the packet
            for ts in 0..10 * i {
                let packet_bytes = [7u8; 153]; // all bytes are 7 for now
                                               // TODO: somehow use the property testing framework for choosing the "time step" between packets that is >= 0
                let header = PacketHeader {
                    seconds: ts as u32,
                    nanoseconds: ts as u32,
                    caplen: packet_bytes.len() as u32,
                    len: packet_bytes.len() as u32,
                };
                unsafe { file.write_all(any_as_u8_slice(&header)).unwrap() };
                file.write_all(&packet_bytes).unwrap();
            }
            //let (mut file, path) = file.keep().unwrap(); // TODO: delete this call and just return (file, file.path()) in the typical case
            //(file, path)
            file
        })
        .collect();

    let mut mergecap = Command::new("mergecap");
    mergecap
        .arg("-F")
        .arg("nseclibpcap")
        .arg("-w")
        .arg("-")
        .args(input_pcaps.iter().map(|file| file.path()));
    let mergecap_output = mergecap.unwrap();

    let mut merge_pcaps = Command::cargo_bin("merge_pcaps")?;
    merge_pcaps.stderr(std::process::Stdio::inherit());
    merge_pcaps.args(input_pcaps.iter().map(|file| file.path()));
    let merge_pcaps_output = merge_pcaps.unwrap();
    assert_eq!(&mergecap_output.stdout[..], &merge_pcaps_output.stdout[..]);

    Ok(())
}
