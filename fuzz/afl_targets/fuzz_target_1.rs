#[macro_use]
extern crate afl;

use aleph_bft::testing::fuzz::fuzz;
use aleph_bft::testing::mock::{init_log, NetworkData, NetworkDataEncoderDecoder};
use log::error;
use std::io::{BufReader, Read};
use tokio::runtime::Runtime;

struct ReadToNetworkDataIterator<R> {
    read: BufReader<R>,
    decoder: NetworkDataEncoderDecoder,
}

impl<R: Read> ReadToNetworkDataIterator<R> {
    fn new(read: R) -> Self {
        ReadToNetworkDataIterator {
            read: BufReader::new(read),
            decoder: NetworkDataEncoderDecoder::new(),
        }
    }
}

impl<R: Read> Iterator for ReadToNetworkDataIterator<R> {
    type Item = NetworkData;

    fn next(&mut self) -> Option<Self::Item> {
        use std::io::BufRead;
        if let Ok(buf) = self.read.fill_buf() {
            if buf.is_empty() {
                return None;
            }
        }
        match self.decoder.decode_from(&mut self.read) {
            Ok(v) => Some(v),
            // otherwise try to read until you reach the END-OF-FILE
            Err(e) => {
                error!(target: "fuzz_target_1", "Unable to parse NetworkData: {:?}.", e);
                self.next()
            }
        }
    }
}

fn main() {
    init_log();
    fuzz!(|data: &[u8]| {
        let data = ReadToNetworkDataIterator::new(data).collect();
        Runtime::new().unwrap().block_on(fuzz::fuzz(data, 4, 30));
    });
}
