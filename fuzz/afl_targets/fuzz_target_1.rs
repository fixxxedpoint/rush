#[macro_use]
extern crate afl;

use arbitrary::{Arbitrary, Unstructured};
use codec::{Decode, Encode};
use log::error;
use rush::testing::fuzz::fuzz;
use rush::testing::mock::{NetworkData, NetworkDataEncoderDecoder};
use std::fmt;
use std::fmt::Debug;
use std::io::{Read, Result as IOResult};

#[derive(Encode, Decode)]
struct StoredNetworkData(NetworkData);

impl fmt::Debug for StoredNetworkData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("not yet implemented")
    }
}

struct IteratorToRead<I: Iterator<Item = u8>>(I);

impl<I: Iterator<Item = u8>> IteratorToRead<I> {
    fn new(iter: I) -> Self {
        IteratorToRead(iter)
    }
}

impl<I> Read for IteratorToRead<I>
where
    I: Iterator<Item = u8>,
{
    fn read(&mut self, buf: &mut [u8]) -> IOResult<usize> {
        Ok(buf.iter_mut().zip(&mut self.0).fold(0, |count, (b, v)| {
            *b = v;
            count + 1
        }))
    }
}

struct UnstructuredReadWrapper<'a, E: Debug, U: Unstructured<Error = E> + ?Sized> {
    wrapped: &'a mut U,
}

impl<'a, E: Debug, U: Unstructured<Error = E> + ?Sized> Read for UnstructuredReadWrapper<'a, E, U> {
    fn read(&mut self, buf: &mut [u8]) -> IOResult<usize> {
        self.wrapped
            .fill_buffer(buf)
            .expect("unable to fill buffer");
        Ok(buf.len())
    }
}

impl Arbitrary for StoredNetworkData {
    fn arbitrary<U: Unstructured + ?Sized>(u: &mut U) -> Result<Self, U::Error> {
        let decoder = NetworkDataEncoderDecoder::new();
        let all_data = UnstructuredReadWrapper { wrapped: u };
        let data = decoder.decode_from(&mut all_data);
        match data {
            Ok(v) => {
                // panic!("1");
                Ok(StoredNetworkData(v))
            }
            Err(_) => {
                panic!("2");
                // Err(Error::IncorrectFormat)
            }
        }
    }
}

#[derive(Debug)]
struct VecOfStoredNetworkData(Vec<StoredNetworkData>);

impl Arbitrary for VecOfStoredNetworkData {
    fn arbitrary<U: Unstructured + ?Sized>(u: &mut U) -> Result<Self, U::Error> {
        let mut result = vec![];
        for nd in u.arbitrary_iter::<StoredNetworkData>()? {
            match nd {
                Err(_) => {
                    error!(target: "Arbitrary for VecOfStoredNetworkData", "Error while generating an instance of the StoredNetworkData type.");
                    continue;
                }
                Ok(v) => result.push(v),
            }
        }
        Ok(VecOfStoredNetworkData(result))
    }
}

fn main() {
    fuzz!(|data: VecOfStoredNetworkData| {
        use tokio::runtime::Runtime;
        let remapped = data.0.into_iter().map(|v| v.0);
        Runtime::new()
            .unwrap()
            .block_on(fuzz::fuzz(remapped, 4, 30));
    });
}
