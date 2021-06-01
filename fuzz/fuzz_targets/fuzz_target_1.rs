#![no_main]
use libfuzzer_sys::arbitrary::{Arbitrary, Result, Unstructured};
use libfuzzer_sys::fuzz_target;
use rush::testing::StoredNetworkData;

struct ArbitraryNetworkData {
    data: StoredNetworkData,
}

fuzz_target!(|data: Vec<ArbitraryNetworkData>| {
    // fuzzed code goes here
});
