use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Result as IOResult, Write},
    path::Path,
    time::Duration,
};

use codec::{Compact, Decode, Encode, IoReader};
use futures::{channel::oneshot, StreamExt};
use futures_timer::Delay;
use log::info;
use tokio::runtime::Runtime;

use crate::{
    nodes::NodeIndex,
    testing::mock::{
        spawn_honest_member, Data, Hasher64, NetworkHook, PartialMultisignature, Signature,
    },
    utils::after_iter,
    Network, NetworkData as ND, SpawnHandle,
};

pub use crate::testing::mock::NetworkData;

use crate::testing::mock::{configure_network, Spawner};

struct FilteringHook<IH, F> {
    wrapped: IH,
    filter: F,
}

impl<IH, F: FnMut(&NetworkData, &NodeIndex, &NodeIndex) -> bool + Send> FilteringHook<IH, F> {
    pub(crate) fn new(wrapped: IH, filter: F) -> Self {
        FilteringHook { wrapped, filter }
    }
}

fn new_for_peer_id<IH>(
    wrapped: IH,
    peer_id: NodeIndex,
) -> FilteringHook<IH, impl FnMut(&NetworkData, &NodeIndex, &NodeIndex) -> bool + Send> {
    FilteringHook::new(wrapped, move |_, _, receiver| *receiver == peer_id)
}

impl<IH: NetworkHook, F: FnMut(&NetworkData, &NodeIndex, &NodeIndex) -> bool + Send> NetworkHook
    for FilteringHook<IH, F>
{
    fn update_state(&mut self, data: &mut NetworkData, sender: NodeIndex, recipient: NodeIndex) {
        if (self.filter)(&data, &sender, &recipient) {
            self.wrapped.update_state(data, sender, recipient);
        }
    }
}

struct EvesdroppingHook<S>
where
    S: Write,
{
    sink: S,
    encoder: NetworkDataEncoding,
}

impl<W: Write> EvesdroppingHook<W> {
    pub(crate) fn new(writer: W) -> EvesdroppingHook<W> {
        EvesdroppingHook {
            sink: writer,
            encoder: NetworkDataEncoding::new(),
        }
    }
}

impl<S: Write + Send> NetworkHook for EvesdroppingHook<S> {
    fn update_state(&mut self, data: &mut NetworkData, _: NodeIndex, _: NodeIndex) {
        self.encoder.encode_into(data, &mut self.sink).unwrap();
    }
}

pub struct NetworkDataEncoding {}

impl NetworkDataEncoding {
    pub fn new() -> Self {
        NetworkDataEncoding {}
    }

    pub fn encode_into<W: Write>(&self, data: &NetworkData, writer: &mut W) -> IOResult<()> {
        writer.write_all(&data.encode()[..])
    }

    pub fn decode_from<R: Read>(
        &self,
        reader: &mut R,
    ) -> core::result::Result<NetworkData, codec::Error> {
        let mut reader = IoReader(reader);
        return <NetworkData>::decode(&mut reader);
    }
}

struct NetworkDataIterator<R> {
    input: R,
    encoding: NetworkDataEncoding,
}

impl<R: Read> NetworkDataIterator<R> {
    fn new(read: R) -> Self {
        NetworkDataIterator {
            input: read,
            encoding: NetworkDataEncoding::new(),
        }
    }
}

impl<R: Read> Iterator for NetworkDataIterator<R> {
    type Item = NetworkData;

    fn next(&mut self) -> Option<Self::Item> {
        match self.encoding.decode_from(&mut self.input) {
            Ok(v) => Some(v),
            Err(_) => None,
        }
    }
}

struct RecorderNetwork<I: Iterator<Item = NetworkData>> {
    data: I,
    delay: Duration,
    next_delay: Delay,
}

impl<I: Iterator<Item = NetworkData> + Send> RecorderNetwork<I> {
    fn new(data: I, delay_millis: u64) -> Self {
        RecorderNetwork {
            data,
            delay: Duration::from_millis(delay_millis),
            next_delay: Delay::new(Duration::from_millis(0)),
        }
    }
}

#[async_trait::async_trait]
impl<I: Iterator<Item = NetworkData> + Send>
    Network<Hasher64, Data, Signature, PartialMultisignature> for RecorderNetwork<I>
{
    type Error = ();

    fn send(
        &self,
        _: ND<Hasher64, Data, Signature, PartialMultisignature>,
        _: NodeIndex,
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    fn broadcast(
        &self,
        _: ND<Hasher64, Data, Signature, PartialMultisignature>,
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    async fn next_event(&mut self) -> Option<ND<Hasher64, Data, Signature, PartialMultisignature>> {
        (&mut self.next_delay).await;
        self.next_delay.reset(self.delay);
        self.data.next()
    }
}

fn test_encode_decode() {
    let test: Vec<u8> = vec![1, 2, 3, 4, 5];
    let mut encoded = test.encode();
    // let fake_size: u32 = u32::max_value() / 8;
    let fake_size = Compact::<u32>(u32::max_value());
    let fake_size = fake_size.encode();
    for (ix, v) in fake_size.iter().enumerate() {
        encoded[ix] = *v;
    }
    let mut reader = IoReader(&encoded[..]);
    let result = <Vec<u8>>::decode(&mut reader);
    println!("result: {:?}", result);
    panic!("abc");
}

async fn load_fuzz(path: &Path, n_members: usize, n_batches: usize) {
    let reader = BufReader::new(File::open(path).expect("unable to open a corpus file"));
    let data_iter = NetworkDataIterator::new(reader).collect();
    fuzz_async(data_iter, n_members, n_batches).await;
}

async fn generate_fuzz_async<W: Write + Send + 'static>(
    output: W,
    n_members: usize,
    n_batches: usize,
) {
    let spawner = Spawner::new();
    let file = BufWriter::new(output);
    let peer_id = NodeIndex(0);
    let network_hook = EvesdroppingHook::new(file);
    let (mut router, mut networks) = configure_network(n_members, 1.0);
    let filtering_hook = new_for_peer_id(network_hook, peer_id);
    router.add_hook(filtering_hook);

    spawner.spawn("network", async move { router.run().await });

    let mut batch_rxs = Vec::new();
    let mut exits = Vec::new();
    for (ix, network) in networks.drain(..).enumerate() {
        let (batch_rx, exit_tx) = spawn_honest_member(
            spawner.clone(),
            ix,
            n_members,
            network.expect("unable to retrieve a network handle"),
        );
        exits.push(exit_tx);
        batch_rxs.push(batch_rx);
    }

    for mut rx in batch_rxs.drain(..) {
        for _ in 0..n_batches {
            rx.next().await.expect("unable to retrieve a batch");
        }
    }
    for e in exits.drain(..) {
        let _ = e.send(());
    }
    spawner.wait().await;
}

pub fn generate_fuzz<W: Write + Send + 'static>(output: W, n_members: usize, n_batches: usize) {
    Runtime::new()
        .unwrap()
        .block_on(generate_fuzz_async(output, n_members, n_batches));
}

pub fn fuzz(data: Vec<NetworkData>, n_members: usize, n_batches: usize) {
    Runtime::new()
        .unwrap()
        .block_on(fuzz_async(data, n_members, n_batches));
}

async fn fuzz_async(data: Vec<NetworkData>, n_members: usize, n_batches: usize) {
    const NETWORK_DELAY: u64 = 200;
    let spawner = Spawner::new();
    let (empty_tx, empty_rx) = oneshot::channel();
    let data = after_iter(data.into_iter(), move || {
        empty_tx.send(()).expect("empty_rx was already closed");
    });

    let network = RecorderNetwork::new(data, NETWORK_DELAY);

    let (mut batch_rx, exit_tx) = spawn_honest_member(spawner.clone(), 0, n_members, network);

    empty_rx.await.expect("empty_tx was unexpectedly dropped");

    let mut batches_count = 0;
    loop {
        match batch_rx.next().await {
            Some(_) => {
                batches_count += 1;
            }
            None => break,
        }
    }
    if n_batches != batches_count {
        info!(target: "rush", "Expected {:?} batches, received {:?}", n_batches, batches_count);
    }

    exit_tx
        .send(())
        .unwrap_or(info!("exit_rx is already closed"));

    spawner.wait().await;
}

#[tokio::test]
#[ignore]
async fn generate_fuzz_corpus() {
    // test_encode_decode();
    let fuzz_output = Path::new("fuzz.corpus");
    let output = File::create(fuzz_output).expect("ubable to create a corpus file");
    generate_fuzz_async(output, 4, 30).await
}

#[tokio::test]
#[ignore]
async fn load_fuzz_corpus() {
    let fuzz_input = Path::new("fuzz")
        .join("corpus")
        .join("fuzz_target_1")
        .join("seed");
    load_fuzz(fuzz_input.as_ref(), 4, 30).await
}
