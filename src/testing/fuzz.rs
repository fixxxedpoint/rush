use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Result as IOResult, Write},
    path::Path,
    time::Duration,
};

use codec::{Decode, Encode, IoReader};
use futures::{
    channel::oneshot::{self, Receiver},
    StreamExt,
};
use futures_timer::Delay;
use log::{error, info};
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
            Ok(v) => {
                println!("decoded");
                return Some(v);
            }
            Err(e) => {
                error!("{:?}", e);
                println!("{:?}", e);
                return None;
            }
        }
    }
}

struct RecorderNetwork<I: Iterator<Item = NetworkData>> {
    data: I,
    delay: Duration,
    next_delay: Delay,
    exit: Receiver<()>,
}

impl<I: Iterator<Item = NetworkData> + Send> RecorderNetwork<I> {
    fn new(data: I, delay_millis: u64, exit: Receiver<()>) -> Self {
        RecorderNetwork {
            data,
            delay: Duration::from_millis(delay_millis),
            next_delay: Delay::new(Duration::from_millis(0)),
            exit,
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
        match self.data.next() {
            Some(v) => Some(v),
            None => {
                let _ = (&mut self.exit).await;
                None
            }
        }
    }
}

async fn load_fuzz(path: &Path, n_members: usize, n_batches: usize) {
    let reader = BufReader::new(File::open(path).expect("unable to open a corpus file"));
    let data_iter = NetworkDataIterator::new(reader);
    execute_fuzz(data_iter, n_members, n_batches).await;
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
    // spawn only byzantine threshold of nodes and networks so all needs to communicate to finish each round
    let threshold = (n_members * 2) / 3 + 1;
    let (mut router, mut networks) =
        configure_network(n_members, 1.0, (0..threshold).map(NodeIndex));
    let filtering_hook = new_for_peer_id(network_hook, peer_id);
    router.add_hook(filtering_hook);

    spawner.spawn("network", async move { router.run().await });
    // spawner.spawn("network", router);

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

pub fn fuzz<I: Iterator<Item = NetworkData> + Send + 'static>(
    data: I,
    n_members: usize,
    n_batches: usize,
) {
    Runtime::new()
        .unwrap()
        .block_on(execute_fuzz(data, n_members, n_batches));
}

async fn execute_fuzz<I: Iterator<Item = NetworkData> + Send + 'static>(
    data: I,
    n_members: usize,
    n_batches: usize,
) {
    const NETWORK_DELAY: u64 = 50;
    let spawner = Spawner::new();
    let (empty_tx, empty_rx) = oneshot::channel();
    let data = after_iter(data, move || {
        empty_tx.send(()).expect("empty_rx was already closed");
    });

    let (net_exit, net_exit_rx) = oneshot::channel();
    let network = RecorderNetwork::new(data, NETWORK_DELAY, net_exit_rx);

    let (mut batch_rx, exit_tx) = spawn_honest_member(spawner.clone(), 0, n_members, network);

    empty_rx.await.expect("empty_tx was unexpectedly dropped");

    let mut batches_count = 0;
    while batches_count < n_batches {
        match batch_rx.next().await {
            Some(_) => {
                batches_count += 1;
            }
            None => break,
        }
    }
    assert!(
        batches_count >= n_batches,
        "Expected at least {:?} batches, but received {:?}.",
        n_batches,
        batches_count
    );

    exit_tx
        .send(())
        .unwrap_or(info!("exit_rx channel is already closed"));

    net_exit
        .send(())
        .unwrap_or(info!("net_exit channel is already closed"));

    spawner.wait().await;
}

async fn generate_fuzz_corpus(fuzz_output: &Path) {
    let output = File::create(fuzz_output).expect("ubable to create a corpus file");
    generate_fuzz_async(output, 4, 30).await
}

async fn load_fuzz_corpus(fuzz_input: &Path) {
    load_fuzz(fuzz_input.as_ref(), 4, 30).await
}

#[tokio::test]
#[ignore]
async fn fuzz_loop() {
    let fuzz_output = Path::new("fuzz.corpus");
    generate_fuzz_corpus(fuzz_output).await;
    load_fuzz_corpus(fuzz_output).await;
}

pub fn check_fuzz() {
    let fuzz_output = Path::new("fuzz.corpus");

    Runtime::new().unwrap().block_on(async move {
        // generate_fuzz_corpus(fuzz_output).await;
        load_fuzz_corpus(fuzz_output).await;
    });
}
