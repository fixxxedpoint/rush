use aleph_bft::DataIO as DataIOT;
use aleph_bft::Index;
use aleph_bft::KeyBox as KeyBoxT;
use aleph_bft::MultiKeychain as MultiKeychainT;
use aleph_bft::NetworkData;
use aleph_bft::OrderedBatch;
use aleph_bft::PartialMultisignature as PartialMultisignatureT;
// use aleph_mock::Spawner;
use futures::task::Poll;
use parking_lot::Mutex;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use aleph_bft::{NodeCount, NodeIndex, SpawnHandle};
use aleph_mock::{configure_network, init_log, spawn_honest_member_generic, NetworkHook};

use codec::{Decode, Encode, IoReader};

use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    channel::oneshot::{self, Receiver},
    Future, StreamExt,
};

use futures_timer::Delay;
use log::{error, info};

use std::{
    io::{BufRead, BufReader, Read, Result as IOResult, Write},
    time::Duration,
};

use async_trait::async_trait;
use tokio::runtime::{Builder, Runtime};
use tokio::task::yield_now;

struct SpyingNetworkHook<W: Write> {
    node: NodeIndex,
    encoder: NetworkDataEncoding,
    output: W,
}

impl<W: Write> SpyingNetworkHook<W> {
    fn new(node: NodeIndex, output: W) -> Self {
        SpyingNetworkHook {
            node,
            encoder: NetworkDataEncoding::default(),
            output,
        }
    }
}

impl<W: Write + Send> NetworkHook<aleph_mock::Hasher64, Data, Signature, PartialMultisignature>
    for SpyingNetworkHook<W>
{
    fn update_state(&mut self, data: &mut FuzzNetworkData, _: NodeIndex, recipient: NodeIndex) {
        if self.node == recipient {
            self.encoder.encode_into(data, &mut self.output).unwrap();
        }
    }
}

#[derive(Default)]
pub(crate) struct NetworkDataEncoding {}

impl NetworkDataEncoding {
    pub(crate) fn encode_into<W: Write>(
        &self,
        data: &FuzzNetworkData,
        writer: &mut W,
    ) -> IOResult<()> {
        writer.write_all(&data.encode()[..])
    }

    pub(crate) fn decode_from<R: Read>(
        &self,
        reader: &mut R,
    ) -> core::result::Result<FuzzNetworkData, codec::Error> {
        let mut reader = IoReader(reader);
        <FuzzNetworkData>::decode(&mut reader)
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
            encoding: NetworkDataEncoding::default(),
        }
    }
}

impl<R: Read> Iterator for NetworkDataIterator<R> {
    type Item = FuzzNetworkData;

    fn next(&mut self) -> Option<Self::Item> {
        match self.encoding.decode_from(&mut self.input) {
            Ok(v) => Some(v),
            Err(e) => {
                error!("Error while decoding NetworkData: {:?}.", e);
                None
            }
        }
    }
}

struct PlaybackNetwork<I, C> {
    data: I,
    delay: Duration,
    next_delay: Delay,
    exit: Receiver<()>,
    finished_callback: Option<C>,
}

impl<I, C> PlaybackNetwork<I, C> {
    fn new(data: I, delay_millis: u64, exit: Receiver<()>, finished: C) -> Self {
        PlaybackNetwork {
            data,
            delay: Duration::from_millis(delay_millis),
            next_delay: Delay::new(Duration::from_millis(delay_millis)),
            exit,
            finished_callback: Some(finished),
        }
    }
}

#[async_trait::async_trait]
impl<I: Iterator<Item = FuzzNetworkData> + Send, C: FnOnce() + Send>
    aleph_bft::Network<aleph_mock::Hasher64, Data, Signature, PartialMultisignature>
    for PlaybackNetwork<I, C>
{
    type Error = ();

    fn send(&self, _: FuzzNetworkData, _: NodeIndex) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    fn broadcast(&self, _: FuzzNetworkData) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    async fn next_event(&mut self) -> Option<FuzzNetworkData> {
        (&mut self.next_delay).await;
        self.next_delay.reset(self.delay);
        match self.data.next() {
            Some(v) => Some(v),
            None => {
                if let Some(finished_call) = self.finished_callback.take() {
                    (finished_call)();
                }
                // wait for an exit call when no more data is available
                let _ = (&mut self.exit).await;
                None
            }
        }
    }
}

pub struct ReadToNetworkDataIterator<R> {
    read: BufReader<R>,
    decoder: NetworkDataEncoding,
}

impl<R: Read> ReadToNetworkDataIterator<R> {
    pub fn new(read: R) -> Self {
        ReadToNetworkDataIterator {
            read: BufReader::new(read),
            decoder: NetworkDataEncoding::default(),
        }
    }
}

impl<R: Read> Iterator for ReadToNetworkDataIterator<R> {
    type Item = FuzzNetworkData;

    fn next(&mut self) -> Option<Self::Item> {
        if let Ok(buf) = self.read.fill_buf() {
            if buf.is_empty() {
                return None;
            }
        }
        match self.decoder.decode_from(&mut self.read) {
            Ok(v) => Some(v),
            // otherwise try to read until you reach END-OF-FILE
            Err(e) => {
                error!(target: "fuzz", "Unable to parse NetworkData: {:?}.", e);
                self.next()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Hash)]
pub struct Data {}

impl Data {
    fn new() -> Self {
        Data {}
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct Signature {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct PartialMultisignature {
    signed_by: Vec<NodeIndex>,
}

impl PartialMultisignatureT for PartialMultisignature {
    type Signature = Signature;
    fn add_signature(self, _: &Self::Signature, index: NodeIndex) -> Self {
        let Self { mut signed_by } = self;
        for id in &signed_by {
            if *id == index {
                return Self { signed_by };
            }
        }
        signed_by.push(index);
        Self { signed_by }
    }
}

pub struct DataIO {
    tx: UnboundedSender<OrderedBatch<Data>>,
}

impl DataIOT<self::Data> for DataIO {
    type Error = ();
    fn get_data(&self) -> Data {
        Data::new()
    }

    fn check_availability(
        &self,
        _: &Data,
    ) -> Option<Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>>> {
        None
    }

    fn send_ordered_batch(&mut self, data: OrderedBatch<Data>) -> Result<(), ()> {
        self.tx.unbounded_send(data).map_err(|e| {
            error!(target: "data-io", "Error when sending data from DataIO {:?}.", e);
        })
    }
}

impl DataIO {
    pub fn new() -> (Self, UnboundedReceiver<OrderedBatch<Data>>) {
        let (tx, rx) = unbounded();
        let data_io = DataIO { tx };
        (data_io, rx)
    }
}

#[derive(Clone)]
pub struct KeyBox {
    count: NodeCount,
    ix: NodeIndex,
}

impl KeyBox {
    pub fn new(count: NodeCount, ix: NodeIndex) -> Self {
        KeyBox { count, ix }
    }
}

impl Index for KeyBox {
    fn index(&self) -> NodeIndex {
        self.ix
    }
}

#[async_trait]
impl KeyBoxT for KeyBox {
    type Signature = Signature;

    fn node_count(&self) -> NodeCount {
        self.count
    }

    async fn sign(&self, _msg: &[u8]) -> Signature {
        Signature {}
    }

    fn verify(&self, _msg: &[u8], _sgn: &Signature, _index: NodeIndex) -> bool {
        true
    }
}

impl MultiKeychainT for KeyBox {
    type PartialMultisignature = PartialMultisignature;
    fn from_signature(&self, _: &Self::Signature, index: NodeIndex) -> Self::PartialMultisignature {
        let signed_by = vec![index];
        PartialMultisignature { signed_by }
    }
    fn is_complete(&self, _: &[u8], partial: &Self::PartialMultisignature) -> bool {
        (self.count * 2) / 3 < NodeCount(partial.signed_by.len())
    }
}

async fn execute_generate_fuzz<'a, W: Write + Send + 'static>(
    output: W,
    n_members: usize,
    n_batches: usize,
) {
    let peer_id = NodeIndex(0);
    let spy = SpyingNetworkHook::new(peer_id, output);
    // spawn only byzantine-threshold of nodes and networks so all enabled nodes are required to finish each round
    let threshold = (n_members * 2) / 3 + 1;
    let (mut router, networks) = configure_network(n_members, 1.0);
    router.add_hook(spy);

    let spawner = Spawner::new();
    spawner.spawn("network", router);

    let mut batch_rxs = Vec::new();
    let mut exits = Vec::new();
    for network in networks.into_iter().take(threshold) {
        let (data_io, batch_rx) = DataIO::new();
        let keybox = KeyBox::new(NodeCount(n_members), network.index());
        let exit_tx = spawn_honest_member_generic(
            spawner.clone(),
            network.index(),
            n_members,
            network,
            data_io,
            keybox,
        );
        exits.push(exit_tx);
        batch_rxs.push(batch_rx);
    }

    for mut rx in batch_rxs {
        for _ in 0..n_batches {
            rx.next().await.expect("unable to retrieve a batch");
        }
    }
    for e in exits {
        let _ = e.send(());
    }
    spawner.wait().await;
}

type FuzzNetworkData = NetworkData<aleph_mock::Hasher64, Data, Signature, PartialMultisignature>;

async fn execute_fuzz(
    data: impl Iterator<Item = FuzzNetworkData> + Send + 'static,
    n_members: usize,
    n_batches: Option<usize>,
) {
    const NETWORK_DELAY: u64 = 1;

    let (net_exit, net_exit_rx) = oneshot::channel();
    let (playback_finished_tx, mut playback_finished_rx) = oneshot::channel();
    let finished_callback = move || {
        playback_finished_tx
            .send(())
            .expect("finished_callback channel already closed");
    };
    let network = PlaybackNetwork::new(data, NETWORK_DELAY, net_exit_rx, finished_callback);

    let spawner = Spawner::new();
    let node_index = NodeIndex(0);
    let (data_io, mut batch_rx) = DataIO::new();
    let keybox = KeyBox::new(NodeCount(n_members), node_index);
    let exit_tx = spawn_honest_member_generic(
        spawner.clone(),
        NodeIndex(0),
        n_members,
        network,
        data_io,
        keybox,
    );

    let (n_batches, batches_expected) = {
        if let Some(batches) = n_batches {
            (batches, true)
        } else {
            (usize::max_value(), false)
        }
    };
    let mut batches_count = 0;
    while batches_count < n_batches {
        futures::select! {
            batch = batch_rx.next() => {
                if batch.is_some() {
                    batches_count += 1;
                } else {
                    break;
                }
            }
            _ = playback_finished_rx => {
                if !batches_expected {
                    spawner.wait_idle().await;
                    // // let it process all received data
                    // Delay::new(Duration::from_secs(1)).await;
                    break;
                }
            }
        }
    }

    if batches_expected {
        assert!(
            batches_count >= n_batches,
            "Expected at least {:?} batches, but received {:?}.",
            n_batches,
            batches_count
        );
    }

    if net_exit.send(()).is_err() {
        info!("net_exit channel is already closed");
    }
    if exit_tx.send(()).is_err() {
        info!("exit channel is already closed");
    }
    spawner.wait().await;
}

fn get_runtime() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

pub fn generate_fuzz<W: Write + Send + 'static>(output: W, n_members: usize, n_batches: usize) {
    let runtime = get_runtime();
    runtime.block_on(execute_generate_fuzz(output, n_members, n_batches));
}

pub fn check_fuzz(input: impl Read + Send + 'static, n_members: usize, n_batches: Option<usize>) {
    init_log();
    let data_iter = NetworkDataIterator::new(input);
    let runtime = get_runtime();
    runtime.block_on(execute_fuzz(data_iter, n_members, n_batches));
}

pub fn fuzz(data: Vec<FuzzNetworkData>, n_members: usize, n_batches: Option<usize>) {
    let runtime = get_runtime();
    runtime.block_on(execute_fuzz(data.into_iter(), n_members, n_batches));
}

#[derive(Clone)]
struct Spawner {
    handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    task_counter: Arc<AtomicU64>,
}

struct SpawnFuture<T> {
    task: Pin<Box<T>>,
    counter: Arc<AtomicU64>,
}

// impl<T: Future<Output = ()> + Send + 'static> AsMut<T> for SpawnFuture<T> {
//     fn as_mut(&mut self) -> &mut T {
//         &mut self.task
//     }
// }

impl<T> SpawnFuture<T> {
    fn new(task: T, counter: Arc<AtomicU64>) -> Self {
        SpawnFuture {
            task: Box::pin(task),
            counter,
        }
    }
}

// impl<T: Future<Output = ()> + Send + 'static> Deref for SpawnFuture<T> {
//     type Target = T;

//     fn deref(&self) -> &Self::Target {
//         &self.task
//     }
// }

// impl<T: Future<Output = ()> + Send + 'static> DerefMut for SpawnFuture<T> {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.task
//     }
// }

impl<T: Future<Output = ()> + Send + 'static> Future for SpawnFuture<T> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let result = Future::poll(self.task.as_mut(), cx);
        self.counter
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        result
    }
}

impl SpawnHandle for Spawner {
    fn spawn(&self, _name: &str, task: impl Future<Output = ()> + Send + 'static) {
        let wrapped = SpawnFuture::new(task, self.task_counter.clone());
        self.handles.lock().push(tokio::spawn(wrapped))
    }
}

impl Spawner {
    pub async fn wait(&self) {
        for h in self.handles.lock().iter_mut() {
            let _ = h.await;
        }
    }

    pub fn new() -> Self {
        Spawner {
            handles: Arc::new(Mutex::new(Vec::new())),
            task_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn wait_idle(&self) {
        while self.task_counter.load(std::sync::atomic::Ordering::Relaxed) > 0 {
            yield_now().await;
        }
    }
}

impl Default for Spawner {
    fn default() -> Self {
        Spawner::new()
    }
}
