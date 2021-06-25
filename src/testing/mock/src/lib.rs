use async_trait::async_trait;
use codec::{Decode, Encode};
use log::{debug, error};
use parking_lot::Mutex;

use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    channel::oneshot,
    Future, StreamExt,
};

use std::{
    cell::{Cell, RefCell},
    collections::{hash_map::DefaultHasher, HashMap},
    hash::Hasher as StdHasher,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use aleph_bft::{
    Config, DataIO as DataIOT, Hasher, Index, KeyBox as KeyBoxT, Member,
    MultiKeychain as MultiKeychainT, Network as NetworkT, NodeCount, NodeIndex, OrderedBatch,
    PartialMultisignature as PartialMultisignatureT, SpawnHandle,
};

pub fn init_log() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::max())
        .is_test(true)
        .try_init();
}

// A hasher from the standard library that hashes to u64, should be enough to
// avoid collisions in testing.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct Hasher64;

impl Hasher for Hasher64 {
    type Hash = [u8; 8];

    fn hash(x: &[u8]) -> Self::Hash {
        let mut hasher = DefaultHasher::new();
        hasher.write(x);
        hasher.finish().to_ne_bytes()
    }
}

pub type Hash64 = <Hasher64 as Hasher>::Hash;

#[derive(Clone)]
pub struct Spawner {
    handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl SpawnHandle for Spawner {
    fn spawn(&self, _name: &str, task: impl Future<Output = ()> + Send + 'static) {
        self.handles.lock().push(tokio::spawn(task))
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
        }
    }
}

impl Default for Spawner {
    fn default() -> Self {
        Spawner::new()
    }
}

pub type NetworkData = aleph_bft::NetworkData<Hasher64, Data, Signature, PartialMultisignature>;
type NetworkReceiver = UnboundedReceiver<(NetworkData, NodeIndex)>;
type NetworkSender = UnboundedSender<(NetworkData, NodeIndex)>;

pub struct Network {
    rx: NetworkReceiver,
    tx: NetworkSender,
    peers: Vec<NodeIndex>,
    index: NodeIndex,
}

impl Network {
    pub fn index(&self) -> NodeIndex {
        self.index
    }
}

#[async_trait::async_trait]
impl NetworkT<Hasher64, Data, Signature, PartialMultisignature> for Network {
    type Error = ();

    fn broadcast(&self, data: NetworkData) -> Result<(), Self::Error> {
        for peer in self.peers.iter() {
            if *peer != self.index {
                self.send(data.clone(), *peer)?;
            }
        }
        Ok(())
    }

    fn send(&self, data: NetworkData, node: NodeIndex) -> Result<(), Self::Error> {
        self.tx.unbounded_send((data, node)).map_err(|_| ())
    }

    async fn next_event(&mut self) -> Option<NetworkData> {
        Some(self.rx.next().await?.0)
    }
}

struct Peer {
    tx: NetworkSender,
    rx: NetworkReceiver,
}

pub struct UnreliableRouter {
    peers: RefCell<HashMap<NodeIndex, Peer>>,
    peer_list: Vec<NodeIndex>,
    hook_list: RefCell<Vec<Box<dyn NetworkHook>>>,
    reliability: f64, //a number in the range [0, 1], 1.0 means perfect reliability, 0.0 means no message gets through
}

impl UnreliableRouter {
    pub fn new(peer_list: Vec<NodeIndex>, reliability: f64) -> Self {
        UnreliableRouter {
            peers: RefCell::new(HashMap::new()),
            peer_list,
            hook_list: RefCell::new(Vec::new()),
            reliability,
        }
    }

    pub fn add_hook<HK: NetworkHook + 'static>(&mut self, hook: HK) {
        self.hook_list.borrow_mut().push(Box::new(hook));
    }

    pub fn connect_peer(&mut self, peer: NodeIndex) -> Network {
        assert!(
            self.peer_list.iter().any(|p| *p == peer),
            "Must connect a peer in the list."
        );
        assert!(
            !self.peers.borrow().contains_key(&peer),
            "Cannot connect a peer twice."
        );
        let (tx_in_hub, rx_in_hub) = unbounded();
        let (tx_out_hub, rx_out_hub) = unbounded();
        let peer_entry = Peer {
            tx: tx_out_hub,
            rx: rx_in_hub,
        };
        self.peers.borrow_mut().insert(peer, peer_entry);
        Network {
            rx: rx_out_hub,
            tx: tx_in_hub,
            peers: self.peer_list.clone(),
            index: peer,
        }
    }
}

impl Future for UnreliableRouter {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut self;
        let mut disconnected_peers: Vec<NodeIndex> = Vec::new();
        let mut buffer = Vec::new();
        for (peer_id, peer) in this.peers.borrow_mut().iter_mut() {
            loop {
                // this call is responsible for waking this Future
                match peer.rx.poll_next_unpin(cx) {
                    Poll::Ready(Some(msg)) => {
                        buffer.push((*peer_id, msg));
                    }
                    Poll::Ready(None) => {
                        disconnected_peers.push(*peer_id);
                        break;
                    }
                    Poll::Pending => {
                        break;
                    }
                }
            }
        }
        for peer_id in disconnected_peers {
            this.peers.borrow_mut().remove(&peer_id);
        }
        for (sender, (mut data, recipient)) in buffer {
            let rand_sample = rand::random::<f64>();
            if rand_sample > this.reliability {
                debug!("Simulated network fail.");
                continue;
            }

            if let Some(peer) = this.peers.borrow().get(&recipient) {
                for hook in this.hook_list.borrow_mut().iter_mut() {
                    hook.update_state(&mut data, sender, recipient);
                }
                peer.tx
                    .unbounded_send((data, sender))
                    .expect("channel should be open");
            }
        }
        if this.peers.borrow().is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

pub trait NetworkHook: Send {
    fn update_state(&mut self, data: &mut NetworkData, sender: NodeIndex, recipient: NodeIndex);
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Hash)]
pub struct Data {
    ix: NodeIndex,
    id: u64,
}

impl Data {
    fn new(ix: NodeIndex, id: u64) -> Self {
        Data { ix, id }
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
    ix: NodeIndex,
    round_counter: Cell<u64>,
    tx: UnboundedSender<OrderedBatch<Data>>,
}

impl DataIOT<Data> for DataIO {
    type Error = ();
    fn get_data(&self) -> Data {
        self.round_counter.set(self.round_counter.get() + 1);
        Data::new(self.ix, self.round_counter.get())
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
    pub fn new(ix: NodeIndex) -> (Self, UnboundedReceiver<OrderedBatch<Data>>) {
        let (tx, rx) = unbounded();
        let data_io = DataIO {
            ix,
            round_counter: Cell::new(0),
            tx,
        };
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

pub fn configure_network(n_members: usize, reliability: f64) -> (UnreliableRouter, Vec<Network>) {
    let peer_list = || (0..n_members).map(NodeIndex);

    let mut router = UnreliableRouter::new(peer_list().collect(), reliability);
    let mut networks = Vec::new();
    for ix in peer_list() {
        let network = router.connect_peer(ix);
        networks.push(network);
    }
    (router, networks)
}

pub fn spawn_honest_member_generic<D: aleph_bft::Data, H: Hasher, K: MultiKeychainT>(
    spawner: impl SpawnHandle,
    config: Config,
    network: impl 'static + NetworkT<H, D, K::Signature, K::PartialMultisignature>,
    data_io: impl DataIOT<D> + Send + 'static,
    mk: &'static K,
) -> oneshot::Sender<()> {
    let (exit_tx, exit_rx) = oneshot::channel();
    let spawner_inner = spawner.clone();
    let member_task = async move {
        let member = Member::new(data_io, mk, config, spawner_inner.clone());
        member.run_session(network, exit_rx).await;
    };
    spawner.spawn("member", member_task);
    exit_tx
}
