use codec::{Decode, Encode};
use log::{debug, error};
use parking_lot::Mutex;

use tokio::{task::yield_now, time::Duration};

use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    Future, StreamExt,
};

use std::{
    cell::Cell,
    collections::{hash_map::DefaultHasher, HashMap},
    fs::File,
    hash::Hasher as StdHasher,
    io::{BufWriter, Write},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    member::{Config, NotificationIn, NotificationOut},
    network::NetworkDataInner,
    units::{Unit, UnitCoord},
    DataIO as DataIOT, Hasher, Index, KeyBox as KeyBoxT, Network as NetworkT,
    NetworkData as NetworkDataT, NodeCount, NodeIndex, OrderedBatch, SpawnHandle,
};

use crate::member::Member;

pub fn init_log() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::max())
        .is_test(true)
        .try_init();
}

// A hasher from the standard library that hashes to u64, should be enough to
// avoid collisions in testing.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Hasher64;

impl Hasher for Hasher64 {
    type Hash = [u8; 8];

    fn hash(x: &[u8]) -> Self::Hash {
        let mut hasher = DefaultHasher::new();
        hasher.write(x);
        hasher.finish().to_ne_bytes()
    }
}

// This struct allows to create a Hub to interconnect several instances of the Consensus engine, without
// requiring the Member wrapper. The Hub notifies all connected instances about newly created units and
// is able to answer unit requests as well. WrongControlHashes are not supported, which means that this
// Hub should be used to run simple tests in honest scenarios only.
// Usage: 1) create an instance using new(n_members), 2) connect all n_members instances, 0, 1, 2, ..., n_members - 1.
// 3) run the HonestHub instance as a Future.
pub(crate) struct HonestHub {
    n_members: usize,
    ntfct_out_rxs: HashMap<NodeIndex, UnboundedReceiver<NotificationOut<Hasher64>>>,
    ntfct_in_txs: HashMap<NodeIndex, UnboundedSender<NotificationIn<Hasher64>>>,
    units_by_coord: HashMap<UnitCoord, Unit<Hasher64>>,
}

impl HonestHub {
    pub(crate) fn new(n_members: usize) -> Self {
        HonestHub {
            n_members,
            ntfct_out_rxs: HashMap::new(),
            ntfct_in_txs: HashMap::new(),
            units_by_coord: HashMap::new(),
        }
    }

    pub(crate) fn connect(
        &mut self,
        node_ix: NodeIndex,
    ) -> (
        UnboundedSender<NotificationOut<Hasher64>>,
        UnboundedReceiver<NotificationIn<Hasher64>>,
    ) {
        let (tx_in, rx_in) = unbounded();
        let (tx_out, rx_out) = unbounded();
        self.ntfct_in_txs.insert(node_ix, tx_in);
        self.ntfct_out_rxs.insert(node_ix, rx_out);
        (tx_out, rx_in)
    }

    fn send_to_all(&mut self, ntfct: NotificationIn<Hasher64>) {
        assert!(
            self.ntfct_in_txs.len() == self.n_members,
            "Must connect to all nodes before running the hub."
        );
        for (_ix, tx) in self.ntfct_in_txs.iter() {
            tx.unbounded_send(ntfct.clone()).unwrap();
        }
    }

    fn send_to_node(&mut self, node_ix: NodeIndex, ntfct: NotificationIn<Hasher64>) {
        let tx = self
            .ntfct_in_txs
            .get(&node_ix)
            .expect("Must connect to all nodes before running the hub.");
        let _ = tx.unbounded_send(ntfct);
    }

    fn on_notification(&mut self, node_ix: NodeIndex, ntfct: NotificationOut<Hasher64>) {
        match ntfct {
            NotificationOut::CreatedPreUnit(pu) => {
                let hash = pu.using_encoded(Hasher64::hash);
                let u = Unit::new(pu, hash);
                let coord = UnitCoord::new(u.round(), u.creator());
                self.units_by_coord.insert(coord, u.clone());
                self.send_to_all(NotificationIn::NewUnits(vec![u]));
            }
            NotificationOut::MissingUnits(coords, _aux_data) => {
                let mut response_units = Vec::new();
                for coord in coords {
                    match self.units_by_coord.get(&coord) {
                        Some(unit) => {
                            response_units.push(unit.clone());
                        }
                        None => {
                            panic!("Unit requested that the hub does not know.");
                        }
                    }
                }
                let ntfct = NotificationIn::NewUnits(response_units);
                self.send_to_node(node_ix, ntfct);
            }
            NotificationOut::WrongControlHash(_u_hash) => {
                panic!("No support for forks in testing.");
            }
            NotificationOut::AddedToDag(_u_hash, _hashes) => {
                // Safe to ignore in testing.
                // Normally this is used in Member to answer parents requests.
            }
        }
    }
}

impl Future for HonestHub {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut ready_ixs: Vec<NodeIndex> = Vec::new();
        let mut buffer = Vec::new();
        for (ix, rx) in self.ntfct_out_rxs.iter_mut() {
            loop {
                match rx.poll_next_unpin(cx) {
                    Poll::Ready(Some(ntfct)) => {
                        buffer.push((*ix, ntfct));
                    }
                    Poll::Ready(None) => {
                        ready_ixs.push(*ix);
                        break;
                    }
                    Poll::Pending => {
                        break;
                    }
                }
            }
        }
        for (ix, ntfct) in buffer {
            self.on_notification(ix, ntfct);
        }
        for ix in ready_ixs {
            self.ntfct_out_rxs.remove(&ix);
        }
        if self.ntfct_out_rxs.is_empty() {
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

#[derive(Default, Clone)]
pub(crate) struct Spawner {
    handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl SpawnHandle for Spawner {
    fn spawn(&self, _name: &str, task: impl Future<Output = ()> + Send + 'static) {
        self.handles.lock().push(tokio::spawn(task))
    }
}

impl Spawner {
    pub(crate) async fn wait(&self) {
        for h in self.handles.lock().iter_mut() {
            let _ = h.await;
        }
    }
    pub(crate) fn new() -> Self {
        Spawner {
            handles: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

type NetworkData = NetworkDataT<Hasher64, Data, Signature>;
type NetworkReceiver = UnboundedReceiver<(NetworkData, NodeIndex)>;
type NetworkSender = UnboundedSender<(NetworkData, NodeIndex)>;

pub(crate) struct Network {
    rx: NetworkReceiver,
    tx: NetworkSender,
    peers: Vec<NodeIndex>,
    index: NodeIndex,
}

#[async_trait::async_trait]
impl NetworkT<Hasher64, Data, Signature> for Network {
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

#[derive(Clone)]
pub(crate) struct UnreliableRouter<Hook> {
    peers: HashMap<NodeIndex, Peer>,
    peer_list: Vec<NodeIndex>,
    hook: Hook,
    reliability: f64, //a number in the range [0, 1], 1.0 means perfect reliability, 0.0 means no message gets through
}

impl<Hook: NetworkHook> UnreliabeRouter<Hook> {
    pub(crate) fn new(peer_list: Vec<NodeIndex>, reliability: f64, hook: Hook) -> Self {
        NetworkHub {
            peers: HashMap::new(),
            peer_list,
            hook,
            reliability,
        }
    }

    pub(crate) fn connect_peer(&mut self, peer: PeerId) -> Network {
        assert!(
            self.peer_list.iter().any(|p| *p == peer),
            "Must connect a peer in the list."
        );
        assert!(
            !self.peers.contains_key(&peer),
            "Cannot connect a peer twice."
        );
        let (tx_in_hub, rx_in_hub) = unbounded();
        let (tx_out_hub, rx_out_hub) = unbounded();
        let peer_entry = Peer {
            tx: tx_out_hub,
            rx: rx_in_hub,
        };
        self.peers.insert(peer.clone(), peer_entry);
        Network {
            rx: rx_out_hub,
            tx: tx_in_hub,
            peer_list: self.peer_list.clone(),
            my_peer_id: peer,
        }
    }

    pub(crate) async fn run(&mut self) {
        let mut disconnected_peers = Vec::new();
        let mut buffer = Vec::new();
        while !self.peers.is_empty() {
            for (peer_id, peer) in self.peers.iter_mut() {
                loop {
                    match peer.rx.try_next() {
                        Ok(Some(msg)) => buffer.push(msg),
                        Ok(None) => {
                            disconnected_peers.push(peer_id.clone());
                            break;
                        }
                        Err(_) => break,
                    }
                }
            }
            for peer_id in disconnected_peers.drain(..) {
                self.peers.remove(&peer_id);
            }
            for addr_msg in buffer.drain(..) {
                let rand_sample = rand::random::<f64>();
                if rand_sample > self.reliability {
                    debug!("Simulated network fail.");
                    continue;
                }
                if let Some(peer) = self.peers.get_mut(&addr_msg.receiver.clone()) {
                    self.hook.update_state(addr_msg.clone());
                    if let Err(e) = peer.tx.unbounded_send(addr_msg) {
                        error!(target: "network-hub", "Error when routing message via hub {:?}.", e);
                    }
                }
                peer.tx
                    .unbounded_send((data, sender))
                    .expect("channel should be open");
            }
            yield_now().await;
        }
    }
}

impl<Hook: NetworkHook + Unpin> Future for NetworkHub<Hook> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let mut disconnected_peers: Vec<PeerId> = Vec::new();
        let mut buffer = Vec::new();
        for (peer_id, peer) in this.peers.iter_mut() {
            loop {
                match peer.rx.poll_next_unpin(cx) {
                    Poll::Ready(Some(msg)) => {
                        buffer.push(msg);
                    }
                    Poll::Ready(None) => {
                        disconnected_peers.push(peer_id.clone());
                        break;
                    }
                    Poll::Pending => {
                        break;
                    }
                }
            }
        }
        for peer_id in disconnected_peers {
            this.peers.remove(&peer_id);
        }
        for addr_msg in buffer {
            let rand_sample = rand::random::<f64>();
            if rand_sample > this.reliability {
                debug!("Simulated network fail.");
                continue;
            }
            if let Some(peer) = this.peers.get_mut(&addr_msg.receiver.clone()) {
                this.hook.update_state(addr_msg.clone());
                if let Err(e) = peer.tx.unbounded_send(addr_msg) {
                    error!(target: "network-hub", "Error when routing message via hub {:?}.", e);
                }
            }
        }

        Poll::Pending
    }
}

pub(crate) trait NetworkHook: Send {
    fn update_state(&mut self, data: NetworkData, sender: NodeIndex, recipient: NodeIndex);
}

pub(crate) struct NoOpHook(());

impl NoOpHook {
    pub(crate) fn new() -> NoOpHook {
        NoOpHook(())
    }
}

impl NetworkHook for NoOpHook {
    fn update_state(&mut self, _: NetworkData, _: NodeIndex, _: NodeIndex) {}
}

#[derive(Clone)]
pub(crate) struct AlertHook {
    alert_count: Arc<Mutex<usize>>,
}

impl AlertHook {
    pub(crate) fn new() -> Self {
        AlertHook {
            alert_count: Arc::new(Mutex::new(0)),
        }
    }

    pub(crate) fn count(&self) -> usize {
        *self.alert_count.lock()
    }
}

impl NetworkHook for AlertHook {
    fn update_state(&self, data: NetworkData, _: NodeIndex, _: NodeIndex) {
        use NetworkDataInner::*;
        if let NetworkDataT(Alert(_)) = data {
            let mut alert_count = self.alert_count.lock();
            *alert_count += 1;
        }
    }
}

pub(crate) struct FilteringHook<IH, F> {
    wrapped: IH,
    filter: F,
}

impl<IH, F: FnMut(&AddressedMessage) -> bool + Send> FilteringHook<IH, F> {
    pub(crate) fn new(wrapped: IH, filter: F) -> FilteringHook<IH, F> {
        FilteringHook { wrapped, filter }
    }
}

impl<IH: NetworkHook, F: FnMut(&AddressedMessage) -> bool + Send> NetworkHook
    for FilteringHook<IH, F>
{
    fn update_state(&mut self, msg: AddressedMessage) {
        if (self.filter)(&msg) {
            self.wrapped.update_state(msg);
        }
    }
}

pub(crate) struct EvesDroppingHook<S> {
    sink: S,
}

impl<W: Write> EvesDroppingHook<BufWriter<W>> {
    pub(crate) fn new(writer: W) -> EvesDroppingHook<BufWriter<W>> {
        let buffered_file = BufWriter::new(writer);
        EvesDroppingHook {
            sink: buffered_file,
        }
    }
}

impl<S: Write + Send> NetworkHook for EvesDroppingHook<S> {
    fn update_state(&mut self, msg: AddressedMessage) {
        self.sink
            .write_all(&msg.message[..])
            .expect("error while evesdropping network communication");
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Hash)]
pub(crate) struct Data {
    coord: UnitCoord,
    variant: u32,
}

impl Data {
    pub(crate) fn new(coord: UnitCoord, variant: u32) -> Self {
        Data { coord, variant }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub(crate) struct Signature {}

pub(crate) struct DataIO {
    ix: NodeIndex,
    round_counter: Cell<usize>,
    tx: UnboundedSender<OrderedBatch<Data>>,
}

impl DataIOT<Data> for DataIO {
    type Error = ();
    fn get_data(&self) -> Data {
        let coord = UnitCoord::new(self.round_counter.get(), self.ix);
        self.round_counter.set(self.round_counter.get() + 1);
        Data { coord, variant: 0 }
    }
    fn send_ordered_batch(&mut self, data: OrderedBatch<Data>) -> Result<(), ()> {
        self.tx.unbounded_send(data).map_err(|e| {
            error!(target: "data-io", "Error when sending data from DataIO {:?}.", e);
        })
    }
}

impl DataIO {
    pub(crate) fn new(ix: NodeIndex) -> (Self, UnboundedReceiver<OrderedBatch<Data>>) {
        let (tx, rx) = unbounded();
        let data_io = DataIO {
            ix,
            round_counter: Cell::new(0),
            tx,
        };
        (data_io, rx)
    }
}

pub(crate) struct KeyBox {
    ix: NodeIndex,
}

impl KeyBox {
    pub(crate) fn new(ix: NodeIndex) -> Self {
        KeyBox { ix }
    }
}

impl Index for KeyBox {
    fn index(&self) -> NodeIndex {
        self.ix
    }
}

impl KeyBoxT for KeyBox {
    type Signature = Signature;
    fn sign(&self, _msg: &[u8]) -> Signature {
        Signature {}
    }
    fn verify(&self, _msg: &[u8], _sgn: &Signature, _index: NodeIndex) -> bool {
        true
    }
}

fn gen_config(ix: NodeIndex, n_members: NodeCount) -> Config {
    Config {
        node_id: ix,
        session_id: 0,
        n_members,
        create_lag: Duration::from_millis(100),
    }
}

pub(crate) type HonestMember<'a> = Member<'a, Hasher64, Data, DataIO, KeyBox>;

pub(crate) fn configure_network(
    n_members: usize,
    reliability: f64,
    hook: impl NetworkHook,
) -> (UnreliableRouter, Vec<Option<Network>>) {
    let peer_list = (0..n_members).map(NodeIndex).collect();

    let router = UnreliableRouter::new(peer_list, reliability, hook);
    let mut networks = Vec::new();
    for ix in 0..n_members {
        let network = router.connect_peer(NodeIndex(ix));
        networks.push(Some(network));
    }
    (router, networks)
}

pub(crate) fn spawn_honest_member(
    spawner: Spawner,
    ix: usize,
    n_members: usize,
    network: Network,
) -> (UnboundedReceiver<OrderedBatch<Data>>, oneshot::Sender<()>) {
    let node_index = NodeIndex(ix);
    let (data_io, rx_batch) = DataIO::new(node_index);
    let config = gen_config(node_index, n_members.into());
    let (exit_tx, exit_rx) = oneshot::channel();
    let spawner_inner = spawner.clone();
    let member_task = async move {
        let keybox = KeyBox::new(node_index);
        let member = HonestMember::new(data_io, &keybox, config);
        member.run_session(network, spawner_inner, exit_rx).await;
    };
    spawner.spawn("member", member_task);
    (rx_batch, exit_tx)
}
