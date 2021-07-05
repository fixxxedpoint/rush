pub(crate) use crate::testing::mock_common::gen_config;
pub(crate) use aleph_mock::{configure_network, init_log, Network, NetworkHook, Spawner};
use async_trait::async_trait;
use codec::{Decode, Encode};
use log::error;
use parking_lot::Mutex;

use crate::{
    member::{NotificationIn, NotificationOut},
    units::{Unit, UnitCoord},
    Hasher, NodeCount, NodeIndex, OrderedBatch,
};

use std::{
    cell::Cell,
    collections::{hash_map::DefaultHasher, HashMap},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    Future,
};

// pub(crate) type Network = crate::Network<Hasher64, Data, Signature, PartialMultisignature>;
// pub(crate) type NetworkHook =
// aleph_mock::NetworkHook<Hasher64, Data, Signature, PartialMultisignature>;
pub(crate) type NetworkData =
    crate::NetworkData<self::Hasher64, self::Data, self::Signature, self::PartialMultisignature>;

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

impl crate::PartialMultisignature for PartialMultisignature {
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

impl crate::DataIO<Data> for DataIO {
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
pub(crate) struct KeyBox {
    count: NodeCount,
    ix: NodeIndex,
}

impl KeyBox {
    pub fn new(count: NodeCount, ix: NodeIndex) -> Self {
        KeyBox { count, ix }
    }
}

impl crate::Index for KeyBox {
    fn index(&self) -> NodeIndex {
        self.ix
    }
}

#[async_trait]
impl crate::KeyBox for KeyBox {
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

impl crate::MultiKeychain for KeyBox {
    type PartialMultisignature = PartialMultisignature;
    fn from_signature(&self, _: &Self::Signature, index: NodeIndex) -> Self::PartialMultisignature {
        let signed_by = vec![index];
        PartialMultisignature { signed_by }
    }
    fn is_complete(&self, _: &[u8], partial: &Self::PartialMultisignature) -> bool {
        (self.count * 2) / 3 < NodeCount(partial.signed_by.len())
    }
}

// A hasher from the standard library that hashes to u64, should be enough to
// avoid collisions in testing.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub(crate) struct Hasher64;

impl Hasher for Hasher64 {
    type Hash = [u8; 8];

    fn hash(x: &[u8]) -> Self::Hash {
        let mut hasher = DefaultHasher::new();
        hasher.write(x);
        hasher.finish().to_ne_bytes()
    }
}

pub(crate) type Hash64 = <Hasher64 as crate::Hasher>::Hash;

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
            tx.unbounded_send(ntfct.clone()).ok();
        }
    }

    fn send_to_node(&mut self, node_ix: NodeIndex, ntfct: NotificationIn<Hasher64>) {
        let tx = self
            .ntfct_in_txs
            .get(&node_ix)
            .expect("Must connect to all nodes before running the hub.");
        tx.unbounded_send(ntfct).expect("Channel should be open");
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
            NotificationOut::MissingUnits(coords) => {
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

#[derive(Clone)]
pub(crate) struct AlertHook {
    alerts_sent_by_connection: Arc<Mutex<HashMap<(NodeIndex, NodeIndex), usize>>>,
}

impl AlertHook {
    pub(crate) fn new() -> Self {
        AlertHook {
            alerts_sent_by_connection: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn count(&self, sender: NodeIndex, recipient: NodeIndex) -> usize {
        match self
            .alerts_sent_by_connection
            .lock()
            .get(&(sender, recipient))
        {
            Some(count) => *count,
            None => 0,
        }
    }
}

impl NetworkHook<self::Hasher64, self::Data, self::Signature, self::PartialMultisignature>
    for AlertHook
{
    fn update_state(&mut self, data: &mut NetworkData, sender: NodeIndex, recipient: NodeIndex) {
        use crate::alerts::AlertMessage::*;
        use crate::network::NetworkDataInner::*;
        if let crate::NetworkData(Alert(ForkAlert(_))) = data {
            *self
                .alerts_sent_by_connection
                .lock()
                .entry((sender, recipient))
                .or_insert(0) += 1;
        }
    }
}

pub(crate) fn spawn_honest_member(
    spawner: Spawner,
    node_index: NodeIndex,
    n_members: usize,
    network: impl 'static + crate::Network<Hasher64, Data, Signature, PartialMultisignature>,
) -> (UnboundedReceiver<OrderedBatch<Data>>, oneshot::Sender<()>) {
    let (data_io, rx_batch) = aleph_mock::DataIO::new(node_index);
    let keybox = aleph_mock::KeyBox::new(NodeCount(n_members), node_index);
    let exit = aleph_mock::spawn_honest_member_generic(
        spawner, node_index, n_members, network, data_io, keybox,
    );
    (rx_batch, exit)
}
