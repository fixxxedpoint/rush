pub use aleph_mock::*;
use async_trait::async_trait;
use codec::{Decode, Encode};
use log::error;

use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    Future,
};
use parking_lot::Mutex;

use std::{
    cell::Cell,
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use crate::{
    config::{exponential_slowdown, Config, DelayConfig},
    member::{NotificationIn, NotificationOut},
    units::{Unit, UnitCoord},
    DataIO as DataIOT, Index, KeyBox as KeyBoxT, MultiKeychain as MultiKeychainT,
    Network as NetworkT, NodeCount, NodeIndex, OrderedBatch,
    PartialMultisignature as PartialMultisignatureT, SpawnHandle,
};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Hash)]
pub struct Data {
    coord: UnitCoord,
    variant: u32,
}

impl Data {
    #[cfg(test)]
    pub(crate) fn new(coord: UnitCoord, variant: u32) -> Self {
        Data { coord, variant }
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

#[derive(Clone)]
pub(crate) struct KeyBox {
    count: NodeCount,
    ix: NodeIndex,
}

impl KeyBox {
    pub(crate) fn new(count: NodeCount, ix: NodeIndex) -> Self {
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

impl aleph_mock::NetworkHook for AlertHook {
    fn update_state(
        &mut self,
        data: &mut aleph_mock::NetworkData,
        sender: NodeIndex,
        recipient: NodeIndex,
    ) {
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

pub fn gen_config(node_ix: NodeIndex, n_members: NodeCount) -> Config {
    let delay_config = DelayConfig {
        tick_interval: Duration::from_millis(5),
        requests_interval: Duration::from_millis(50),
        unit_broadcast_delay: Arc::new(|t| exponential_slowdown(t, 100.0, 1, 3.0)),
        //100, 100, 300, 900, 2700, ...
        unit_creation_delay: Arc::new(|t| exponential_slowdown(t, 50.0, usize::MAX, 1.000)),
        //50, 50, 50, 50, ...
    };
    Config {
        node_ix,
        session_id: 0,
        n_members,
        delay_config,
        rounds_margin: 200,
        max_units_per_alert: 200,
        max_round: 5000,
    }
}

pub fn spawn_honest_member(
    spawner: impl SpawnHandle,
    node_index: NodeIndex,
    n_members: usize,
    network: impl 'static + NetworkT<aleph_mock::Hasher64, Data, Signature, PartialMultisignature>,
) -> (UnboundedReceiver<OrderedBatch<Data>>, oneshot::Sender<()>) {
    let (data_io, rx_batch) = DataIO::new(node_index);
    let keybox = KeyBox::new(NodeCount(n_members), node_index);
    let config = gen_config(node_index, n_members.into());
    let exit_tx =
        aleph_mock::spawn_honest_member_generic(spawner, config, network, data_io, &keybox);
    (rx_batch, exit_tx)
}
