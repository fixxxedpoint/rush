use codec::{Decode, Encode};
use futures::{
    channel::{mpsc, oneshot},
    pin_mut,
    stream::iter,
    FutureExt, StreamExt,
};
use log::{debug, error, info, trace, warn};
use rand::Rng;

use crate::{
    config::Config,
    network::{NetworkHub, Recipient},
    runway::{Runway, RunwayConfigBuilder},
    signed::Signature,
    units::{PreUnit, UncheckedSignedUnit, Unit, UnitCoord, UnitStore},
    Data, DataIO, Hasher, MultiKeychain, Network, NodeCount, NodeIndex, Sender, SpawnHandle,
};

use futures_timer::Delay;
use std::{cmp::Ordering, collections::BinaryHeap, fmt::Debug, iter::repeat, time};

/// A message concerning units, either about new units or some requests for them.
#[derive(Debug, Encode, Decode, Clone)]
pub(crate) enum UnitMessage<H: Hasher, D: Data, S: Signature> {
    /// For disseminating newly created units.
    NewUnit(UncheckedSignedUnit<H, D, S>),
    /// Request for a unit by its coord.
    RequestCoord(NodeIndex, UnitCoord),
    /// Response to a request by coord.
    ResponseCoord(UncheckedSignedUnit<H, D, S>),
    /// Request for the full list of parents of a unit.
    RequestParents(NodeIndex, H::Hash),
    /// Response to a request for a full list of parents.
    ResponseParents(H::Hash, Vec<UncheckedSignedUnit<H, D, S>>),
}

impl<H: Hasher, D: Data, S: Signature> UnitMessage<H, D, S> {
    pub(crate) fn included_data(&self) -> Vec<D> {
        match self {
            Self::NewUnit(uu) => vec![uu.as_signable().data().clone()],
            Self::RequestCoord(_, _) => Vec::new(),
            Self::ResponseCoord(uu) => vec![uu.as_signable().data().clone()],
            Self::RequestParents(_, _) => Vec::new(),
            Self::ResponseParents(_, units) => units
                .iter()
                .map(|uu| uu.as_signable().data().clone())
                .collect(),
        }
    }
}

/// Type for incoming notifications: Member to Consensus.
#[derive(Clone, PartialEq)]
pub(crate) enum NotificationIn<H: Hasher> {
    /// A notification carrying a single unit. This might come either from multicast or
    /// from a response to a request. This is of no importance at this layer.
    NewUnits(Vec<Unit<H>>),
    /// Response to a request to decode parents when the control hash is wrong.
    UnitParents(H::Hash, Vec<H::Hash>),
}

/// Type for outgoing notifications: Consensus to Member.
#[derive(Debug, PartialEq)]
pub(crate) enum NotificationOut<H: Hasher> {
    /// Notification about a preunit created by this Consensus Node. Member is meant to
    /// disseminate this preunit among other nodes.
    CreatedPreUnit(PreUnit<H>),
    /// Notification that some units are needed but missing. The role of the Member
    /// is to fetch these unit (somehow).
    MissingUnits(Vec<UnitCoord>),
    /// Notification that Consensus has parents incompatible with the control hash.
    WrongControlHash(H::Hash),
    /// Notification that a new unit has been added to the DAG, list of decoded parents provided
    AddedToDag(H::Hash, Vec<H::Hash>),
}

#[derive(Eq, PartialEq)]
enum Task<H: Hasher> {
    CoordRequest(UnitCoord),
    ParentsRequest(H::Hash),
    //The index of a unit in our local store, and the number of this multicast (i.e., how many times was the unit multicast already).
    UnitMulticast(usize, usize),
}

#[derive(Eq, PartialEq)]
struct ScheduledTask<H: Hasher> {
    task: Task<H>,
    scheduled_time: time::Instant,
}

impl<H: Hasher> ScheduledTask<H> {
    fn new(task: Task<H>, scheduled_time: time::Instant) -> Self {
        ScheduledTask {
            task,
            scheduled_time,
        }
    }
}

impl<H: Hasher> Ord for ScheduledTask<H> {
    fn cmp(&self, other: &Self) -> Ordering {
        // we want earlier times to come first when used in max-heap, hence the below:
        other.scheduled_time.cmp(&self.scheduled_time)
    }
}

impl<H: Hasher> PartialOrd for ScheduledTask<H> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A representation of a committee member responsible for establishing the consensus.
///
/// It depends on the following objects (for more detailed description of the above obejcts, see their references):
/// - [`Hasher`] - an abstraction for creating identifiers for units, alerts, and other internal objects,
/// - [`DataIO`] - an abstraction for a component that outputs data items and allows to input ordered data items,
/// - [`MultiKeychain`] - an abstraction for digitally signing arbitrary data and verifying signatures,
/// - [`Network`] - an abstraction for a network connecting the committee members,
/// - [`SpawnHandle`] - an abstraction for an executor of asynchronous tasks.
///
/// For a detailed description of the consensus implemented in Member see
/// [docs for devs](https://cardinal-cryptography.github.io/AlephBFT/index.html)
/// or the [original paper](https://arxiv.org/abs/1908.05156).
pub struct Member<'a, H, D, DP, MK, SH>
where
    H: Hasher,
    D: Data,
    DP: DataIO<D> + Send,
    MK: MultiKeychain,
    SH: SpawnHandle,
{
    config: Config,
    tx_consensus: Option<Sender<NotificationIn<H>>>,
    data_io: Option<DP>,
    keybox: &'a MK,
    store: UnitStore<H, D, MK>,
    requests: BinaryHeap<ScheduledTask<H>>,
    threshold: NodeCount,
    n_members: NodeCount,
    unit_messages_for_network: Option<Sender<(UnitMessage<H, D, MK::Signature>, Recipient)>>,
    spawn_handle: SH,
    scheduled_units: Vec<UncheckedSignedUnit<H, D, MK::Signature>>,
}

impl<H, D, DP, MK, SH> Member<'static, H, D, DP, MK, SH>
where
    H: Hasher,
    D: Data,
    DP: 'static + DataIO<D> + Send,
    MK: MultiKeychain,
    SH: SpawnHandle,
{
    /// Create a new instance of the Member for a given session. Under the hood, the Member implementation
    /// makes an extensive use of asynchronous features of Rust, so creating a new Member doesn't start it.
    /// See [`Member::run_session`].
    pub fn new(data_io: DP, keybox: &'static MK, config: Config, spawn_handle: SH) -> Self {
        let n_members = config.n_members;
        let threshold = (n_members * 2) / 3 + NodeCount(1);
        let max_round = config.max_round;
        Member {
            config,
            tx_consensus: None,
            data_io: Some(data_io),
            keybox,
            store: UnitStore::new(n_members, threshold, max_round),
            requests: BinaryHeap::new(),
            threshold,
            n_members,
            unit_messages_for_network: None,
            spawn_handle,
            scheduled_units: Vec::new(),
        }
    }

    fn on_create(&mut self, u: UncheckedSignedUnit<H, D, MK::Signature>) {
        let index = self.scheduled_units.len();
        self.scheduled_units.push(u);
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::UnitMulticast(index, 0), curr_time);
        self.requests.push(task);
    }

    fn on_request_coord(&mut self, node: NodeIndex, coord: UnitCoord) {
        trace!(target: "AlephBFT-member", "{:?} Dealing with missing coord notification {:?}.", self.index(), coord);
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::CoordRequest(coord), curr_time);
        self.requests.push(task);
    }

    fn on_request_parents(&mut self, index: NodeIndex, u_hash: H::Hash) {
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::ParentsRequest(u_hash), curr_time);
        self.requests.push(task);
        self.trigger_tasks();
    }

    // Pulls tasks from the priority queue (sorted by scheduled time) and sends them to random peers
    // as long as they are scheduled at time <= curr_time
    pub(crate) fn trigger_tasks(&mut self) {
        while let Some(request) = self.requests.peek() {
            let curr_time = time::Instant::now();
            if request.scheduled_time > curr_time {
                break;
            }
            let request = self.requests.pop().expect("The element was peeked");

            match request.task {
                Task::CoordRequest(coord) => {
                    self.schedule_coord_request(coord, curr_time);
                }
                Task::UnitMulticast(index, multicast_number) => {
                    self.schedule_unit_multicast(index, multicast_number, curr_time);
                }
                Task::ParentsRequest(u_hash) => {
                    self.schedule_parents_request(u_hash, curr_time);
                }
            }
        }
    }

    fn random_peer(&self) -> NodeIndex {
        rand::thread_rng()
            .gen_range(0..self.n_members.into())
            .into()
    }

    fn index(&self) -> NodeIndex {
        self.keybox.index()
    }

    fn send_unit_message(&mut self, message: UnitMessage<H, D, MK::Signature>, peer_id: NodeIndex) {
        self.unit_messages_for_network
            .as_ref()
            .unwrap()
            .unbounded_send((message, Recipient::Node(peer_id)))
            .expect("Channel to network should be open")
    }

    fn broadcast_units(&mut self, message: UnitMessage<H, D, MK::Signature>) {
        self.unit_messages_for_network
            .as_ref()
            .unwrap()
            .unbounded_send((message, Recipient::Everyone))
            .expect("Channel to network should be open")
    }

    fn schedule_parents_request(&mut self, u_hash: H::Hash, curr_time: time::Instant) {
        if self.store.get_parents(u_hash).is_none() {
            let message = UnitMessage::<H, D, MK::Signature>::RequestParents(self.index(), u_hash);
            let peer_id = self.random_peer();
            self.send_unit_message(message, peer_id);
            trace!(target: "AlephBFT-member", "{:?} Fetch parents for {:?} sent.", self.index(), u_hash);
            let delay = self.config.delay_config.requests_interval;
            self.requests.push(ScheduledTask::new(
                Task::ParentsRequest(u_hash),
                curr_time + delay,
            ));
        } else {
            trace!(target: "AlephBFT-member", "{:?} Request dropped as the parents are in store for {:?}.", self.index(), u_hash);
        }
    }

    fn schedule_coord_request(&mut self, coord: UnitCoord, curr_time: time::Instant) {
        trace!(target: "AlephBFT-member", "{:?} Starting request for {:?}", self.index(), coord);
        // If we already have a unit with such a coord in our store then there is no need to request it.
        // It will be sent to consensus soon (or have already been sent).
        if self.store.contains_coord(&coord) {
            trace!(target: "AlephBFT-member", "{:?} Request dropped as the unit is in store already {:?}", self.index(), coord);
            return;
        }
        let message = UnitMessage::<H, D, MK::Signature>::RequestCoord(self.index(), coord);
        let peer_id = self.random_peer();
        self.send_unit_message(message, peer_id);
        trace!(target: "AlephBFT-member", "{:?} Fetch request for {:?} sent.", self.index(), coord);
        let delay = self.config.delay_config.requests_interval;
        self.requests.push(ScheduledTask::new(
            Task::CoordRequest(coord),
            curr_time + delay,
        ));
    }

    fn schedule_unit_multicast(
        &mut self,
        index: usize,
        multicast_number: usize,
        curr_time: time::Instant,
    ) {
        let signed_unit = self
            .scheduled_units
            .get(index)
            .expect("we store all scheduled units")
            .clone();
        let hash = signed_unit.as_signable().hash();
        let message = UnitMessage::<H, D, MK::Signature>::NewUnit(signed_unit.into());
        // TODO consider not using hash() here
        trace!(target: "AlephBFT-member", "{:?} Sending a unit {:?} over network {:?}th time.", self.index(), hash, multicast_number);
        self.broadcast_units(message);
        let delay = (self.config.delay_config.unit_broadcast_delay)(multicast_number);
        self.requests.push(ScheduledTask::new(
            Task::UnitMulticast(index, multicast_number + 1),
            curr_time + delay,
        ));
    }

    fn on_unit_message_from_units(
        &mut self,
        message: UnitMessage<H, D, MK::Signature>,
        recipient: Option<Recipient>,
    ) {
        match message {
            UnitMessage::NewUnit(u) => self.on_create(u),
            UnitMessage::RequestCoord(index, coord) => self.on_request_coord(index, coord),
            UnitMessage::ResponseCoord(_) => match recipient {
                Some(Recipient::Node(node_ix)) => {
                    self.send_unit_message(message, node_ix);
                }
                _ => {
                    warn!(target: "AlephBFT-member", "{:?} Missing Recipient for a ResponseCoord message..", self.index());
                }
            },
            UnitMessage::RequestParents(index, hash) => self.on_request_parents(index, hash),
            UnitMessage::ResponseParents(_, _) => match recipient {
                Some(Recipient::Node(node_ix)) => {
                    self.send_unit_message(message, node_ix);
                }
                _ => {
                    warn!(target: "AlephBFT-member", "{:?} Missing Recipient for a ResponseParents message..", self.index());
                }
            },
        }
    }

    /// Actually start the Member as an async task. It stops establishing consensus for new data items after
    /// reaching the threshold specified in [`Config::max_round`] or upon receiving a stop signal from `exit`.
    pub async fn run_session<
        N: Network<H, D, MK::Signature, MK::PartialMultisignature> + 'static,
    >(
        mut self,
        network: N,
        mut exit: oneshot::Receiver<()>,
    ) {
        let index = self.index();
        info!(target: "AlephBFT-member", "{:?} Spawning party for a session.", index);
        let config = self.config.clone();
        let sh = self.spawn_handle.clone();

        let (alert_messages_for_alerter, alert_messages_from_network) = mpsc::unbounded();
        let (alert_messages_for_network, alert_messages_from_alerter) = mpsc::unbounded();
        let (unit_messages_for_units, unit_messages_from_network) = mpsc::unbounded();
        let (unit_messages_for_network, unit_messages_from_units) = mpsc::unbounded();
        self.unit_messages_for_network = Some(unit_messages_for_network);
        let (network_exit, exit_stream) = oneshot::channel();
        info!(target: "AlephBFT-member", "{:?} Spawning network.", index);
        let network_handle = self
            .spawn_handle
            .spawn_essential("member/network", async move {
                NetworkHub::new(
                    network,
                    unit_messages_from_units,
                    unit_messages_for_units,
                    alert_messages_from_alerter,
                    alert_messages_for_alerter,
                )
                .run(exit_stream)
                .await
            })
            .then(|_| async { iter(repeat(())) })
            .flatten_stream();
        pin_mut!(network_handle);

        let runway_config = RunwayConfigBuilder::build(
            config,
            self.keybox,
            self.data_io.take().unwrap(),
            self.spawn_handle.clone(),
            alert_messages_for_network,
            alert_messages_from_network,
            unit_messages_from_network,
        );
        let (runway_exit, exit_stream) = oneshot::channel();
        let (unit_messages_for_network_proxy, mut unit_messages_from_units_proxy) =
            mpsc::unbounded();
        let runway = Runway::new(runway_config, move |unit_message| {
            unit_messages_for_network_proxy
                .unbounded_send(unit_message)
                .expect("proxy connection should be open");
        });
        let runway_handle = self
            .spawn_handle
            .spawn_essential("member/runway", async move {
                runway.run(exit_stream).await;
            })
            .then(|_| async { iter(repeat(())) })
            .flatten_stream();
        pin_mut!(runway_handle);

        let ticker_delay = self.config.delay_config.tick_interval;
        let mut ticker = Delay::new(ticker_delay).fuse();

        info!(target: "AlephBFT-member", "{:?} Start routing messages from consensus to network", index);
        loop {
            futures::select! {
                event = unit_messages_from_units_proxy.next() => match event {
                    Some((message, recipient)) => {
                        self.on_unit_message_from_units(message, recipient);
                    },
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Unit message stream from Runway closed.", index);
                        break;
                    },
                },

                _ = network_handle.next() => {
                    debug!(target: "AlephBFT-member", "{:?} network task terminated early.", index);
                    break;
                },

                _ = runway_handle.next() => {
                    debug!(target: "AlephBFT-member", "{:?} runway task terminated early.", index);
                    break;
                },

                _ = &mut ticker => {
                    self.trigger_tasks();
                    ticker = Delay::new(ticker_delay).fuse();
                },

                _ = &mut exit => break,
            }
        }
        info!(target: "AlephBFT-member", "{:?} Ending run.", index);

        let _ = runway_exit.send(());
        runway_handle.next().await.unwrap();

        let _ = network_exit.send(());
        network_handle.next().await.unwrap();

        info!(target: "AlephBFT-member", "{:?} Run ended.", index);
    }
}
