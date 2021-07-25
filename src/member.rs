use crate::{
    config::Config,
    network::{NetworkHub, Recipient},
    runway::Runway,
    signed::Signature,
    units::{PreUnit, UncheckedSignedUnit, Unit, UnitCoord, UnitStore},
    Data, DataIO, Hasher, MultiKeychain, Network, NodeCount, NodeIndex, Sender, SpawnHandle,
};
use codec::{Decode, Encode};
use futures::{
    channel::{
        mpsc::{self, unbounded},
        oneshot,
    },
    future::ready,
    stream::iter,
    Future, FutureExt, Stream, StreamExt,
};
use futures_timer::Delay;
use log::{debug, error, info, trace, warn};
use rand::Rng;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
    fmt::Debug,
    iter::repeat,
    time,
};

pub(crate) fn into_infinite_stream<F: Future>(f: F) -> impl Stream<Item = ()> {
    f.then(|_| ready(iter(repeat(())))).flatten_stream()
}

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
    data_io: Option<DP>,
    keybox: &'a MK,
    requests: BinaryHeap<ScheduledTask<H>>,
    n_members: NodeCount,
    unit_messages_for_network: Sender<(UnitMessage<H, D, MK::Signature>, Recipient)>,
    spawn_handle: SH,
    scheduled_units: Vec<UncheckedSignedUnit<H, D, MK::Signature>>,
    coord_requests: HashSet<UnitCoord>,
    parents_requests: HashSet<H::Hash>,
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
            data_io: Some(data_io),
            keybox,
            requests: BinaryHeap::new(),
            n_members,
            unit_messages_for_network: unbounded().0,
            spawn_handle,
            scheduled_units: Vec::new(),
            coord_requests: HashSet::new(),
            parents_requests: HashSet::new(),
        }
    }

    fn on_create(&mut self, u: UncheckedSignedUnit<H, D, MK::Signature>) {
        let index = self.scheduled_units.len();
        self.scheduled_units.push(u);
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::UnitMulticast(index, 0), curr_time);
        self.requests.push(task);
    }

    fn on_request_coord(&mut self, coord: UnitCoord) {
        trace!(target: "AlephBFT-member", "{:?} Dealing with missing coord notification {:?}.", self.index(), coord);
        self.coord_requests.insert(coord);
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::CoordRequest(coord), curr_time);
        self.requests.push(task);
        self.trigger_tasks();
    }

    fn on_request_parents(&mut self, u_hash: H::Hash) {
        self.parents_requests.insert(u_hash);
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
        self.config.node_ix
    }

    fn send_unit_message(&mut self, message: UnitMessage<H, D, MK::Signature>, peer_id: NodeIndex) {
        self.unit_messages_for_network
            .unbounded_send((message, Recipient::Node(peer_id)))
            .expect("Channel to network should be open")
    }

    fn broadcast_units(&mut self, message: UnitMessage<H, D, MK::Signature>) {
        self.unit_messages_for_network
            .unbounded_send((message, Recipient::Everyone))
            .expect("Channel to network should be open")
    }

    fn schedule_parents_request(&mut self, u_hash: H::Hash, curr_time: time::Instant) {
        if self.parents_requests.contains(&u_hash) {
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
        // If we already received or never asked for such coord then there is no need to request it.
        // It will be sent to consensus soon (or have already been sent).
        if !self.coord_requests.contains(&coord) {
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
            UnitMessage::RequestCoord(_, coord) => self.on_request_coord(coord),
            UnitMessage::ResponseCoord(_) => match recipient {
                Some(Recipient::Node(peer_id)) => {
                    self.send_unit_message(message, peer_id);
                }
                _ => {
                    warn!(target: "AlephBFT-member", "{:?} Missing Recipient for a ResponseCoord message.", self.index());
                }
            },
            UnitMessage::RequestParents(_, hash) => self.on_request_parents(hash),
            UnitMessage::ResponseParents(_, _) => match recipient {
                Some(Recipient::Node(peer_id)) => {
                    self.send_unit_message(message, peer_id);
                }
                _ => {
                    warn!(target: "AlephBFT-member", "{:?} Missing Recipient for a ResponseParents message.", self.index());
                }
            },
        }
    }

    fn on_unit_message_from_network(
        &mut self,
        message: UnitMessage<H, D, MK::Signature>,
        proxy: &mut Sender<UnitMessage<H, D, MK::Signature>>,
    ) {
        let mut requested = false;
        match message {
            UnitMessage::ResponseCoord(ref unit) => {
                // TODO refactor to a separate method
                // stop sending requests for which we received some answer
                // if response is invalid, we will receive another request for it
                requested = self.coord_requests.remove(&unit.as_signable().coord());
            }
            UnitMessage::ResponseParents(ref hash, _) => {
                // TODO refactor to a separate method
                // stop sending requests for which we received some answer
                // if response is invalid, we will receive another request for it
                requested = self.parents_requests.remove(hash);
            }
            _ => {}
        }
        // if requested {
        //     proxy
        //         .unbounded_send(message)
        //         .expect("proxy should not be closed");
        // }
        proxy
            .unbounded_send(message)
            .expect("proxy should not be closed");
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

        let (alert_messages_for_alerter, alert_messages_from_network) = mpsc::unbounded();
        let (alert_messages_for_network, alert_messages_from_alerter) = mpsc::unbounded();
        let (unit_messages_for_units, mut unit_messages_from_network) = mpsc::unbounded();
        let (unit_messages_for_network, unit_messages_from_units) = mpsc::unbounded();
        self.unit_messages_for_network = unit_messages_for_network;
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
            });
        let mut network_handle = into_infinite_stream(network_handle).fuse();

        let (runway_exit, exit_stream) = oneshot::channel();
        let (unit_messages_for_network_proxy, mut unit_messages_from_units_proxy) =
            mpsc::unbounded();
        let (mut unit_messages_for_units_proxy, unit_messages_from_network_proxy) =
            mpsc::unbounded();
        let runway = Runway::new(
            config,
            self.keybox,
            self.data_io.take().unwrap(),
            self.spawn_handle.clone(),
            alert_messages_for_network,
            alert_messages_from_network,
            unit_messages_from_network_proxy,
            move |unit_message| {
                unit_messages_for_network_proxy
                    .unbounded_send(unit_message)
                    .expect("proxy connection should be open");
            },
        );
        let runway_handle = self
            .spawn_handle
            .spawn_essential("member/runway", async move {
                runway.run(exit_stream).await;
            });
        let mut runway_handle = into_infinite_stream(runway_handle).fuse();

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

                event = unit_messages_from_network.next() => match event {
                    Some(message) => {
                        todo!("process all responses");
                        self.on_unit_message_from_network(message, &mut unit_messages_for_units_proxy);
                    },
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Unit message stream from network closed.", index);
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
