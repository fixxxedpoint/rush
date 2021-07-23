use codec::{Decode, Encode};
use futures::{
    channel::{mpsc, oneshot},
    FutureExt, StreamExt,
};
use log::{debug, error, info, trace, warn};
use rand::Rng;

use crate::{
    alerts,
    alerts::{Alert, AlertConfig, ForkProof, ForkingNotification},
    config::Config,
    consensus,
    network::{NetworkHub, Recipient},
    runway::{Runway, RunwayConfigBuilder},
    signed::{Signature, Signed},
    units::{
        ControlHash, FullUnit, PreUnit, SignedUnit, UncheckedSignedUnit, Unit, UnitCoord, UnitStore,
    },
    Data, DataIO, Hasher, Index, MultiKeychain, Network, NodeCount, NodeIndex, NodeMap,
    OrderedBatch, Sender, SpawnHandle,
};

use futures_timer::Delay;
use std::{cmp::Ordering, collections::BinaryHeap, fmt::Debug, time};

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
struct ScheduledTask<H: Hasher, D: Data, MK: MultiKeychain> {
    task: Task<'a, H, D, MK>,
    scheduled_time: time::Instant,
}

impl<'a, H: Hasher, D: Data, MK: MultiKeychain> ScheduledTask<'a, H, D, MK> {
    fn new(task: Task<'a, H, D, MK>, scheduled_time: time::Instant) -> Self {
        ScheduledTask {
            task,
            scheduled_time,
        }
    }
}

impl<'a, H: Hasher, D: Data, MK: MultiKeychain> Ord for ScheduledTask<'a, H, D, MK> {
    fn cmp(&self, other: &Self) -> Ordering {
        // we want earlier times to come first when used in max-heap, hence the below:
        other.scheduled_time.cmp(&self.scheduled_time)
    }
}

impl<'a, H: Hasher, D: Data, MK: MultiKeychain> PartialOrd for ScheduledTask<'a, H, D, MK> {
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
    DP: DataIO<D>,
    MK: MultiKeychain,
    SH: SpawnHandle,
{
    config: Config,
    tx_consensus: Option<Sender<NotificationIn<H>>>,
    data_io: DP,
    keybox: &'a MK,
    store: UnitStore<'a, H, D, MK>,
    requests: BinaryHeap<ScheduledTask<'a, H, D, MK>>,
    threshold: NodeCount,
    n_members: NodeCount,
    unit_messages_for_network: Option<Sender<(UnitMessage<H, D, MK::Signature>, Recipient)>>,
    alerts_for_alerter: Option<Sender<Alert<H, D, MK::Signature>>>,
    spawn_handle: SH,
    scheduled_units: Vec<UncheckedSignedUnit<H, D, MK::Signature>>,
}

impl<'a, H, D, DP, MK, SH> Member<'a, H, D, DP, MK, SH>
where
    H: Hasher,
    D: Data,
    DP: DataIO<D>,
    MK: MultiKeychain,
    SH: SpawnHandle,
{
    /// Create a new instance of the Member for a given session. Under the hood, the Member implementation
    /// makes an extensive use of asynchronous features of Rust, so creating a new Member doesn't start it.
    /// See [`Member::run_session`].
    pub fn new(data_io: DP, keybox: &'a MK, config: Config, spawn_handle: SH) -> Self {
        let n_members = config.n_members;
        let threshold = (n_members * 2) / 3 + NodeCount(1);
        let max_round = config.max_round;
        Member {
            config,
            tx_consensus: None,
            data_io,
            keybox,
            store: UnitStore::new(n_members, threshold, max_round),
            requests: BinaryHeap::new(),
            threshold,
            n_members,
            unit_messages_for_network: None,
            alerts_for_alerter: None,
            spawn_handle,
            scheduled_units: Vec::new(),
        }
    }

    fn send_consensus_notification(&mut self, notification: NotificationIn<H>) {
        self.tx_consensus
            .as_ref()
            .unwrap()
            .unbounded_send(notification)
            .expect("Channel to consensus should be open")
    }

    async fn on_create_new(&mut self, u: PreUnit<H>) {
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::UnitMulticast(hash, 0), curr_time);
        self.requests.push(task);
    }

    async fn on_create(&mut self, u: PreUnit<H>) {
        debug!(target: "AlephBFT-member", "{:?} On create notification.", self.index());
        let data = self.data_io.get_data();
        let full_unit = FullUnit::new(u, data, self.config.session_id);
        let hash = full_unit.hash();
        let signed_unit = Signed::sign(full_unit, self.keybox).await;
        self.store.add_unit(signed_unit, false);
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::UnitMulticast(hash, 0), curr_time);
        self.requests.push(task);
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
                Task::UnitMulticast(hash, multicast_number) => {
                    self.schedule_unit_multicast(hash, multicast_number, curr_time);
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
        hash: H::Hash,
        multicast_number: usize,
        curr_time: time::Instant,
    ) {
        let signed_unit = self
            .store
            .unit_by_hash(&hash)
            .cloned()
            .expect("Our units are in store.");
        let message = UnitMessage::<H, D, MK::Signature>::NewUnit(signed_unit.into());
        trace!(target: "AlephBFT-member", "{:?} Sending a unit {:?} over network {:?}th time.", self.index(), hash, multicast_number);
        self.broadcast_units(message);
        let delay = (self.config.delay_config.unit_broadcast_delay)(multicast_number);
        self.requests.push(ScheduledTask::new(
            Task::UnitMulticast(hash, multicast_number + 1),
            curr_time + delay,
        ));
    }

    fn on_missing_coords(&mut self, coords: Vec<UnitCoord>) {
        trace!(target: "AlephBFT-member", "{:?} Dealing with missing coords notification {:?}.", self.index(), coords);
        let curr_time = time::Instant::now();
        for coord in coords {
            if !self.store.contains_coord(&coord) {
                let task = ScheduledTask::new(Task::CoordRequest(coord), curr_time);
                self.requests.push(task);
            }
        }
        self.trigger_tasks();
    }

    fn on_wrong_control_hash(&mut self, u_hash: H::Hash) {
        trace!(target: "AlephBFT-member", "{:?} Dealing with wrong control hash notification {:?}.", self.index(), u_hash);
        if let Some(p_hashes) = self.store.get_parents(u_hash) {
            // We have the parents by some strange reason (someone sent us parents
            // without us requesting them).
            let p_hashes = p_hashes.clone();
            trace!(target: "AlephBFT-member", "{:?} We have the parents for {:?} even though we did not request them.", self.index(), u_hash);
            self.send_consensus_notification(NotificationIn::UnitParents(u_hash, p_hashes));
        } else {
            let curr_time = time::Instant::now();
            let task = ScheduledTask::new(Task::ParentsRequest(u_hash), curr_time);
            self.requests.push(task);
            self.trigger_tasks();
        }
    }

    async fn on_consensus_notification(&mut self, notification: NotificationOut<H>) {
        match notification {
            NotificationOut::CreatedPreUnit(pu) => {
                self.on_create(pu).await;
            }
            NotificationOut::MissingUnits(coords) => {
                self.on_missing_coords(coords);
            }
            NotificationOut::WrongControlHash(h) => {
                self.on_wrong_control_hash(h);
            }
            NotificationOut::AddedToDag(h, p_hashes) => {
                self.store.add_parents(h, p_hashes);
            }
        }
    }

    fn validate_unit_parents(&self, su: &SignedUnit<'a, H, D, MK>) -> bool {
        // NOTE: at this point we cannot validate correctness of the control hash, in principle it could be
        // just a random hash, but we still would not be able to deduce that by looking at the unit only.
        let pre_unit = su.as_signable().as_pre_unit();
        if pre_unit.n_members() != self.config.n_members {
            warn!(target: "AlephBFT-member", "{:?} Unit with wrong length of parents map.", self.index());
            return false;
        }
        let round = pre_unit.round();
        let n_parents = pre_unit.n_parents();
        if round == 0 && n_parents > NodeCount(0) {
            warn!(target: "AlephBFT-member", "{:?} Unit of round zero with non-zero number of parents.", self.index());
            return false;
        }
        let threshold = self.threshold;
        if round > 0 && n_parents < threshold {
            warn!(target: "AlephBFT-member", "{:?} Unit of non-zero round with only {:?} parents while at least {:?} are required.", self.index(), n_parents, threshold);
            return false;
        }
        let control_hash = &pre_unit.control_hash();
        if round > 0 && !control_hash.parents_mask[pre_unit.creator()] {
            warn!(target: "AlephBFT-member", "{:?} Unit does not have its creator's previous unit as parent.", self.index());
            return false;
        }
        true
    }

    // TODO: we should return an error and handle it outside
    fn validate_unit(
        &self,
        uu: UncheckedSignedUnit<H, D, MK::Signature>,
    ) -> Option<SignedUnit<'a, H, D, MK>> {
        let su = match uu.check(self.keybox) {
            Ok(su) => su,
            Err(uu) => {
                warn!(target: "AlephBFT-member", "{:?} Wrong signature received {:?}.", self.index(), &uu);
                return None;
            }
        };
        let full_unit = su.as_signable();
        if full_unit.session_id() != self.config.session_id {
            // NOTE: this implies malicious behavior as the unit's session_id
            // is incompatible with session_id of the message it arrived in.
            warn!(target: "AlephBFT-member", "{:?} A unit with incorrect session_id! {:?}", self.index(), full_unit);
            return None;
        }
        if full_unit.round() > self.store.limit_per_node() {
            warn!(target: "AlephBFT-member", "{:?} A unit with too high round {}! {:?}", self.index(), full_unit.round(), full_unit);
            return None;
        }
        if full_unit.creator().0 >= self.config.n_members.0 {
            warn!(target: "AlephBFT-member", "{:?} A unit with too high creator index {}! {:?}", self.index(), full_unit.creator().0, full_unit);
            return None;
        }
        if !self.validate_unit_parents(&su) {
            warn!(target: "AlephBFT-member", "{:?} A unit did not pass parents validation. {:?}", self.index(), full_unit);
            return None;
        }
        Some(su)
    }

    fn add_unit_to_store_unless_fork(&mut self, su: SignedUnit<'a, H, D, MK>) {
        let full_unit = su.as_signable();
        trace!(target: "AlephBFT-member", "{:?} Adding member unit to store {:?}", self.index(), full_unit);
        if self.store.is_forker(full_unit.creator()) {
            trace!(target: "AlephBFT-member", "{:?} Ignoring forker's unit {:?}", self.index(), full_unit);
            return;
        }
        if let Some(sv) = self.store.is_new_fork(full_unit) {
            let creator = full_unit.creator();
            if !self.store.is_forker(creator) {
                // We need to mark the forker if it is not known yet.
                let proof = (su.into(), sv.into());
                self.on_new_forker_detected(creator, proof);
            }
            // We ignore this unit. If it is legit, it will arrive in some alert and we need to wait anyway.
            // There is no point in keeping this unit in any kind of buffer.
            return;
        }
        let u_round = full_unit.round();
        let round_in_progress = self.store.get_round_in_progress();
        let rounds_margin = self.config.rounds_margin;
        if u_round <= round_in_progress + rounds_margin {
            self.store.add_unit(su, false);
        } else {
            warn!(target: "AlephBFT-member", "{:?} Unit {:?} ignored because of too high round {} when round in progress is {}.", self.index(), full_unit, u_round, round_in_progress);
        }
    }

    fn move_units_to_consensus(&mut self) {
        let units_to_move = self
            .store
            .yield_buffer_units()
            .into_iter()
            .map(|su| su.as_signable().unit())
            .collect();
        self.send_consensus_notification(NotificationIn::NewUnits(units_to_move))
    }

    fn on_unit_received(&mut self, uu: UncheckedSignedUnit<H, D, MK::Signature>, alert: bool) {
        if let Some(su) = self.validate_unit(uu) {
            if alert {
                // Units from alerts explicitly come from forkers, and we want them anyway.
                self.store.add_unit(su, true);
            } else {
                self.add_unit_to_store_unless_fork(su);
            }
        }
    }

    fn on_request_coord(&mut self, peer_id: NodeIndex, coord: UnitCoord) {
        debug!(target: "AlephBFT-member", "{:?} Received fetch request for coord {:?} from {:?}.", self.index(), coord, peer_id);
        let maybe_su = (self.store.unit_by_coord(coord)).cloned();

        if let Some(su) = maybe_su {
            trace!(target: "AlephBFT-member", "{:?} Answering fetch request for coord {:?} from {:?}.", self.index(), coord, peer_id);
            let message = UnitMessage::ResponseCoord(su.into());
            self.send_unit_message(message, peer_id);
        } else {
            trace!(target: "AlephBFT-member", "{:?} Not answering fetch request for coord {:?}. Unit not in store.", self.index(), coord);
        }
    }

    fn on_request_parents(&mut self, peer_id: NodeIndex, u_hash: H::Hash) {
        debug!(target: "AlephBFT-member", "{:?} Received parents request for hash {:?} from {:?}.", self.index(), u_hash, peer_id);
        let maybe_p_hashes = self.store.get_parents(u_hash);

        if let Some(p_hashes) = maybe_p_hashes {
            let p_hashes = p_hashes.clone();
            trace!(target: "AlephBFT-member", "{:?} Answering parents request for hash {:?} from {:?}.", self.index(), u_hash, peer_id);
            let mut full_units = Vec::new();
            for hash in p_hashes.iter() {
                if let Some(fu) = self.store.unit_by_hash(hash) {
                    full_units.push(fu.clone().into());
                } else {
                    debug!(target: "AlephBFT-member", "{:?} Not answering parents request, one of the parents missing from store.", self.index());
                    //This can happen if we got a parents response from someone, but one of the units was a fork and we dropped it.
                    //Either this parent is legit and we will soon get it in alert or the parent is not legit in which case
                    //the unit u, whose parents are beeing seeked here is not legit either.
                    //In any case, if a node added u to its Dag, then it should never reach this place in code when answering
                    //a parents request (as all the parents must be legit an thus must be in store).
                    return;
                }
            }
            let message = UnitMessage::ResponseParents(u_hash, full_units);
            self.send_unit_message(message, peer_id);
        } else {
            trace!(target: "AlephBFT-member", "{:?} Not answering parents request for hash {:?}. Unit not in DAG yet.", self.index(), u_hash);
        }
    }

    fn on_parents_response(
        &mut self,
        u_hash: H::Hash,
        parents: Vec<UncheckedSignedUnit<H, D, MK::Signature>>,
    ) {
        if self.store.get_parents(u_hash).is_some() {
            trace!(target: "AlephBFT-member", "{:?} We got parents response but already know the parents.", self.index());
            return;
        }
        let (u_round, u_control_hash, parent_ids) = match self.store.unit_by_hash(&u_hash) {
            Some(su) => {
                let full_unit = su.as_signable();
                let parent_ids: Vec<_> = full_unit.control_hash().parents().collect();
                (
                    full_unit.round(),
                    full_unit.control_hash().combined_hash,
                    parent_ids,
                )
            }
            None => {
                trace!(target: "AlephBFT-member", "{:?} We got parents but don't even know the unit. Ignoring.", self.index());
                return;
            }
        };

        if parent_ids.len() != parents.len() {
            warn!(target: "AlephBFT-member", "{:?} In received parent response expected {} parents got {} for unit {:?}.", self.index(), parents.len(), parent_ids.len(), u_hash);
            return;
        }

        let mut p_hashes_node_map: NodeMap<Option<H::Hash>> =
            NodeMap::new_with_len(self.config.n_members);
        for (i, uu) in parents.into_iter().enumerate() {
            let su = match self.validate_unit(uu) {
                None => {
                    warn!(target: "AlephBFT-member", "{:?} In received parent response received a unit that does not pass validation.", self.index());
                    return;
                }
                Some(su) => su,
            };
            let full_unit = su.as_signable();
            if full_unit.round() + 1 != u_round {
                warn!(target: "AlephBFT-member", "{:?} In received parent response received a unit with wrong round.", self.index());
                return;
            }
            if full_unit.creator() != parent_ids[i] {
                warn!(target: "AlephBFT-member", "{:?} In received parent response received a unit with wrong creator.", self.index());
                return;
            }
            let p_hash = full_unit.hash();
            let ix = full_unit.creator();
            p_hashes_node_map[ix] = Some(p_hash);
            // There might be some optimization possible here to not validate twice, but overall
            // this piece of code should be executed extremely rarely.
            self.add_unit_to_store_unless_fork(su);
        }

        if ControlHash::<H>::combine_hashes(&p_hashes_node_map) != u_control_hash {
            warn!(target: "AlephBFT-member", "{:?} In received parent response the control hash is incorrect {:?}.", self.index(), p_hashes_node_map);
            return;
        }
        let p_hashes: Vec<H::Hash> = p_hashes_node_map.into_iter().flatten().collect();
        self.store.add_parents(u_hash, p_hashes.clone());
        trace!(target: "AlephBFT-member", "{:?} Succesful parents reponse for {:?}.", self.index(), u_hash);
        self.send_consensus_notification(NotificationIn::UnitParents(u_hash, p_hashes));
    }

    fn form_alert(
        &self,
        proof: ForkProof<H, D, MK::Signature>,
        units: Vec<SignedUnit<'a, H, D, MK>>,
    ) -> Alert<H, D, MK::Signature> {
        Alert::new(
            self.config.node_ix,
            proof,
            units.into_iter().map(|signed| signed.into()).collect(),
        )
    }

    fn on_new_forker_detected(&mut self, forker: NodeIndex, proof: ForkProof<H, D, MK::Signature>) {
        let max_units_alert = self.config.max_units_per_alert;
        let mut alerted_units = self.store.mark_forker(forker);
        if alerted_units.len() > max_units_alert {
            // The ordering is increasing w.r.t. rounds.
            alerted_units.reverse();
            alerted_units.truncate(max_units_alert);
            alerted_units.reverse();
        }
        let alert = self.form_alert(proof, alerted_units);
        self.alerts_for_alerter
            .as_ref()
            .unwrap()
            .unbounded_send(alert)
            .expect("Channel to alerter should be open")
    }

    fn on_alert_notification(&mut self, notification: ForkingNotification<H, D, MK::Signature>) {
        use ForkingNotification::*;
        match notification {
            Forker(proof) => {
                let forker = proof.0.index();
                if !self.store.is_forker(forker) {
                    self.on_new_forker_detected(forker, proof);
                }
            }
            Units(units) => {
                for uu in units {
                    self.on_unit_received(uu, true);
                }
            }
        }
    }

    fn on_unit_message(&mut self, message: UnitMessage<H, D, MK::Signature>) {
        use UnitMessage::*;
        match message {
            NewUnit(u) => {
                trace!(target: "AlephBFT-member", "{:?} New unit received {:?}.", self.index(), &u);
                self.on_unit_received(u, false);
            }
            RequestCoord(peer_id, coord) => {
                self.on_request_coord(peer_id, coord);
            }
            ResponseCoord(u) => {
                trace!(target: "AlephBFT-member", "{:?} Fetch response received {:?}.", self.index(), &u);
                self.on_unit_received(u, false);
            }
            RequestParents(peer_id, u_hash) => {
                trace!(target: "AlephBFT-member", "{:?} Parents request received {:?}.", self.index(), u_hash);
                self.on_request_parents(peer_id, u_hash);
            }
            ResponseParents(u_hash, parents) => {
                trace!(target: "AlephBFT-member", "{:?} Response parents received {:?}.", self.index(), u_hash);
                self.on_parents_response(u_hash, parents);
            }
        }
    }

    fn on_ordered_batch(&mut self, batch: Vec<H::Hash>) {
        let batch = batch
            .iter()
            .map(|h| {
                self.store
                    .unit_by_hash(h)
                    .expect("Ordered units must be in store")
                    .as_signable()
                    .data()
                    .clone()
            })
            .collect::<OrderedBatch<D>>();
        if let Err(e) = self.data_io.send_ordered_batch(batch) {
            error!(target: "AlephBFT-member", "{:?} Error when sending batch {:?}.", self.index(), e);
        }
    }

    fn on_unit_message_from_units(
        &mut self,
        message: UnitMessage<H, D, MK::Signature>,
        recipient: Recipient,
    ) {
        match message {
            UnitMessage::NewUnit(_) => todo!(),
            UnitMessage::RequestCoord(_, _) => todo!(),
            UnitMessage::ResponseCoord(_) => todo!(),
            UnitMessage::RequestParents(_, _) => todo!(),
            UnitMessage::ResponseParents(_, _) => todo!(),
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
        info!(target: "AlephBFT-member", "{:?} Spawning party for a session.", self.index());
        let config = self.config.clone();
        let sh = self.spawn_handle.clone();

        let (alert_messages_for_alerter, alert_messages_from_network) = mpsc::unbounded();
        let (alert_messages_for_network, alert_messages_from_alerter) = mpsc::unbounded();
        let (unit_messages_for_units, mut unit_messages_from_network) = mpsc::unbounded();
        let (unit_messages_for_network, unit_messages_from_units) = mpsc::unbounded();
        let (unit_messages_for_network_proxy, unit_messages_from_units_proxy) = mpsc::unbounded();
        let (network_exit, exit_stream) = oneshot::channel();
        info!(target: "AlephBFT-member", "{:?} Spawning network.", self.index());
        let mut network_handle = self
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
            .fuse();

        let runway_config = RunwayConfigBuilder::build(
            config,
            self.keybox,
            self.data_io,
            self.spawn_handle,
            alert_messages_for_network,
            alert_messages_from_network,
            unit_messages_for_network_proxy,
            unit_messages_from_network,
        );
        let runway = Runway::new(runway_config, |unit_message| {});

        let ticker_delay = self.config.delay_config.tick_interval;
        let mut ticker = Delay::new(ticker_delay).fuse();

        let mut consensus_exited = false;
        let mut network_exited = false;
        let mut alerter_exited = false;
        info!(target: "AlephBFT-member", "{:?} Start routing messages from consensus to network", self.index());
        loop {
            futures::select! {
                event = unit_messages_from_units_proxy.next() => match event {
                    Some((message, recipient)) => {
                        self.on_unit_message_from_units(message, recipient);
                    },
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Unit message stream from Runway closed.", self.index());
                        break;
                    },
                },

                notification = rx_consensus.next() => match notification {
                        Some(notification) => self.on_consensus_notification(notification).await,
                        None => {
                            error!(target: "AlephBFT-member", "{:?} Consensus notification stream closed.", self.index());
                            break;
                        }
                },

                notification = notifications_from_alerter.next() => match notification {
                    Some(notification) => self.on_alert_notification(notification),
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Alert notification stream closed.", self.index());
                        break;
                    }
                },

                event = unit_messages_from_network.next() => match event {
                    Some(event) => self.on_unit_message(event),
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Unit message stream closed.", self.index());
                        break;
                    }
                },

                batch = ordered_batch_rx.next() => match batch {
                    Some(batch) => self.on_ordered_batch(batch),
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Ordered batch stream closed.", self.index());
                        break;
                    }
                },
                _ = &mut ticker => {
                    self.trigger_tasks();
                    ticker = Delay::new(ticker_delay).fuse();
                },
                _ = &mut consensus_handle => {
                    consensus_exited = true;
                    debug!(target: "AlephBFT-member", "{:?} consensus task terminated early.", self.index());
                    break;
                },
                _ = &mut network_handle => {
                    network_exited = true;
                    debug!(target: "AlephBFT-member", "{:?} network task terminated early.", self.index());
                    break;
                },
                _ = &mut alerts_handle => {
                    alerter_exited = true;
                    debug!(target: "AlephBFT-member", "{:?} alerts task terminated early.", self.index());
                    break;
                },
                _ = &mut exit => break,
            }
            self.move_units_to_consensus();
        }
        info!(target: "AlephBFT-member", "{:?} Ending run.", self.index());

        let _ = consensus_exit.send(());
        if !consensus_exited {
            consensus_handle.await.unwrap();
        }
        let _ = alerter_exit.send(());
        if !alerter_exited {
            alerts_handle.await.unwrap();
        }
        let _ = network_exit.send(());
        if !network_exited {
            network_handle.await.unwrap();
        }
    }
}
