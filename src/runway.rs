use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};

use crate::{
    alerts::{Alert, AlertConfig, AlertMessage, Alerter, ForkProof, ForkingNotification},
    consensus::Consensus,
    member::{into_infinite_stream, NotificationIn, NotificationOut, UnitMessage},
    network::Recipient,
    nodes::NodeMap,
    units::{
        ControlHash, FullUnit, PreUnit, SignedUnit, UncheckedSignedUnit, UnitCoord, UnitStore,
    },
    Config, Data, DataIO, Hasher, Index, MultiKeychain, NodeCount, NodeIndex, OrderedBatch,
    Receiver, Sender, Signed, SpawnHandle, TaskHandle,
};
use futures::{
    channel::{mpsc, oneshot},
    StreamExt,
};
use log::{debug, error, info, trace, warn};
use tokio::sync::Barrier;

pub(crate) enum OutgoingMessage<M> {
    WithTrackedRequest(TrackedRequest, M),
    Raw(M),
}

pub(crate) struct RunwayFacade<H, D, MK>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
{
    // runway: Runway<'a, H, D, MK, DP, SH>,
    runway_handle: TaskHandle,
    runway_exit: oneshot::Sender<()>,
    outgoing_messages: Receiver<OutgoingMessage<(UnitMessage<H, D, MK::Signature>, Recipient)>>,
    incoming_messages: Sender<UnitMessage<H, D, MK::Signature>>,
    missing_coords: HashMap<UnitCoord, TrackedRequest>,
    missing_parents: HashMap<H::Hash, TrackedRequest>,
}

impl<H, D, MK> RunwayFacade<H, D, MK>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
{
    fn new(
        runway_handle: TaskHandle,
        runway_exit: oneshot::Sender<()>,
        outgoing_messages: Receiver<OutgoingMessage<(UnitMessage<H, D, MK::Signature>, Recipient)>>,
        incoming_messages: Sender<UnitMessage<H, D, MK::Signature>>,
    ) -> Self {
        RunwayFacade {
            runway_handle,
            runway_exit,
            outgoing_messages,
            incoming_messages,
            missing_coords: HashMap::new(),
            missing_parents: HashMap::new(),
        }
    }

    // pub(crate) fn new(
    //     config: Config,
    //     keychain: &'a MK,
    //     data_io: DP,
    //     spawn_handle: SH,
    //     alert_messages_for_network: Sender<(
    //         AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>,
    //         Recipient,
    //     )>,
    //     alert_messages_from_network: Receiver<
    //         AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>,
    //     >,
    // ) -> RunwayFacade<'a, H, D, MK, DP, SH> {
    //     let (unit_messages_for_units, unit_messages_from_network) = mpsc::unbounded();
    //     let (unit_messages_for_network, unit_messages_from_units) = mpsc::unbounded();
    //     let runway = InitializedRunway::new(
    //         config,
    //         keychain,
    //         data_io,
    //         spawn_handle,
    //         alert_messages_for_network,
    //         alert_messages_from_network,
    //         unit_messages_from_network,
    //         unit_messages_for_network,
    //     );
    //     RunwayFacade {
    //         runway,
    //         outgoing_messages: unit_messages_from_units,
    //         incoming_messages: unit_messages_for_units,
    //         store: runway.store.clone(),
    //     }
    // }

    pub(crate) fn enqueue_message(&mut self, message: UnitMessage<H, D, MK::Signature>) {
        self.incoming_messages
            .unbounded_send(message)
            .expect("incoming_messages channel should be open")
    }

    pub(crate) async fn next_outgoing_message(
        &mut self,
    ) -> Option<(UnitMessage<H, D, MK::Signature>, Recipient)> {
        let out_msg = self.outgoing_messages.next().await;
        match out_msg {
            Some(out_msg) => match out_msg {
                OutgoingMessage::WithTrackedRequest(request, out_msg) => {
                    match out_msg.0 {
                        UnitMessage::RequestCoord(_, ref coord) => {
                            self.missing_coords.insert(*coord, request);
                        }
                        UnitMessage::RequestParents(_, ref u_hash) => {
                            self.missing_parents.insert(*u_hash, request);
                        }
                        _ => {}
                    }
                    Some(out_msg)
                }
                OutgoingMessage::Raw(out_msg) => Some(out_msg),
            },
            None => None,
        }
    }

    pub(crate) fn missing_parents(&mut self, u_hash: &H::Hash) -> bool {
        match self.missing_parents.get(u_hash) {
            Some(r) => {
                if r.is_satisfied() {
                    self.missing_parents.remove(u_hash);
                    false
                } else {
                    true
                }
            }
            None => true,
        }
    }

    pub(crate) fn missing_coords(&mut self, coord: &UnitCoord) -> bool {
        match self.missing_coords.get(coord) {
            Some(r) => {
                if r.is_satisfied() {
                    self.missing_coords.remove(coord);
                    false
                } else {
                    true
                }
            }
            None => true,
        }
    }

    pub(crate) async fn stop(self) {
        if self.runway_exit.send(()).is_err() {
            warn!(target: "AlephBFT-runway", "runway already stopped");
        }
        self.runway_handle.await;
    }
}

pub(crate) struct InitializedRunway<'a, H, D, MK, DP, SH>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    DP: DataIO<D>,
    SH: SpawnHandle,
{
    runway: Runway<'a, H, D, MK, DP, SH>,
    alerter: Alerter<'a, H, D, MK>,
    consensus: Consensus<H, SH>,
    outgoing_messages: Receiver<OutgoingMessage<(UnitMessage<H, D, MK::Signature>, Recipient)>>,
    incoming_messages: Sender<UnitMessage<H, D, MK::Signature>>,
}

impl<'a, H, D, MK, DP, SH> InitializedRunway<'a, H, D, MK, DP, SH>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    DP: DataIO<D>,
    SH: SpawnHandle,
{
    pub(crate) fn new(
        config: Config,
        keychain: &'a MK,
        data_io: DP,
        spawn_handle: SH,
        alert_messages_for_network: Sender<(
            AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>,
            Recipient,
        )>,
        alert_messages_from_network: Receiver<
            AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>,
        >,
    ) -> Self {
        let (tx_consensus, consensus_stream) = mpsc::unbounded();
        let (consensus_sink, rx_consensus) = mpsc::unbounded();
        let (ordered_batch_tx, ordered_batch_rx) = mpsc::unbounded();
        let (unit_messages_for_network, unit_messages_from_units) = mpsc::unbounded();
        let (unit_messages_for_units, unit_messages_from_network) = mpsc::unbounded();

        let (alert_notifications_for_units, notifications_from_alerter) = mpsc::unbounded();
        let (alerts_for_alerter, alerts_from_units) = mpsc::unbounded();
        let alert_config = AlertConfig {
            session_id: config.session_id,
            max_units_per_alert: config.max_units_per_alert,
            n_members: config.n_members,
        };
        let alerter = Alerter::new(
            keychain,
            alert_messages_for_network,
            alert_messages_from_network,
            alert_notifications_for_units,
            alerts_from_units,
            alert_config,
        );

        let consensus = Consensus::new(
            config.clone(),
            spawn_handle.clone(),
            consensus_stream,
            consensus_sink,
            ordered_batch_tx,
        );

        let n_members = config.n_members;
        let threshold = (n_members * 2) / 3 + NodeCount(1);
        let max_round = config.max_round;
        let store = UnitStore::new(n_members, threshold, max_round);

        let runway = Runway {
            config,
            store,
            keybox: keychain,
            alerts_for_alerter,
            notifications_from_alerter,
            tx_consensus,
            rx_consensus,
            data_io,
            unit_messages_from_network,
            unit_messages_for_network,
            spawn_handle,
            ordered_batch_rx,
            request_tracker: RequestTracker::new(),
        };
        InitializedRunway {
            runway,
            alerter,
            consensus,
            outgoing_messages: unit_messages_from_units,
            incoming_messages: unit_messages_for_units,
        }
    }
}

impl<H, D, MK, DP, SH> InitializedRunway<'static, H, D, MK, DP, SH>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    DP: DataIO<D> + Send + 'static,
    SH: SpawnHandle + Send + 'static,
{
    pub(crate) fn start(self) -> RunwayFacade<H, D, MK> {
        let (runway_exit, exit_stream) = oneshot::channel();
        let runway = self.runway;
        let spawn_handle = runway.spawn_handle.clone();
        let alerter = self.alerter;
        let consensus = self.consensus;
        let runway_handle = spawn_handle.spawn_essential("runway", async move {
            runway.run(exit_stream, alerter, consensus).await
        });
        let outgoing_messages = self.outgoing_messages;
        let incoming_messages = self.incoming_messages;
        RunwayFacade::new(
            runway_handle,
            runway_exit,
            outgoing_messages,
            incoming_messages,
        )
    }

    // pub(crate) async fn run(mut self, mut exit: oneshot::Receiver<()>) {
    //     todo!("start a new parallel task and return a handle to it");
    //     self.runway.run(exit, self.alerter, self.consensus).await
    // }
}

pub(crate) struct Runway<'a, H, D, MK, DP, SH>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    DP: DataIO<D>,
    SH: SpawnHandle,
{
    config: Config,
    store: UnitStore<H, D, MK>,
    keybox: &'a MK,
    alerts_for_alerter: Sender<Alert<H, D, MK::Signature>>,
    notifications_from_alerter: Receiver<ForkingNotification<H, D, MK::Signature>>,
    unit_messages_from_network: Receiver<UnitMessage<H, D, MK::Signature>>,
    unit_messages_for_network:
        Sender<OutgoingMessage<(UnitMessage<H, D, MK::Signature>, Recipient)>>,
    tx_consensus: Sender<NotificationIn<H>>,
    rx_consensus: Receiver<NotificationOut<H>>,
    ordered_batch_rx: Receiver<Vec<H::Hash>>,
    data_io: DP,
    spawn_handle: SH,
    request_tracker: RequestTracker<H>,
}

impl<'a, H, D, MK, DP, SH> Runway<'a, H, D, MK, DP, SH>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    DP: DataIO<D>,
    SH: SpawnHandle,
{
    fn index(&self) -> NodeIndex {
        self.config.node_ix
    }

    fn on_unit_message(&mut self, message: UnitMessage<H, D, MK::Signature>) {
        use UnitMessage::*;
        match message {
            NewUnit(u) => {
                trace!(target: "AlephBFT-runway", "{:?} New unit received {:?}.", self.index(), &u);
                if self.on_unit_received(u, false).is_err() {
                    debug!(target: "AlephBFT-runway", "{:?} Failed to process a received unit.", self.index());
                }
            }
            RequestCoord(peer_id, coord) => {
                self.on_request_coord(peer_id, coord);
            }
            ResponseCoord(u) => {
                trace!(target: "AlephBFT-runway", "{:?} Fetch response received {:?}.", self.index(), &u);
                // todo!("request again if invalid");
                let coord = u.as_signable().coord();
                if self.on_unit_received(u, false).is_err() {
                    self.on_missing_coords([coord].into())
                }
            }
            RequestParents(peer_id, u_hash) => {
                trace!(target: "AlephBFT-runway", "{:?} Parents request received {:?}.", self.index(), u_hash);
                self.on_request_parents(peer_id, u_hash);
            }
            ResponseParents(u_hash, parents) => {
                trace!(target: "AlephBFT-runway", "{:?} Response parents received {:?}.", self.index(), u_hash);
                // todo!("request again if invalid");
                if self.on_parents_response(u_hash, parents).is_err() {
                    let message =
                        UnitMessage::<H, D, MK::Signature>::RequestParents(self.index(), u_hash);
                    self.unit_messages_for_network.unbounded_send(
                        OutgoingMessage::WithTrackedRequest(
                            self.request_tracker.new_missing_parents_request(&u_hash),
                            (message, Recipient::Everyone),
                        ),
                    );
                }
            }
        }
    }

    fn on_unit_received(
        &mut self,
        uu: UncheckedSignedUnit<H, D, MK::Signature>,
        alert: bool,
    ) -> Result<(), ()> {
        // todo!();
        if self.store.contains_coord(&uu.as_signable().coord()) {
            return Ok(());
        }
        if let Some(su) = self.validate_unit(uu) {
            if alert {
                // Units from alerts explicitly come from forkers, and we want them anyway.
                self.request_tracker
                    .coordinates_resolved(&su.as_signable().coord());
                self.store.add_unit(su, true);
            } else {
                self.add_unit_to_store_unless_fork(su);
            }
            return Ok(());
        } else {
            return Err(());
        }
    }

    // TODO: we should return an error and handle it outside
    fn validate_unit(
        &self,
        uu: UncheckedSignedUnit<H, D, MK::Signature>,
    ) -> Option<SignedUnit<H, D, MK>> {
        let su = match uu.check(self.keybox) {
            Ok(su) => su,
            Err(uu) => {
                warn!(target: "AlephBFT-runway", "{:?} Wrong signature received {:?}.", self.index(), &uu);
                return None;
            }
        };
        let full_unit = su.as_signable();
        if full_unit.session_id() != self.config.session_id {
            // NOTE: this implies malicious behavior as the unit's session_id
            // is incompatible with session_id of the message it arrived in.
            warn!(target: "AlephBFT-runway", "{:?} A unit with incorrect session_id! {:?}", self.index(), full_unit);
            return None;
        }
        if full_unit.round() > self.store.limit_per_node() {
            warn!(target: "AlephBFT-runway", "{:?} A unit with too high round {}! {:?}", self.index(), full_unit.round(), full_unit);
            return None;
        }
        if full_unit.creator().0 >= self.config.n_members.0 {
            warn!(target: "AlephBFT-runway", "{:?} A unit with too high creator index {}! {:?}", self.index(), full_unit.creator().0, full_unit);
            return None;
        }
        if !self.validate_unit_parents(&su) {
            warn!(target: "AlephBFT-runway", "{:?} A unit did not pass parents validation. {:?}", self.index(), full_unit);
            return None;
        }
        Some(su)
    }

    fn add_unit_to_store_unless_fork(&mut self, su: SignedUnit<H, D, MK>) {
        let full_unit = su.as_signable();
        trace!(target: "AlephBFT-runway", "{:?} Adding member unit to store {:?}", self.index(), full_unit);
        if self.store.is_forker(full_unit.creator()) {
            trace!(target: "AlephBFT-runway", "{:?} Ignoring forker's unit {:?}", self.index(), full_unit);
            return;
        }
        let sv = self.store.is_new_fork(full_unit);
        if let Some(sv) = sv {
            let creator = full_unit.creator();
            let is_forker = self.store.is_forker(creator);
            if !is_forker {
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
            self.request_tracker
                .coordinates_resolved(&su.as_signable().coord());
            self.store.add_unit(su, false);
        } else {
            warn!(target: "AlephBFT-runway", "{:?} Unit {:?} ignored because of too high round {} when round in progress is {}.", self.index(), full_unit, u_round, round_in_progress);
        }
    }

    fn validate_unit_parents(&self, su: &SignedUnit<H, D, MK>) -> bool {
        // NOTE: at this point we cannot validate correctness of the control hash, in principle it could be
        // just a random hash, but we still would not be able to deduce that by looking at the unit only.
        let pre_unit = su.as_signable().as_pre_unit();
        if pre_unit.n_members() != self.config.n_members {
            warn!(target: "AlephBFT-runway", "{:?} Unit with wrong length of parents map.", self.index());
            return false;
        }
        let round = pre_unit.round();
        let n_parents = pre_unit.n_parents();
        if round == 0 && n_parents > NodeCount(0) {
            warn!(target: "AlephBFT-runway", "{:?} Unit of round zero with non-zero number of parents.", self.index());
            return false;
        }
        let threshold = self.threshold();
        if round > 0 && n_parents < threshold {
            warn!(target: "AlephBFT-runway", "{:?} Unit of non-zero round with only {:?} parents while at least {:?} are required.", self.index(), n_parents, threshold);
            return false;
        }
        let control_hash = &pre_unit.control_hash();
        if round > 0 && !control_hash.parents_mask[pre_unit.creator()] {
            warn!(target: "AlephBFT-runway", "{:?} Unit does not have its creator's previous unit as parent.", self.index());
            return false;
        }
        true
    }

    fn threshold(&self) -> NodeCount {
        (self.config.n_members * 2) / 3 + NodeCount(1)
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
            .unbounded_send(alert)
            .expect("Channel to alerter should be open")
    }

    fn form_alert(
        &self,
        proof: ForkProof<H, D, MK::Signature>,
        units: Vec<SignedUnit<H, D, MK>>,
    ) -> Alert<H, D, MK::Signature> {
        Alert::new(
            self.config.node_ix,
            proof,
            units.into_iter().map(|signed| signed.into()).collect(),
        )
    }

    fn on_request_coord(&mut self, peer_id: NodeIndex, coord: UnitCoord) {
        debug!(target: "AlephBFT-runway", "{:?} Received fetch request for coord {:?} from {:?}.", self.index(), coord, peer_id);
        let maybe_su = (self.store.unit_by_coord(coord)).cloned();

        if let Some(su) = maybe_su {
            trace!(target: "AlephBFT-runway", "{:?} Answering fetch request for coord {:?} from {:?}.", self.index(), coord, peer_id);
            let message = UnitMessage::ResponseCoord(su.into());
            self.send_unit_message(message, peer_id);
        } else {
            trace!(target: "AlephBFT-runway", "{:?} Not answering fetch request for coord {:?}. Unit not in store.", self.index(), coord);
        }
    }

    fn send_unit_message(&mut self, message: UnitMessage<H, D, MK::Signature>, peer_id: NodeIndex) {
        self.unit_messages_for_network
            .unbounded_send(OutgoingMessage::Raw((message, Recipient::Node(peer_id))))
            .expect("network's channel should be open")
    }

    fn on_request_parents(&mut self, peer_id: NodeIndex, u_hash: H::Hash) {
        debug!(target: "AlephBFT-runway", "{:?} Received parents request for hash {:?} from {:?}.", self.index(), u_hash, peer_id);

        let mut message = None;
        if let Some(p_hashes) = self.store.get_parents(u_hash) {
            let p_hashes = p_hashes.clone();
            trace!(target: "AlephBFT-runway", "{:?} Answering parents request for hash {:?} from {:?}.", self.index(), u_hash, peer_id);
            let mut full_units = Vec::new();
            for hash in p_hashes.iter() {
                if let Some(fu) = self.store.unit_by_hash(hash) {
                    full_units.push(fu.clone().into());
                } else {
                    debug!(target: "AlephBFT-runway", "{:?} Not answering parents request, one of the parents missing from store.", self.index());
                    //This can happen if we got a parents response from someone, but one of the units was a fork and we dropped it.
                    //Either this parent is legit and we will soon get it in alert or the parent is not legit in which case
                    //the unit u, whose parents are beeing seeked here is not legit either.
                    //In any case, if a node added u to its Dag, then it should never reach this place in code when answering
                    //a parents request (as all the parents must be legit an thus must be in store).
                    return;
                }
            }
            message = Some(UnitMessage::ResponseParents(u_hash, full_units));
        } else {
            trace!(target: "AlephBFT-runway", "{:?} Not answering parents request for hash {:?}. Unit not in DAG yet.", self.index(), u_hash);
        }
        if let Some(message) = message {
            self.send_unit_message(message, peer_id);
        }
    }

    fn on_parents_response(
        &mut self,
        u_hash: H::Hash,
        parents: Vec<UncheckedSignedUnit<H, D, MK::Signature>>,
    ) -> Result<(), ()> {
        if self.store.get_parents(u_hash).is_some() {
            trace!(target: "AlephBFT-runway", "{:?} We got parents response but already know the parents.", self.index());
            return Ok(());
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
                trace!(target: "AlephBFT-runway", "{:?} We got parents but don't even know the unit. Ignoring.", self.index());
                return Ok(());
            }
        };

        if parent_ids.len() != parents.len() {
            warn!(target: "AlephBFT-runway", "{:?} In received parent response expected {} parents got {} for unit {:?}.", self.index(), parents.len(), parent_ids.len(), u_hash);
            return Err(());
        }

        let mut p_hashes_node_map: NodeMap<Option<H::Hash>> =
            NodeMap::new_with_len(self.config.n_members);
        for (i, uu) in parents.into_iter().enumerate() {
            let su = match self.validate_unit(uu) {
                None => {
                    warn!(target: "AlephBFT-runway", "{:?} In received parent response received a unit that does not pass validation.", self.index());
                    return Err(());
                }
                Some(su) => su,
            };
            let full_unit = su.as_signable();
            if full_unit.round() + 1 != u_round {
                warn!(target: "AlephBFT-runway", "{:?} In received parent response received a unit with wrong round.", self.index());
                return Err(());
            }
            if full_unit.creator() != parent_ids[i] {
                warn!(target: "AlephBFT-runway", "{:?} In received parent response received a unit with wrong creator.", self.index());
                return Err(());
            }
            let p_hash = full_unit.hash();
            let ix = full_unit.creator();
            p_hashes_node_map[ix] = Some(p_hash);
            // There might be some optimization possible here to not validate twice, but overall
            // this piece of code should be executed extremely rarely.
            self.add_unit_to_store_unless_fork(su);
        }

        if ControlHash::<H>::combine_hashes(&p_hashes_node_map) != u_control_hash {
            warn!(target: "AlephBFT-runway", "{:?} In received parent response the control hash is incorrect {:?}.", self.index(), p_hashes_node_map);
            return Err(());
        }
        let p_hashes: Vec<H::Hash> = p_hashes_node_map.into_iter().flatten().collect();
        self.store.add_parents(u_hash, p_hashes.clone());
        self.request_tracker.parents_resolved(&u_hash);
        trace!(target: "AlephBFT-runway", "{:?} Succesful parents reponse for {:?}.", self.index(), u_hash);
        self.send_consensus_notification(NotificationIn::UnitParents(u_hash, p_hashes));
        Ok(())
    }

    fn send_consensus_notification(&mut self, notification: NotificationIn<H>) {
        self.tx_consensus
            .unbounded_send(notification)
            .expect("Channel to consensus should be open")
    }

    async fn on_alert_notification(
        &mut self,
        notification: ForkingNotification<H, D, MK::Signature>,
    ) {
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
                    if self.on_unit_received(uu, true).is_err() {
                        debug!(target: "AlephBFT-runway", "{:?} on_alert_notification failed to process units.", self.index());
                    }
                }
            }
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
                self.request_tracker.parents_resolved(&h);
            }
        }
    }

    async fn on_create(&mut self, u: PreUnit<H>) {
        debug!(target: "AlephBFT-runway", "{:?} On create notification.", self.index());
        let data = self.data_io.get_data();
        let full_unit = FullUnit::new(u, data, self.config.session_id);
        let hash: <H as Hasher>::Hash = full_unit.hash();
        let signed_unit: Signed<FullUnit<H, D>, MK> = Signed::sign(full_unit, self.keybox).await;
        self.store.add_unit(signed_unit.clone(), false);

        let message = UnitMessage::<H, D, MK::Signature>::NewUnit(signed_unit.into());
        trace!(target: "AlephBFT-runway", "{:?} Sending a unit {:?}.", self.index(), hash);
        self.unit_messages_for_network
            .unbounded_send(OutgoingMessage::Raw((message, Recipient::Everyone)))
            .expect("network's channel should be open")
    }

    fn on_missing_coords(&mut self, mut coords: Vec<UnitCoord>) {
        trace!(target: "AlephBFT-runway", "{:?} Dealing with missing coords notification {:?}.", self.index(), coords);
        coords.retain(|coord| !self.store.contains_coord(coord));
        for coord in coords {
            let message = UnitMessage::<H, D, MK::Signature>::RequestCoord(self.index(), coord);
            self.unit_messages_for_network
                .unbounded_send(OutgoingMessage::WithTrackedRequest(
                    self.request_tracker.new_missing_coords_request(&coord),
                    (message, Recipient::Everyone),
                ))
                .expect("network's channel should be open");
        }
    }

    async fn on_wrong_control_hash(&mut self, u_hash: H::Hash) {
        trace!(target: "AlephBFT-runway", "{:?} Dealing with wrong control hash notification {:?}.", self.index(), u_hash);
        let mut notification = None;
        if let Some(p_hashes) = self.store.get_parents(u_hash) {
            // We have the parents by some strange reason (someone sent us parents
            // without us requesting them).
            let p_hashes = p_hashes.clone();
            trace!(target: "AlephBFT-runway", "{:?} We have the parents for {:?} even though we did not request them.", self.index(), u_hash);
            notification = Some(NotificationIn::UnitParents(u_hash, p_hashes));
        } else {
            let message = UnitMessage::<H, D, MK::Signature>::RequestParents(self.index(), u_hash);
            self.unit_messages_for_network
                .unbounded_send(OutgoingMessage::WithTrackedRequest(
                    self.request_tracker.new_missing_parents_request(&u_hash),
                    (message, Recipient::Everyone),
                ))
                .expect("network's channel should be open");
        }
        if let Some(notification) = notification {
            self.send_consensus_notification(notification);
        }
    }

    async fn on_ordered_batch(&mut self, batch: Vec<H::Hash>) {
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
            error!(target: "AlephBFT-runway", "{:?} Error when sending batch {:?}.", self.index(), e);
        }
    }

    async fn move_units_to_consensus(&mut self) {
        let units_to_move = self
            .store
            .yield_buffer_units()
            .into_iter()
            .map(|su| su.as_signable().unit())
            .collect();
        self.send_consensus_notification(NotificationIn::NewUnits(units_to_move))
    }
}

impl<H, D, MK, DP, SH> Runway<'static, H, D, MK, DP, SH>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    DP: DataIO<D>,
    SH: SpawnHandle,
{
    pub(crate) async fn run(
        mut self,
        mut exit: oneshot::Receiver<()>,
        alerter: Alerter<'static, H, D, MK>,
        consensus: Consensus<H, SH>,
    ) {
        info!(target: "AlephBFT-runway", "{:?} Runway starting.", self.index());

        let barrier = Arc::new(Barrier::new(2));
        let index = self.index();
        let (alerter_exit, exit_stream) = oneshot::channel();
        let alerter_barrier = barrier.clone();
        let alerter_handle = self
            .spawn_handle
            .spawn_essential("runway/alerter", async move {
                alerter.run(exit_stream).await;
                alerter_barrier.wait().await;
            });
        let mut alerter_handle = into_infinite_stream(alerter_handle).fuse();

        let (consensus_exit, exit_stream) = oneshot::channel();
        let consensus_barrier = barrier.clone();
        let consensus_handle = self
            .spawn_handle
            .spawn_essential("runway/consensus", async move {
                consensus.run(exit_stream).await;
                consensus_barrier.wait().await;
            });
        let mut consensus_handle = into_infinite_stream(consensus_handle).fuse();

        info!(target: "AlephBFT-runway", "{:?} Runway started.", index);
        loop {
            futures::select! {
                notification = self.rx_consensus.next() => match notification {
                        Some(notification) => self.on_consensus_notification(notification).await,
                        None => {
                            error!(target: "AlephBFT-runway", "{:?} Consensus notification stream closed.", index);
                            break;
                        }
                },

                notification = self.notifications_from_alerter.next() => match notification {
                    Some(notification) => self.on_alert_notification(notification).await,
                    None => {
                        error!(target: "AlephBFT-runway", "{:?} Alert notification stream closed.", index);
                        break;
                    }
                },

                event = self.unit_messages_from_network.next() => match event {
                    Some(event) => self.on_unit_message(event),
                    None => {
                        error!(target: "AlephBFT-runway", "{:?} Unit message stream closed.", index);
                        break;
                    }
                },

                batch = self.ordered_batch_rx.next() => match batch {
                    Some(batch) => self.on_ordered_batch(batch).await,
                    None => {
                        error!(target: "AlephBFT-runway", "{:?} Ordered batch stream closed.", index);
                        break;
                    }
                },

                _ = alerter_handle.next() => {
                    debug!(target: "AlephBFT-runway", "{:?} alerter task terminated early.", index);
                    break;
                },

                _ = consensus_handle.next() => {
                    debug!(target: "AlephBFT-runway", "{:?} consensus task terminated early.", index);
                    break;
                },

                _ = &mut exit => break,
            }
            self.move_units_to_consensus();
        }

        info!(target: "AlephBFT-runway", "{:?} Ending run.", index);

        if consensus_exit.send(()).is_err() {
            debug!(target: "AlephBFT-runway", "{:?} consensus already stopped.", index);
        }
        consensus_handle.next().await.unwrap();
        if alerter_exit.send(()).is_err() {
            debug!(target: "AlephBFT-runway", "{:?} alerter already stopped.", index);
        }
        alerter_handle.next().await.unwrap();

        info!(target: "AlephBFT-runway", "{:?} Run ended.", index);
    }
}

#[derive(Clone)]
pub(crate) struct TrackedRequest {
    satisfied: Arc<AtomicBool>,
}

impl TrackedRequest {
    fn new() -> Self {
        TrackedRequest {
            satisfied: Arc::new(AtomicBool::new(false)),
        }
    }

    fn is_satisfied(&self) -> bool {
        self.satisfied.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn set_satisfied(&mut self) {
        self.satisfied
            .store(true, std::sync::atomic::Ordering::Relaxed)
    }
}

struct RequestTracker<H: Hasher> {
    missing_coords: HashMap<UnitCoord, TrackedRequest>,
    missing_parents: HashMap<H::Hash, TrackedRequest>,
}

impl<H: Hasher> RequestTracker<H> {
    fn new() -> Self {
        RequestTracker {
            missing_coords: HashMap::new(),
            missing_parents: HashMap::new(),
        }
    }

    fn new_missing_coords_request(&mut self, coord: &UnitCoord) -> TrackedRequest {
        self.missing_coords
            .entry(*coord)
            .or_insert_with(TrackedRequest::new)
            .clone()
    }

    fn new_missing_parents_request(&mut self, u_hash: &H::Hash) -> TrackedRequest {
        self.missing_parents
            .entry(*u_hash)
            .or_insert_with(TrackedRequest::new)
            .clone()
    }

    fn parents_resolved(&mut self, u_hash: &H::Hash) {
        self.missing_parents
            .remove(u_hash)
            .into_iter()
            .for_each(|mut req| req.set_satisfied());
    }

    fn coordinates_resolved(&mut self, coord: &UnitCoord) {
        self.missing_coords
            .remove(coord)
            .into_iter()
            .for_each(|mut req| req.set_satisfied())
    }
}
