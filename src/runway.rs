use crate::{
    alerts::{Alert, AlertConfig, AlertMessage, Alerter, ForkProof, ForkingNotification},
    consensus::Consensus,
    member::{NotificationIn, NotificationOut, UnitMessage},
    network::Recipient,
    nodes::NodeMap,
    units::{
        ControlHash, FullUnit, PreUnit, SignedUnit, UncheckedSignedUnit, UnitCoord, UnitStore,
    },
    Config, Data, DataIO, Hasher, Index, KeyBox, MultiKeychain, NodeCount, NodeIndex, OrderedBatch,
    Receiver, Sender, Signed, SpawnHandle,
};
use futures::{
    channel::{mpsc, oneshot},
    FutureExt,
};
use log::{debug, error, info, trace, warn};

pub(crate) struct Runway<'a, H, D, MK, DP, NH, SH>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    DP: DataIO<D>,
    NH: FnMut((UnitMessage<H, D, MK::Signature>, Option<Recipient>)),
    SH: SpawnHandle,
{
    config: Config,
    store: UnitStore<'a, H, D, MK>,
    index: NodeIndex,
    keybox: &'a MK,
    alerts_for_alerter: Sender<Alert<H, D, MK::Signature>>,
    notifications_from_alerter: Receiver<ForkingNotification<H, D, MK::Signature>>,
    unit_messages_for_network: Sender<(UnitMessage<H, D, MK::Signature>, Recipient)>,
    unit_messages_from_network: Receiver<UnitMessage<H, D, MK::Signature>>,
    tx_consensus: Sender<NotificationIn<H>>,
    rx_consensus: Receiver<NotificationOut<H>>,
    ordered_batch_rx: Receiver<Vec<H::Hash>>,
    notification_handler: H,
    data_io: DP,
    spawn_handle: SH,
    alerter: Alerter<'a, H, D, MK>,
    consensus: Consensus<H>,
}

pub(crate) struct RunwayConfig<
    'a,
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    DP: DataIO<D>,
    SH: SpawnHandle,
> {
    config: Config,
    data_io: DP,
    keybox: &'a MK,
    spawn_handle: SH,
    alerter: (
        Alerter<'a, H, D, MK>,
        Receiver<ForkingNotification<H, D, MK::Signature>>,
        Sender<Alert<H, D, MK::Signature>>,
    ),
    consensus: (
        Consensus<H>,
        Sender<NotificationIn<H>>,
        Receiver<NotificationOut<H>>,
        Receiver<OrderedBatch<H::Hash>>,
    ),
    unit_messages_for_network: Sender<(UnitMessage<H, D, MK::Signature>, Recipient)>,
    unit_messages_from_network: Receiver<UnitMessage<H, D, MK::Signature>>,
}

pub(crate) struct RunwayConfigBuilder {}

impl RunwayConfigBuilder {
    pub(crate) fn build<
        'a,
        H: Hasher,
        D: Data,
        MK: MultiKeychain,
        DP: DataIO<D>,
        SH: SpawnHandle,
    >(
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
        unit_messages_for_network: Sender<(UnitMessage<H, D, MK::Signature>, Recipient)>,
        unit_messages_from_network: Receiver<UnitMessage<H, D, MK::Signature>>,
    ) -> RunwayConfig<'a, H, D, MK, DP, SH> {
        let (tx_consensus, consensus_stream) = mpsc::unbounded();
        let (consensus_sink, mut rx_consensus) = mpsc::unbounded();
        let (ordered_batch_tx, mut ordered_batch_rx) = mpsc::unbounded();
        let consensus = Consensus::new(config, consensus_stream, consensus_sink, ordered_batch_tx);

        let (alert_notifications_for_units, mut notifications_from_alerter) = mpsc::unbounded();
        let (alerts_for_alerter, alerts_from_units) = mpsc::unbounded();
        let (alerter_exit, exit_stream) = oneshot::channel();
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

        RunwayConfig {
            config,
            data_io,
            keybox: keychain,
            spawn_handle,
            alerter: (alerter, notifications_from_alerter, alerts_for_alerter),
            consensus: (consensus, tx_consensus, rx_consensus, ordered_batch_rx),
            unit_messages_for_network,
            unit_messages_from_network,
        }
    }
}

impl<'a, H, D, MK, DP, NH, SH> Runway<'a, H, D, MK, DP, NH, SH>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    DP: DataIO<D>,
    NH: FnMut(NotificationOut<H>),
    SH: SpawnHandle,
{
    pub(crate) fn new(
        config: RunwayConfig<'a, H, D, MK, DP, SH>,
        handler: impl FnMut((UnitMessage<H, D, MK::Signature>, Option<Recipient>)),
    ) -> Self {
        let n_members = config.n_members;
        let threshold = (n_members * 2) / 3 + NodeCount(1);
        let max_round = config.max_round;
        Runway {
            config,
            store: UnitStore::new(n_members, threshold, max_round),
            index: config.config.node_ix,
            keybox: config.keybox,
            alerts_for_alerter: config.alerter.2,
            notifications_from_alerter: config.alerter.1,
            alerter: config.alerter.0,
            tx_consensus: config.consensus.2,
            rx_consensus: config.consensus.1,
            consensus: config.consensus.0,
            data_io: config.data_io,
            unit_messages_for_network: config.unit_messages_for_network,
            unit_messages_from_network: config.unit_messages_from_network,
            spawn_handle: config.spawn_handle,
            ordered_batch_rx: config.consensus.3,
            notification_handler: handler,
        }
    }

    fn index(&self) -> NodeIndex {
        self.config.node_ix
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
        let threshold = self.threshold();
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
        units: Vec<SignedUnit<'a, H, D, MK>>,
    ) -> Alert<H, D, MK::Signature> {
        Alert::new(
            self.config.node_ix,
            proof,
            units.into_iter().map(|signed| signed.into()).collect(),
        )
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

    fn send_unit_message(&mut self, message: UnitMessage<H, D, MK::Signature>, peer_id: NodeIndex) {
        self.unit_messages_for_network
            .as_ref()
            .unwrap()
            .unbounded_send((message, Recipient::Node(peer_id)))
            .expect("Channel to network should be open")
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

    fn send_consensus_notification(&mut self, notification: NotificationIn<H>) {
        self.tx_consensus
            .unbounded_send(notification)
            .expect("Channel to consensus should be open")
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

    async fn on_create(&mut self, u: PreUnit<H>) {
        debug!(target: "AlephBFT-member", "{:?} On create notification.", self.index());
        let data = self.data_io.get_data();
        let full_unit = FullUnit::new(u, data, self.config.session_id);
        let hash: <H as Hasher>::Hash = full_unit.hash();
        let signed_unit: Signed<FullUnit<H, D>, MK> = Signed::sign(full_unit, self.keybox).await;
        self.store.add_unit(signed_unit, false);

        let message = UnitMessage::<H, D, MK::Signature>::NewUnit(signed_unit.into());
        trace!(target: "AlephBFT-runway", "{:?} Sending a unit {:?}.", self.index(), hash);
        (self.notification_handler)((message, Some(Recipient::Everyone)));
    }

    pub(crate) fn on_missing_coords(&mut self, coords: Vec<UnitCoord>) {
        trace!(target: "AlephBFT-runway", "{:?} Dealing with missing coords notification {:?}.", self.index(), coords);
        for coord in coords.retain(|coord| !self.store.contains_coord(coord)) {
            let message = UnitMessage::<H, D, MK::Signature>::RequestCoord(self.index(), coord);
            (self.notification_handler)((message, None));
        }
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
            let message = UnitMessage::<H, D, MK::Signature>::RequestParents(self.index(), u_hash);
            (self.notification_handler)((message, None));
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

    fn move_units_to_consensus(&mut self) {
        let units_to_move = self
            .store
            .yield_buffer_units()
            .into_iter()
            .map(|su| su.as_signable().unit())
            .collect();
        self.send_consensus_notification(NotificationIn::NewUnits(units_to_move))
    }

    pub async fn run(mut self, mut exit: oneshot::Receiver<()>) {
        info!(target: "AlephBFT-airstrip", "{:?} Airstrip starting.", self.index());

        let (alerter_exit, exit_stream) = oneshot::channel();
        let alerter_handle = self
            .spawn_handle
            .spawn_essential("runway/alerter", async move {
                self.alerter.run(exit_stream).await;
            })
            .then(|_| async { 0.. })
            .flatten_stream();

        let (consensus_exit, exit_stream) = oneshot::channel();
        let consensus_handle = self
            .spawn_handle
            .spawn_essential("runway/consensus", async move {
                self.consensus.run(self.spawn_handle, exit_stream).await;
            })
            .then(|_| async { 0.. })
            .flatten_stream();

        let mut alerter_exited = false;
        let mut consensus_exited = false;
        info!(target: "AlephBFT-airstrip", "{:?} Airstrip started.", self.index());

        loop {
            futures::select! {
                notification = self.rx_consensus.next() => match notification {
                        Some(notification) => self.on_consensus_notification(notification).await,
                        None => {
                            error!(target: "AlephBFT-member", "{:?} Consensus notification stream closed.", self.index());
                            break;
                        }
                },

                notification = self.notifications_from_alerter.next() => match notification {
                    Some(notification) => self.on_alert_notification(notification),
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Alert notification stream closed.", self.index());
                        break;
                    }
                },

                event = self.unit_messages_from_network.next() => match event {
                    Some(event) => self.on_unit_message(event),
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Unit message stream closed.", self.index());
                        break;
                    }
                },

                batch = self.ordered_batch_rx.next() => match batch {
                    Some(batch) => self.on_ordered_batch(batch),
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Ordered batch stream closed.", self.index());
                        break;
                    }
                },

                _ = &mut alerter_handle.next() => {
                    debug!(target: "AlephBFT-runway", "{:?} alerter task terminated early.", self.index());
                    break;
                },

                _ = &mut consensus_handle.next() => {
                    debug!(target: "AlephBFT-runway", "{:?} consensus task terminated early.", self.index());
                    break;
                },

                _ = &mut exit => break,
            }
            self.move_units_to_consensus();
        }

        info!(target: "AlephBFT-airstrip", "{:?} Ending run.", self.index());

        let _ = consensus_exit.send(());
        consensus_handle.next().await.unwrap();

        let _ = alerter_exit.send(());
        alerter_handle.next().await.unwrap();

        info!(target: "AlephBFT-airstrip", "{:?} Run ended.", self.index());
    }
}
