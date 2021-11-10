use crate::{
    config::{Config as GeneralConfig, DelaySchedule},
    nodes::{NodeCount, NodeIndex},
    runway::NotificationOut,
    units::{PreUnit, Unit},
    Hasher, Round, ToReceiver, ToSender, CP2,
};
use futures::{channel::oneshot, FutureExt, StreamExt};
use futures_timer::Delay;
use log::{debug, error, info, warn};
use std::time::Duration;

mod creator;

use creator::Creator;

/// The configuration needed for the process creating new units.
pub struct Config {
    node_id: NodeIndex,
    n_members: NodeCount,
    create_lag: DelaySchedule,
    max_round: Round,
}

impl From<GeneralConfig> for Config {
    fn from(conf: GeneralConfig) -> Self {
        Config {
            node_id: conf.node_ix,
            n_members: conf.n_members,
            create_lag: conf.delay_config.unit_creation_delay,
            max_round: conf.max_round,
        }
    }
}

pub struct IO<H: Hasher, CH: CP2<Unit<H>, NotificationOut<H>>> {
    pub(crate) incoming_parents: ToReceiver<CH, Unit<H>>,
    pub(crate) outgoing_units: ToSender<CH, NotificationOut<H>>,
}

async fn create_unit<H: Hasher, CH: CP2<Unit<H>, NotificationOut<H>>>(
    round: Round,
    creator: &mut Creator<H>,
    create_lag: &DelaySchedule,
    mut can_create: bool,
    incoming_parents: &mut ToReceiver<CH, Unit<H>>,
    mut exit: &mut oneshot::Receiver<()>,
) -> Result<(PreUnit<H>, Vec<H::Hash>), ()> {
    let mut delay = Delay::new(create_lag(round.into())).fuse();
    loop {
        if can_create {
            if let Some(result) = creator.create_unit(round) {
                return Ok(result);
            }
        }
        futures::select! {
            unit = incoming_parents.next() => match unit {
                Some(unit) => creator.add_unit(&unit),
                None => {
                    info!(target: "AlephBFT-creator", "Incoming parent channel closed, exiting.");
                    return Err(());
                }
            },
            _ = &mut delay => {
                if can_create {
                    warn!(target: "AlephBFT-creator", "Delay passed despite us not waiting for it.");
                }
                can_create = true;
                delay = Delay::new(Duration::from_secs(30 * 60)).fuse();
            },
            _ = exit => {
                info!(target: "AlephBFT-creator", "Received exit signal.");
                return Err(());
            },
        }
    }
}

/// A process responsible for creating new units. It receives all the units added locally to the Dag
/// via the `incoming_parents` channel. It creates units according to an internal strategy respecting
/// always the following constraints: if round is equal to 0, U has no parents, otherwise for a unit U of round r > 0
/// - all U's parents are from round (r-1),
/// - all U's parents are created by different nodes,
/// - one of U's parents is the (r-1)-round unit by U's creator,
/// - U has > floor(2*N/3) parents.
/// - U will appear in the channel only if all U's parents appeared there before
/// The currently implemented strategy creates the unit U according to a delay schedule and when enough
/// candidates for parents are available for all the above constraints to be satisfied.
///
/// We refer to the documentation https://cardinal-cryptography.github.io/AlephBFT/internals.html
/// Section 5.1 for a discussion of this component.
pub async fn run<H: Hasher, CH: CP2<Unit<H>, NotificationOut<H>>>(
    conf: Config,
    io: IO<H, CH>,
    starting_round: oneshot::Receiver<Round>,
    mut exit: oneshot::Receiver<()>,
) {
    let Config {
        node_id,
        n_members,
        create_lag,
        max_round,
    } = conf;
    let mut creator = Creator::new(node_id, n_members);
    let IO {
        mut incoming_parents,
        outgoing_units,
    } = io;
    let starting_round = match starting_round.await {
        Ok(round) => round,
        Err(e) => {
            error!(target: "AlephBFT-creator", "Starting round not provided: {}", e);
            return;
        }
    };
    debug!(target: "AlephBFT-creator", "Creator starting from round {}", starting_round);
    for round in starting_round..max_round {
        // Skip waiting if someone created a unit of a higher round.
        // In such a case at least 2/3 nodes created units from this round so we aren't skipping a
        // delay we should observe.
        let ignore_delay = creator.current_round() > round;
        let (unit, parent_hashes) = match create_unit(
            round,
            &mut creator,
            &create_lag,
            ignore_delay,
            &mut incoming_parents,
            &mut exit,
        )
        .await
        {
            Ok((u, ph)) => (u, ph),
            Err(_) => {
                return;
            }
        };
        if let Err(e) =
            outgoing_units.unbounded_send(NotificationOut::CreatedPreUnit(unit, parent_hashes))
        {
            warn!(target: "AlephBFT-creator", "Notification send error: {}. Exiting.", e);
            return;
        }
    }
    warn!(target: "AlephBFT-creator", "Maximum round reached. Not creating another unit.");
}
