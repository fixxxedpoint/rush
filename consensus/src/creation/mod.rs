use crate::{
    config::{Config as GeneralConfig, DelaySchedule},
    runway::NotificationOut,
    units::{PreUnit, Unit},
    Hasher, NodeCount, NodeIndex, Receiver, Round, Sender, Terminator,
};
use futures::{channel::oneshot, future::FusedFuture, pin_mut, FutureExt, StreamExt};
use futures_timer::Delay;
use log::{debug, error, trace, warn};
use std::{
    fmt::{Debug, Formatter},
    time::Duration,
};

mod creator;

pub use creator::Creator;

/// The configuration needed for the process creating new units.
#[derive(Clone)]
pub struct Config {
    node_id: NodeIndex,
    n_members: NodeCount,
    create_lag: DelaySchedule,
    max_round: Round,
}

impl Debug for Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("node id", &self.node_id)
            .field("member count", &self.n_members)
            .field("max round", &self.max_round)
            .finish()
    }
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

pub struct IO<H: Hasher> {
    pub(crate) incoming_parents: Receiver<Unit<H>>,
    pub(crate) outgoing_units: Sender<NotificationOut<H>>,
}

fn very_long_delay() -> Delay {
    Delay::new(Duration::from_secs(30 * 60))
}

async fn create_unit<H: Hasher>(
    round: Round,
    creator: &mut Creator<H>,
    incoming_parents: &mut Receiver<Unit<H>>,
    exit: &mut oneshot::Receiver<()>,
) -> Result<(PreUnit<H>, Vec<H::Hash>), ()> {
    let mut delay = very_long_delay().fuse();
    loop {
        match creator.create_unit(round) {
            Ok(unit) => return Ok(unit),
            Err(err) => {
                debug!(target: "AlephBFT-creator", "Creator unable to create a new unit at round {:?}: {}.", round, err)
            }
        }
        if process_parent(creator, incoming_parents, &mut delay, exit).await? {
            warn!(target: "AlephBFT-creator", "Delay passed at round {} despite us not waiting for it.", &round);
            delay = very_long_delay().fuse();
        }
    }
}

/// Tries to process a single parent from given `incoming_parents` receiver.
/// Returns `true` iff given `delay` passed. Returns error when `exit` or `incoming_parents` are closed.
async fn process_parent<H: Hasher>(
    creator: &mut Creator<H>,
    incoming_parents: &mut Receiver<Unit<H>>,
    delay: impl FusedFuture<Output = ()>,
    mut exit: &mut oneshot::Receiver<()>,
) -> anyhow::Result<bool, ()> {
    pin_mut!(delay);
    if delay.as_mut().now_or_never().is_some() {
        debug!(target: "AlephBFT-creator", "Delay passed.");
        return Ok(true);
    }
    futures::select! {
        unit = incoming_parents.next() => match unit {
            Some(unit) => {
                creator.add_unit(&unit);
                Ok(false)
            },
            None => {
                debug!(target: "AlephBFT-creator", "Incoming parent channel closed, exiting.");
                Err(())
            }
        },
        _ = &mut exit => {
            debug!(target: "AlephBFT-creator", "Received exit signal.");
            Err(())
        },
        _ = delay => {
            debug!(target: "AlephBFT-creator", "Delay passed.");
            Ok(true)
        },
    }
}

/// Tries to process new parents from `incoming_parents` until it encounters an error or a signal on `exit` channel.
async fn keep_processing_parents<H: Hasher>(
    creator: &mut Creator<H>,
    incoming_parents: &mut Receiver<Unit<H>>,
    delay: Delay,
    exit: &mut oneshot::Receiver<()>,
) -> anyhow::Result<(), ()> {
    let mut delay = delay.fuse();
    loop {
        if process_parent(creator, incoming_parents, &mut delay, exit).await? {
            return Ok(());
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
pub async fn run<H: Hasher>(
    conf: Config,
    io: IO<H>,
    mut starting_round: oneshot::Receiver<Option<Round>>,
    mut terminator: Terminator,
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

    let starting_round = futures::select! {
        maybe_round =  starting_round => match maybe_round {
            Ok(Some(round)) => round,
            Ok(None) => {
                warn!(target: "AlephBFT-creator", "None starting round provided. Exiting.");
                return;
            }
            Err(e) => {
                error!(target: "AlephBFT-creator", "Starting round not provided: {}", e);
                return;
            }
        },
        _ = &mut terminator.get_exit() => {
            terminator.terminate_sync().await;
            return;
        },
    };

    debug!(target: "AlephBFT-creator", "Creator starting from round {}", starting_round);
    for round in starting_round..max_round {
        // Skip waiting if someone created a unit of a higher round.
        // In such a case at least 2/3 nodes created units from this round so we aren't skipping a
        // delay we should observe.
        // NOTE: even we've observed a unit from a higher round, our own unit from previous round
        // might not be yet added to `creator`. We still might need to wait for its arrival.
        let should_delay = !(creator.current_round() > round);
        if should_delay {
            let lag = Delay::new(create_lag(round.into()));

            if keep_processing_parents(
                &mut creator,
                &mut incoming_parents,
                lag,
                terminator.get_exit(),
            )
            .await
            .is_err()
            {
                return;
            }
        }

        let (unit, parent_hashes) = match create_unit(
            round,
            &mut creator,
            &mut incoming_parents,
            terminator.get_exit(),
        )
        .await
        {
            Ok((u, ph)) => (u, ph),
            Err(_) => {
                terminator.terminate_sync().await;
                return;
            }
        };
        trace!(target: "AlephBFT-creator", "Created a new unit {:?} at round {:?}.", unit, round);
        if let Err(e) =
            outgoing_units.unbounded_send(NotificationOut::CreatedPreUnit(unit, parent_hashes))
        {
            warn!(target: "AlephBFT-creator", "Notification send error: {}. Exiting.", e);
            return;
        }
    }

    warn!(target: "AlephBFT-creator", "Maximum round reached. Not creating another unit.");
}
