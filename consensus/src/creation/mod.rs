use crate::{
    config::{Config as GeneralConfig, DelaySchedule},
    runway::NotificationOut,
    units::{PreUnit, Unit},
    Hasher, NodeCount, NodeIndex, Receiver, Round, Sender, Terminator,
};
use futures::{
    channel::{mpsc::SendError, oneshot},
    FutureExt, StreamExt,
};
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
) -> Result<(PreUnit<H>, Vec<H::Hash>), ()> {
    let mut delay = very_long_delay().fuse();
    loop {
        match creator.create_unit(round) {
            Ok(unit) => return Ok(unit),
            Err(err) => {
                debug!(target: "AlephBFT-creator", "Creator unable to create a new unit at round {:?}: {}.", round, err)
            }
        }
        futures::select! {
            _ = delay => {
                warn!(target: "AlephBFT-creator", "Delay passed at round {} despite us not waiting for it.", &round);
                delay = very_long_delay().fuse();
            },
            result = process_parent(creator, incoming_parents).fuse() => {
                result?
            }
        }
    }
}

/// Tries to process a single parent from given `incoming_parents` receiver.
/// Returns `true` iff given `delay` passed. Returns error when `exit` or `incoming_parents` are closed.
async fn process_parent<H: Hasher>(
    creator: &mut Creator<H>,
    incoming_parents: &mut Receiver<Unit<H>>,
) -> anyhow::Result<(), ()> {
    let unit = incoming_parents.next().await.ok_or(())?;
    creator.add_unit(&unit);
    Ok(())
}

/// Tries to process new parents from `incoming_parents`. Return an error when `incoming_parents` is closed.
async fn keep_processing_parents<H: Hasher>(
    creator: &mut Creator<H>,
    incoming_parents: &mut Receiver<Unit<H>>,
) -> anyhow::Result<(), ()> {
    loop {
        process_parent(creator, incoming_parents).await?;
    }
}

async fn keep_processing_parents_until<H: Hasher>(
    creator: &mut Creator<H>,
    incoming_parents: &mut Receiver<Unit<H>>,
    until: Delay,
) -> anyhow::Result<(), ()> {
    futures::select! {
        result = keep_processing_parents(creator, incoming_parents).fuse() => {
            result?
        },
        _ = until.fuse() => {
            debug!(target: "AlephBFT-creator", "Delay passed.");
        },
    }
    Ok(())
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
    mut io: IO<H>,
    starting_round: oneshot::Receiver<Option<Round>>,
    mut terminator: Terminator,
) {
    futures::select! {
        _ = terminator.get_exit() => {},
        _ = async {
            let maybe_round = starting_round.await;
            let starting_round = match maybe_round {
                Ok(Some(round)) => round,
                Ok(None) => {
                    warn!(target: "AlephBFT-creator", "None starting round provided. Exiting.");
                    return;
                }
                Err(e) => {
                    error!(target: "AlephBFT-creator", "Starting round not provided: {}", e);
                    return;
                }
            };

            match run_creator(conf, &mut io, starting_round).await {
                Ok(_) => {},
                Err(err) =>
                    match err {
                        CreatorError::OutChannelClosed(e) => warn!(target: "AlephBFT-creator", "Notification send error: {}. Exiting.", e),
                        CreatorError::ParentsChannelClosed => debug!(target: "AlephBFT-creator", "Incoming parent channel closed, exiting."),
                    },
            };
        }.fuse() => {},
    }

    terminator.terminate_sync().await;
}

enum CreatorError {
    OutChannelClosed(SendError),
    ParentsChannelClosed,
}

async fn run_creator<H: Hasher>(
    conf: Config,
    io: &mut IO<H>,
    starting_round: Round,
) -> anyhow::Result<(), CreatorError> {
    let Config {
        node_id,
        n_members,
        create_lag,
        max_round,
    } = conf;
    let mut creator = Creator::new(node_id, n_members);
    let incoming_parents = &mut io.incoming_parents;
    let outgoing_units = &io.outgoing_units;

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
            keep_processing_parents_until(&mut creator, incoming_parents, lag)
                .await
                .map_err(|_| CreatorError::ParentsChannelClosed)?;
        }

        let (unit, parent_hashes) = create_unit(round, &mut creator, incoming_parents)
            .await
            .map_err(|_| CreatorError::ParentsChannelClosed)?;

        trace!(target: "AlephBFT-creator", "Created a new unit {:?} at round {:?}.", unit, round);
        if let Err(e) =
            outgoing_units.unbounded_send(NotificationOut::CreatedPreUnit(unit, parent_hashes))
        {
            warn!(target: "AlephBFT-creator", "Notification send error: {}. Exiting.", e);
            return Err(CreatorError::OutChannelClosed(e.into_send_error()));
        }
    }

    warn!(target: "AlephBFT-creator", "Maximum round reached. Not creating another unit.");
    Ok(())
}
