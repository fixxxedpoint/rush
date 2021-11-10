use futures::{
    channel::{mpsc, oneshot},
    future::FusedFuture,
    FutureExt,
};
use log::{debug, info};

use crate::{
    config::Config,
    creation,
    extender::Extender,
    runway::{NotificationIn, NotificationOut},
    terminal::Terminal,
    Hasher, OrderedBatch, Round, SpawnHandle, ToOneShotReceiver, ToReceiver, ToSender, CP5,
};

pub(crate) async fn run<
    H: Hasher + 'static,
    CH: CP5<NotificationIn<H>, NotificationOut<H>, OrderedBatch<H::Hash>, Round, ()>,
>(
    conf: Config,
    incoming_notifications: ToReceiver<CH, NotificationIn<H>>,
    outgoing_notifications: ToSender<CH, NotificationOut<H>>,
    ordered_batch_tx: ToSender<CH, OrderedBatch<H::Hash>>,
    spawn_handle: impl SpawnHandle,
    starting_round: ToOneShotReceiver<CH, Round>,
    mut exit: ToOneShotReceiver<CH, ()>,
) {
    info!(target: "AlephBFT", "{:?} Starting all services...", conf.node_ix);

    let n_members = conf.n_members;
    let index = conf.node_ix;

    let (electors_tx, electors_rx) = mpsc::unbounded();
    let mut extender = Extender::<H>::new(index, n_members, electors_rx, ordered_batch_tx);
    let (extender_exit, exit_rx) = oneshot::channel();
    let mut extender_handle = spawn_handle
        .spawn_essential("consensus/extender", async move {
            extender.extend(exit_rx).await
        })
        .fuse();

    let (parents_for_creator, parents_from_terminal) = mpsc::unbounded();

    let (creator_exit, exit_rx) = oneshot::channel();
    let io = creation::IO {
        outgoing_units: outgoing_notifications.clone(),
        incoming_parents: parents_from_terminal,
    };
    let mut creator_handle = spawn_handle
        .spawn_essential("consensus/creation", async move {
            creation::run(conf.clone().into(), io, starting_round, exit_rx).await;
        })
        .fuse();

    let mut terminal = Terminal::new(index, incoming_notifications, outgoing_notifications);

    // send a new parent candidate to the creator
    terminal.register_post_insert_hook(Box::new(move |u| {
        parents_for_creator
            .unbounded_send(u.into())
            .expect("Channel to creator should be open.");
    }));
    // try to extend the partial order after adding a unit to the dag
    terminal.register_post_insert_hook(Box::new(move |u| {
        electors_tx
            .unbounded_send(u.into())
            .expect("Channel to extender should be open.")
    }));

    let (terminal_exit, exit_rx) = oneshot::channel();
    let mut terminal_handle = spawn_handle
        .spawn_essential(
            "consensus/terminal",
            async move { terminal.run(exit_rx).await },
        )
        .fuse();
    info!(target: "AlephBFT", "{:?} All services started.", index);

    futures::select! {
        _ = exit => {},
        _ = terminal_handle => {
            debug!(target: "AlephBFT-consensus", "{:?} terminal task terminated early.", index);
        },
        _ = creator_handle => {
            debug!(target: "AlephBFT-consensus", "{:?} creator task terminated early.", index);
        },
        _ = extender_handle => {
            debug!(target: "AlephBFT-consensus", "{:?} extender task terminated early.", index);
        }
    }

    // we stop no matter if received Ok or Err
    if terminal_exit.send(()).is_err() {
        debug!(target: "AlephBFT-consensus", "{:?} terminal already stopped.", index);
    }
    if !terminal_handle.is_terminated() {
        terminal_handle.await.unwrap();
    }

    if creator_exit.send(()).is_err() {
        debug!(target: "AlephBFT-consensus", "{:?} creator already stopped.", index);
    }
    if !creator_handle.is_terminated() {
        creator_handle.await.unwrap();
    }

    if extender_exit.send(()).is_err() {
        debug!(target: "AlephBFT-consensus", "{:?} extender already stopped.", index);
    }
    if !extender_handle.is_terminated() {
        extender_handle.await.unwrap();
    }

    info!(target: "AlephBFT", "{:?} All services stopped.", index);
}
