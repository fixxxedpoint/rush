//! Implements the Aleph BFT Consensus protocol as a "finality gadget". The [Member] struct
//! requires access to a network layer, a cryptographic primitive, and a data provider that
//! gives appropriate access to the set of available data that we need to make consensus on.

use codec::{Decode, Encode};
use futures::{channel::mpsc, stream::Next, Future, Sink, SinkExt, StreamExt};
use std::{
    fmt::{self, Debug},
    hash::Hash as StdHash,
    marker::PhantomData,
    pin::Pin,
};

use crate::nodes::NodeMap;

pub use config::{default_config, exponential_slowdown, Config, DelayConfig};
pub use member::run_session;
pub use network::{Network, NetworkData, Recipient};
pub use nodes::{NodeCount, NodeIndex};

mod alerts;
mod consensus;
mod creation;
mod extender;
mod member;
mod network;
mod nodes;
mod runway;
mod signed;
pub use signed::*;
mod config;
pub mod rmc;
mod terminal;
#[cfg(test)]
pub mod testing;
mod units;

/// The number of a session for which the consensus is run.
pub type SessionId = u64;

/// The source of data items that consensus should order.
///
/// AlephBFT internally calls [`DataIO::get_data`] whenever a new unit is created and data needs to be placed inside.
/// The [`DataIO::send_ordered_batch`] method is called whenever a new round has been decided and thus a new batch of units
/// (or more precisely the data they carry) is available.
///
/// We refer to the documentation https://cardinal-cryptography.github.io/AlephBFT/aleph_bft_api.html for a discussion
/// and examples of how this trait can be implemented.
pub trait DataIO<Data> {
    type Error: Debug + 'static;
    /// Outputs a new data item to be ordered
    fn get_data(&self) -> Data;
    /// Takes a new ordered batch of data item.
    fn send_ordered_batch(&mut self, data: OrderedBatch<Data>) -> Result<(), Self::Error>;
}

/// Indicates that an implementor has been assigned some index.
pub trait Index {
    fn index(&self) -> NodeIndex;
}

/// A hasher, used for creating identifiers for blocks or units.
pub trait Hasher: Eq + Clone + Send + Sync + Debug + 'static {
    /// A hash, as an identifier for a block or unit.
    type Hash: AsRef<[u8]>
        + Eq
        + Ord
        + Copy
        + Clone
        + Send
        + Sync
        + Debug
        + StdHash
        + Encode
        + Decode;

    fn hash(s: &[u8]) -> Self::Hash;
}

/// Data type that we want to order.
pub trait Data: Eq + Clone + Send + Sync + Debug + StdHash + Encode + Decode + 'static {}

impl<T> Data for T where T: Eq + Clone + Send + Sync + Debug + StdHash + Encode + Decode + 'static {}

/// An asynchronous round of the protocol.
pub type Round = u16;

/// Type for sending a new ordered batch of data items.
pub type OrderedBatch<Data> = Vec<Data>;

/// A handle for waiting the task's completion.
pub type TaskHandle = Pin<Box<dyn Future<Output = Result<(), ()>> + Send>>;

/// An abstraction for an execution engine for Rust's asynchronous tasks.
pub trait SpawnHandle: Clone + Send + 'static {
    /// Run a new task.
    fn spawn(&self, name: &'static str, task: impl Future<Output = ()> + Send + 'static);
    /// Run a new task and returns a handle to it. If there is some error or panic during
    /// execution of the task, the handle should return an error.
    fn spawn_essential(
        &self,
        name: &'static str,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> TaskHandle;
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SendErrorKind {
    Full,
    Disconnected,
}

/// The error type for [`Sender`s](Sender) used as `Sink`s.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SendError {
    kind: SendErrorKind,
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_full() {
            write!(f, "send failed because channel is full")
        } else {
            write!(f, "send failed because receiver is gone")
        }
    }
}

impl std::error::Error for SendError {}

impl SendError {
    /// Returns `true` if this error is a result of the channel being full.
    pub fn is_full(&self) -> bool {
        match self.kind {
            SendErrorKind::Full => true,
            _ => false,
        }
    }

    /// Returns `true` if this error is a result of the receiver being dropped.
    pub fn is_disconnected(&self) -> bool {
        match self.kind {
            SendErrorKind::Disconnected => true,
            _ => false,
        }
    }
}

/// The error type returned from [`try_send`](Sender::try_send).
#[derive(Clone, PartialEq, Eq)]
pub struct TrySendError<T> {
    err: SendError,
    val: T,
}

impl<T> From<mpsc::TrySendError<T>> for TrySendError<T> {
    fn from(error: mpsc::TrySendError<T>) -> Self {
        todo!()
    }
}

impl<T> TrySendError<T> {
    /// Returns `true` if this error is a result of the channel being full.
    pub fn is_full(&self) -> bool {
        self.err.is_full()
    }

    /// Returns `true` if this error is a result of the receiver being dropped.
    pub fn is_disconnected(&self) -> bool {
        self.err.is_disconnected()
    }

    /// Returns the message that was attempted to be sent but failed.
    pub fn into_inner(self) -> T {
        self.val
    }

    /// Drops the message and converts into a `SendError`.
    pub fn into_send_error(self) -> SendError {
        self.err
    }
}

pub(crate) trait SenderT<T> {
    fn send(&self, to_send: T) -> Result<(), TrySendError<T>>;
}

struct UnboundedSender<T>(mpsc::UnboundedSender<T>);

impl<T> UnboundedSender<T> {
    pub(crate) fn new(sender: mpsc::UnboundedSender<T>) -> Self {
        Self(sender)
    }
}

impl<T> SenderT<T> for UnboundedSender<T> {
    fn send(&self, to_send: T) -> Result<(), TrySendError<T>> {
        self.0.unbounded_send(to_send).map_err(TrySendError::from)
    }
}

pub(crate) struct SenderImpl<T, S = UnboundedSender<T>>(S, PhantomData<T>);

impl<T, S> SenderImpl<T, S> {
    pub(crate) fn new(sender: S) -> Self {
        Self(sender, PhantomData)
    }
}

impl<T, S: SenderT<T>> SenderImpl<T, S> {
    fn send(&self, to_send: T) -> Result<(), TrySendError<T>> {
        self.0.send(to_send)
    }
}

pub(crate) struct ReceiverImpl<T, R = mpsc::UnboundedReceiver<T>>(R, PhantomData<T>);

impl<T, S: StreamExt<Item = T> + Unpin> ReceiverImpl<S, T> {
    fn next(&mut self) -> Next<'_, S> {
        self.0.next()
    }
}

impl<T, S: StreamExt<Item = T> + Default> ReceiverImpl<T, S> {
    pub(crate) fn new() -> Self {
        Self(S::default(), PhantomData)
    }
}

impl<T, S: StreamExt<Item = T> + Default> Default for ReceiverImpl<T, S> {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) type Receiver<T> = mpsc::UnboundedReceiver<T>;
pub(crate) type Sender<T> = mpsc::UnboundedSender<T>;
