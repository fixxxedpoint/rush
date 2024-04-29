use async_trait::async_trait;

use crate::{Data, Hasher, NodeIndex, Round};

/// The source of data items that consensus should order.
///
/// AlephBFT internally calls [`DataProvider::get_data`] whenever a new unit is created and data
/// needs to be placed inside.
///
/// We refer to the documentation
/// https://cardinal-cryptography.github.io/AlephBFT/aleph_bft_api.html for a discussion and
/// examples of how this trait can be implemented.
#[async_trait]
pub trait DataProvider: Sync + Send + 'static {
    /// Type of data returned by this provider.
    type Output: Data;
    /// Outputs a new data item to be ordered.
    async fn get_data(&mut self) -> Option<Self::Output>;
}

/// The source of finalization of the units that consensus produces.
///
/// The [`FinalizationHandler::data_finalized`] method is called whenever a piece of data input
/// to the algorithm using [`DataProvider::get_data`] has been finalized, in order of finalization.
pub trait FinalizationHandler<D>: Sync + Send + 'static {
    /// Data, provided by [DataProvider::get_data], has been finalized.
    /// The calls to this function follow the order of finalization.
    fn data_finalized(&mut self, data: D);
}

/// Represents state of the main internal data structure of AlephBFT (i.e. direct acyclic graph) used for
/// achieving consensus.
///
/// Instances of this type are returned indirectly by [`member::run_session_for_units`] method using the
/// [`UnitFinalizationHandler`]. This way it allows to reconstruct the DAG's structure used by AlephBFT,
/// which can be then used for example for the purpose of node's performance evaluation.
pub struct OrderedUnit<D: Data, H: Hasher> {
    pub data: Option<D>,
    pub parents: Vec<H::Hash>,
    pub hash: H::Hash,
    pub creator: NodeIndex,
    pub round: Round,
}

pub type BatchOfUnits<D, H> = Vec<OrderedUnit<D, H>>;

impl<D: Data, H: Hasher, FH: FinalizationHandler<D>> FinalizationHandler<BatchOfUnits<D, H>>
    for FH
{
    fn data_finalized(&mut self, batch: BatchOfUnits<D, H>) {
        for unit in batch {
            if let Some(data) = unit.data {
                self.data_finalized(data)
            }
        }
    }
}
