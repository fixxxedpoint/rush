use aleph_bft_crypto::NodeMap;
use async_trait::async_trait;

use crate::{NodeIndex, Round};

/// The source of data items that consensus should order.
///
/// AlephBFT internally calls [`DataProvider::get_data`] whenever a new unit is created and data needs to be placed inside.
///
/// We refer to the documentation https://cardinal-cryptography.github.io/AlephBFT/aleph_bft_api.html for a discussion
/// and examples of how this trait can be implemented.
#[async_trait]
pub trait DataProvider<Data>: Sync + Send + 'static {
    /// Outputs a new data item to be ordered
    async fn get_data(&mut self) -> Option<Data>;
}

/// Represents state of the main internal data structure of AlephBFT (direct acyclic graph) used for achieving distributed
/// consensus.
///
/// Instances of this type are returned by implementations of the [`UnitFinalizationHandler`] trait. This way it allows to
/// reconstruct the DAG's structure used by AlephBFT, which can be then used for example for the purpose of node's performance
/// evaluation.
pub struct OrderedUnit<Data, Hash> {
    pub data: Option<Data>,
    pub parents: NodeMap<Hash>,
    pub hash: Hash,
    pub creator: NodeIndex,
    pub round: Round,
}

impl<Data, Hash> OrderedUnit<Data, Hash> {
    /// Constructs an instance the `OrderedUnit` type. It represents state of the AlephBFT's internal DAG data structure.
    pub fn new(
        data: Option<Data>,
        parents: NodeMap<Hash>,
        hash: Hash,
        creator: NodeIndex,
        round: Round,
    ) -> Self {
        Self {
            data,
            parents,
            hash,
            creator,
            round,
        }
    }
}

impl<Data, Hash> From<OrderedUnit<Data, Hash>> for Option<Data> {
    fn from(unit: OrderedUnit<Data, Hash>) -> Self {
        unit.data
    }
}

/// The source of finalization of the units that consensus produces.
///
/// The [`FinalizationHandler::data_finalized`] method is called whenever a piece of data input to the algorithm
/// using [`DataProvider::get_data`] has been finalized, in order of finalization.
pub trait FinalizationHandler<Data, Hash>: Sync + Send + 'static {
    /// Data, provided by [DataProvider::get_data], has been finalized.
    /// The calls to this function follow the order of finalization.
    fn data_finalized(&mut self, _data: Data) {}

    /// A batch of units has been finalized. You can overwrite the default implementation for more advanced finalization
    /// handling in which case the method [`FinalizationHandler::data_finalized`] will not be called anymore if a unit is
    /// finalized. Please note that this interface is less stable as it exposes intrinsics which migh be subject to change. Do
    /// not implement this method and only implement [`FinalizationHandler::data_finalized`] unless you absolutely know what you
    /// are doing.
    fn batch_finalized(&mut self, batch: Vec<OrderedUnit<Data, Hash>>) {
        for unit in batch {
            let data = unit.into();
            if let Some(data) = data {
                self.data_finalized(data);
            }
        }
    }
}
