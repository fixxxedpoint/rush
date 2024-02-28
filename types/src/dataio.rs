use aleph_bft_crypto::NodeMap;
use async_trait::async_trait;

use crate::{NodeIndex, Hasher, Round};

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

/// The source of finalization of the units that consensus produces.
///
/// The [`FinalizationHandler::data_finalized`] method is called whenever a piece of data input to the algorithm
/// using [`DataProvider::get_data`] has been finalized, in order of finalization.
pub trait FinalizationHandler<Data>: Sync + Send + 'static {
    /// Data, provided by [DataProvider::get_data], has been finalized.
    /// The calls to this function follow the order of finalization.
    fn data_finalized(&mut self, data: Data, creator: NodeIndex);
}

pub trait Unit<Data, H: Hasher>: Into<Option<Data>> {
    fn creator(&self) -> NodeIndex;
    fn round(&self) -> Round;
    fn data(&self) -> &Option<Data>;
    fn parents(&self) -> &NodeMap<H::Hash>;
    fn hash(&self) -> H::Hash;
}

pub trait UnitFinalizationHandler<D> {
    fn batch_ordered<H, U>(&mut self, units: impl IntoIterator<Item = U>) where H: Hasher, U: Unit<D, H>;
}
