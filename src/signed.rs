use crate::{
    nodes::{NodeCount, NodeIndex, NodeMap},
    Index,
};
use async_trait::async_trait;
use codec::{Decode, Encode, Error, Input, Output};
use log::warn;
use std::fmt::Debug;
/// The type used as a signature.
///
/// The Signature typically does not contain the index of the node who signed the data.
pub trait Signature: Debug + Clone + Encode + Decode + Send + Sync + Eq {}

impl<T: Debug + Clone + Encode + Decode + Send + Sync + Eq> Signature for T {}

struct ForAll;

trait Plug<A> {
    type Plugged;
}

trait SignedT<T: Signable, KB: KeyBox<T>> {
    fn as_signable(&self) -> &T;

    fn signature(&self) -> &KB::Signature;

    fn into_signable(self) -> (T, KB::Signature);
}

/// Abstraction of the signing data and verifying signatures.
///
/// A typical implementation of KeyBox would be a collection of `N` public keys,
/// an index `i` and a single private key corresponding to the public key number `i`.
/// The meaning of sign is then to produce a signature `s` using the given private key,
/// and `verify(msg, s, j)` is to verify whether the signature s under the message msg is
/// correct with respect to the public key of the jth node.
#[async_trait]
pub trait KeyBox<T: Signable>: Index + Clone + Send + Sync {
    type Signature: Signature;
    type Signed: SignedT<T, Self>;
    type UncheckedSigned: SignedT<T, Self>;
    type Error;

    /// Returns the total number of known public keys.
    fn node_count(&self) -> NodeCount;

    /// Signs a message `msg`.
    async fn sign(&self, msg: T) -> Self::Signed;

    /// Verifies whether a node with `index` correctly signed the message `msg`.
    async fn verify(
        &self,
        unchecked: Self::UncheckedSigned,
        index: NodeIndex,
    ) -> Result<Self::Signed, (Self::Error, Self::UncheckedSigned)>;
}

/// A type to which signatures can be aggregated.
///
/// Any signature can be added to multisignature.
/// After adding sufficiently many signatures, the partial multisignature becomes a "complete"
/// multisignature.
/// Whether a multisignature is complete, can be verified with [`MultiKeychain::is_complete`] method.
/// The signature and the index passed to the `add_signature` method are required to be valid.
pub trait PartialMultisignature: Signature {
    type Signature: Signature;
    /// Adds the signature.
    fn add_signature(self, signature: &Self::Signature, index: NodeIndex) -> Self;
}

/// Extends KeyBox with multisigning functionalities.
///
/// A single Signature can be rised to a Multisignature.
/// Allows to verify whether a partial multisignature is complete (and valid).
pub trait MultiKeychain<T>: KeyBox<T> {
    type PartialMultisignature: PartialMultisignature<Signature = Self::Signature>;
    /// Transform a single signature to a multisignature consisting of the signature.
    fn from_signature(
        &self,
        signature: &Self::Signature,
        index: NodeIndex,
    ) -> Self::PartialMultisignature;
    /// Checks if enough signatures have beed added.
    fn is_complete(&self, msg: &[u8], partial: &Self::PartialMultisignature) -> bool;
}

/// Data which can be signed.
///
/// Signable data should provide a hash of type [`Self::Hash`] which is build from all parts of the
/// data which should be signed. The type [`Self::Hash`] should implement [`AsRef<[u8]>`], and
/// the bytes returned by `hash.as_ref()` are used by a [`MultiKeychain`] to sign the data.
pub trait Signable {
    type Hash: AsRef<[u8]>;
    /// Return a hash for signing.
    fn hash(&self) -> Self::Hash;
}

impl<T: AsRef<[u8]> + Clone> Signable for T {
    type Hash = T;
    fn hash(&self) -> Self::Hash {
        self.clone()
    }
}

/// A pair consisting of an instance of the `Signable` trait and an (arbitrary) signature.
///
/// The method `[UncheckedSigned::check]` can be used to upgrade this `struct` to
/// `[Signed<T, KB>]` which ensures that the signature matches the signed object.
#[derive(Clone, Debug, Decode, Encode, PartialEq, Eq, Hash)]
pub struct UncheckedSigned<T: Signable, S: Signature> {
    signable: T,
    signature: S,
}

impl<T: Signable, S: Signature> UncheckedSigned<T, S> {
    pub fn as_signable(&self) -> &T {
        &self.signable
    }

    pub fn into_signable(self) -> T {
        self.signable
    }

    pub fn signature(&self) -> S {
        self.signature.clone()
    }
}

impl<T: Signable, S: Signature> UncheckedSigned<Indexed<T>, S> {
    pub(crate) fn as_signable_strip_index(&self) -> &T {
        &self.signable.signable
    }
}

#[cfg(test)]
impl<T: Signable, S: Signature> UncheckedSigned<T, S> {
    pub(crate) fn new(signable: T, signature: S) -> Self {
        UncheckedSigned {
            signable,
            signature,
        }
    }
    pub(crate) fn new_with_index(
        signable: T,
        index: NodeIndex,
        signature: S,
    ) -> UncheckedSigned<Indexed<T>, S> {
        UncheckedSigned::new(Indexed::new(signable, index), signature)
    }
    pub(crate) fn as_signable_mut(&mut self) -> &mut T {
        &mut self.signable
    }
    pub(crate) fn signature_mut(&mut self) -> &mut S {
        &mut self.signature
    }
}

/// Error type returned when a verification of a signature fails.
#[derive(Clone, Debug)]
pub struct SignatureError<T: Signable, S: Signature> {
    pub unchecked: UncheckedSigned<T, S>,
}

impl<T: Signable + Index, S: Signature> UncheckedSigned<T, S> {
    /// Verifies whether the signature matches the key with the index as in the signed data.
    pub fn check<KB: KeyBox<T, Signature = S>>(
        self,
        key_box: &KB,
    ) -> Result<Signed<T, KB>, SignatureError<T, S>> {
        let index = self.signable.index();
        if !key_box.verify(self.signable.hash().as_ref(), &self.signature, index) {
            return Err(SignatureError { unchecked: self });
        }
        Ok(Signed { unchecked: self })
    }
}

impl<T: Signable + Index, S: Signature> Index for UncheckedSigned<T, S> {
    fn index(&self) -> NodeIndex {
        self.signable.index()
    }
}

impl<T: Signable, S: PartialMultisignature> UncheckedSigned<T, S> {
    /// Verifies whether the multisignature matches the signed data.
    pub fn check_multi<MK: MultiKeychain<T, PartialMultisignature = S>>(
        self,
        keychain: &MK,
    ) -> Result<Multisigned<T, MK>, SignatureError<T, S>> {
        if !(keychain.is_complete(self.signable.hash().as_ref(), &self.signature)) {
            return Err(SignatureError { unchecked: self });
        }
        Ok(Multisigned { unchecked: self })
    }
}

impl<T: Signable, S: Signature> UncheckedSigned<Indexed<T>, S> {
    fn strip_index(self) -> UncheckedSigned<T, S> {
        UncheckedSigned {
            signable: self.signable.strip_index(),
            signature: self.signature,
        }
    }
}

impl<T: Signable, S: Signature> From<UncheckedSigned<Indexed<T>, S>> for UncheckedSigned<T, S> {
    fn from(us: UncheckedSigned<Indexed<T>, S>) -> Self {
        us.strip_index()
    }
}

/// A correctly signed object of type `T`.
///
/// The correctness is guaranteed by storing a (phantom) reference to the `KeyBox` that verified
/// the signature.
#[derive(Debug)]
pub struct Signed<T: Signable + Index, KB: KeyBox<T>> {
    unchecked: UncheckedSigned<T, KB::Signature>,
}

impl<T: Signable + Index, KB: KeyBox<T>, A: Signable + Index> Plug<A> for Signed<T, KB> {
    type Plugged = Signed<A, KB>;
}

impl<T: Signable + Clone + Index, KB: KeyBox<T>> Clone for Signed<T, KB> {
    fn clone(&self) -> Self {
        Signed {
            unchecked: self.unchecked.clone(),
        }
    }
}

impl<T: Signable + Index, KB: KeyBox<T>> Signed<T, KB> {
    /// Create a signed object from a signable. The index of `signable` must match the index of the `key_box`.
    pub async fn sign(signable: T, key_box: &KB) -> Signed<T, KB> {
        assert_eq!(signable.index(), key_box.index());
        let signature = key_box.sign(signable.hash().as_ref()).await;
        Signed {
            unchecked: UncheckedSigned {
                signable,
                signature,
            },
        }
    }

    /// Get a reference to the signed object.
    pub fn as_signable(&self) -> &T {
        &self.unchecked.signable
    }

    pub fn into_signable(self) -> T {
        self.unchecked.signable
    }

    pub(crate) fn into_unchecked(self) -> UncheckedSigned<T, KB::Signature> {
        self.unchecked
    }
}

impl<T: Signable, KB: KeyBox<T>> Signed<Indexed<T>, KB> {
    /// Create a signed object from a signable. The index is added based on the index of the `key_box`.
    pub async fn sign_with_index(signable: T, key_box: &KB) -> Signed<Indexed<T>, KB> {
        Signed::sign(Indexed::new(signable, key_box.index()), key_box).await
    }
}

impl<T: Signable, MK: MultiKeychain<T>> Signed<Indexed<T>, MK> {
    /// Transform a singly signed object into a partially multisigned consisting of just the signed object.
    /// Note that depending on the setup, it may yield a complete signature.
    pub fn into_partially_multisigned(self, keychain: &MK) -> PartiallyMultisigned<T, MK> {
        let multisignature =
            keychain.from_signature(&self.unchecked.signature, self.unchecked.signable.index);
        let unchecked = UncheckedSigned {
            signable: self.unchecked.signable.strip_index(),
            signature: multisignature,
        };
        if keychain.is_complete(unchecked.signable.hash().as_ref(), &unchecked.signature) {
            PartiallyMultisigned::Complete {
                multisigned: Multisigned { unchecked },
            }
        } else {
            PartiallyMultisigned::Incomplete { unchecked }
        }
    }
}

impl<T: Signable + Index, KB: KeyBox<T>> From<Signed<T, KB>> for UncheckedSigned<T, KB::Signature> {
    fn from(signed: Signed<T, KB>) -> Self {
        signed.into_unchecked()
    }
}

/// A pair consistsing of signable data and a [`NodeIndex`].
///
/// This is a wrapper used for signing data which does not implement the [`Index`] trait.
/// If a node with an index `i` needs to sign some data `signable` which does not
/// implement the [`Index`] trait, it should use the `Signed::sign_with_index` method which will
/// use this wrapper transparently. Note that in the implementation of `Signable` for `Indexed<T>`,
/// the hash is the hash of the underlying data `T`. Therefore, instances of the type
/// [`Signed<Indexed<T>, MK>`] can be aggregated into `Multisigned<T, MK>`
#[derive(Clone, Encode, Decode, Debug, PartialEq, Eq, Hash)]
pub struct Indexed<T: Signable> {
    signable: T,
    index: NodeIndex,
}

impl<T: Signable> Indexed<T> {
    fn new(signable: T, index: NodeIndex) -> Self {
        Indexed { signable, index }
    }

    fn strip_index(self) -> T {
        self.signable
    }
}

impl<T: Signable> Signable for Indexed<T> {
    type Hash = T::Hash;

    fn hash(&self) -> Self::Hash {
        self.signable.hash()
    }
}

impl<T: Signable> Index for Indexed<T> {
    fn index(&self) -> NodeIndex {
        self.index
    }
}

/// Signable data together with a complete multisignature.
///
/// An instance of `Multisigned<T: Signable, MK: MultiKeychain>` consists of a data of type `T`
/// together with a multisignature which is valid and complete according to a multikeychain
/// reference `&MK`. The lifetime parameter ensures that the data with a multisignature do not
/// outlive the session.
#[derive(Debug)]
pub struct Multisigned<T: Signable, MK: MultiKeychain<T>> {
    unchecked: UncheckedSigned<T, MK::PartialMultisignature>,
}

impl<T: Signable, MK: MultiKeychain<T>> Multisigned<T, MK> {
    /// Get a reference to the multisigned object.
    pub fn as_signable(&self) -> &T {
        &self.unchecked.signable
    }

    pub fn into_unchecked(self) -> UncheckedSigned<T, MK::PartialMultisignature> {
        self.unchecked
    }
}

impl<T: Signable, MK: MultiKeychain<T>> From<Multisigned<T, MK>>
    for UncheckedSigned<T, MK::PartialMultisignature>
{
    fn from(signed: Multisigned<T, MK>) -> Self {
        signed.into_unchecked()
    }
}

impl<T: Signable + Clone, MK: MultiKeychain<T>> Clone for Multisigned<T, MK> {
    fn clone(&self) -> Self {
        Multisigned {
            unchecked: self.unchecked.clone(),
        }
    }
}

#[derive(Debug)]
pub struct IncompleteMultisignatureError<T: Signable, MK: MultiKeychain<T>> {
    pub partial: PartiallyMultisigned<T, MK>,
}

/// Signable data together with a valid partial multisignature.
///
/// Instances of this type keep track whether the partial multisignautre is complete or not.
/// If the multisignature is complete, you can get [`Multisigned`] by pattern matching
/// against the variant [`PartiallyMultisigned::Complete`].
#[derive(Debug)]
pub enum PartiallyMultisigned<T: Signable, MK: MultiKeychain<T>> {
    Incomplete {
        unchecked: UncheckedSigned<T, MK::PartialMultisignature>,
    },
    Complete {
        multisigned: Multisigned<T, MK>,
    },
}

impl<T: Signable, MK: MultiKeychain<T>> PartiallyMultisigned<T, MK> {
    /// Create a partially multisigned object.
    pub async fn sign(signable: T, keychain: &MK) -> PartiallyMultisigned<T, MK> {
        Signed::sign_with_index(signable, keychain)
            .await
            .into_partially_multisigned(keychain)
    }

    /// Chceck if the partial multisignature is complete.
    pub fn is_complete(&self) -> bool {
        match self {
            PartiallyMultisigned::Incomplete { .. } => false,
            PartiallyMultisigned::Complete { .. } => true,
        }
    }

    /// Get a reference to the multisigned object.
    pub fn as_signable(&self) -> &T {
        match self {
            PartiallyMultisigned::Incomplete { unchecked } => unchecked.as_signable(),
            PartiallyMultisigned::Complete { multisigned } => multisigned.as_signable(),
        }
    }

    /// Return the object that is being signed.
    pub fn into_unchecked(self) -> UncheckedSigned<T, MK::PartialMultisignature> {
        match self {
            PartiallyMultisigned::Incomplete { unchecked } => unchecked,
            PartiallyMultisigned::Complete { multisigned } => multisigned.unchecked,
        }
    }

    /// Adds a signature and checks if multisignature is complete.
    pub fn add_signature(self, signed: Signed<Indexed<T>, MK>, keychain: &MK) -> Self {
        if self.as_signable().hash().as_ref() != signed.as_signable().hash().as_ref() {
            warn!(target: "AlephBFT-signed", "Tried to add a signature of a different object");
            return self;
        }
        match self {
            PartiallyMultisigned::Incomplete { mut unchecked } => {
                unchecked.signature = unchecked
                    .signature
                    .add_signature(&signed.unchecked.signature, signed.unchecked.signable.index);
                if keychain.is_complete(unchecked.signable.hash().as_ref(), &unchecked.signature) {
                    PartiallyMultisigned::Complete {
                        multisigned: Multisigned { unchecked },
                    }
                } else {
                    PartiallyMultisigned::Incomplete { unchecked }
                }
            }
            PartiallyMultisigned::Complete { .. } => self,
        }
    }
}

/// A set of signatures of a subset of nodes serving as a (partial) multisignature
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignatureSet<S: Signature> {
    signatures: NodeMap<Option<S>>,
}

impl<S: Signature> Encode for SignatureSet<S> {
    fn size_hint(&self) -> usize {
        self.signatures.size_hint()
    }

    fn encode_to<T: Output + ?Sized>(&self, dest: &mut T) {
        self.signatures.encode_to(dest);
    }

    fn encode(&self) -> Vec<u8> {
        self.signatures.encode()
    }

    fn using_encoded<R, F: FnOnce(&[u8]) -> R>(&self, f: F) -> R {
        self.signatures.using_encoded(f)
    }
}

impl<S: Signature> Decode for SignatureSet<S> {
    fn decode<I: Input>(input: &mut I) -> Result<Self, Error> {
        let signatures = NodeMap::decode(input)?;
        Ok(SignatureSet { signatures })
    }
}

impl<S: Signature> SignatureSet<S> {
    /// Construct an empty set of signatures for a committee of a given size.
    pub(crate) fn new(len: NodeCount) -> Self {
        SignatureSet {
            signatures: NodeMap::new_with_len(len),
        }
    }
}

impl<S: Signature> PartialMultisignature for SignatureSet<S> {
    type Signature = S;

    fn add_signature(mut self, signature: &Self::Signature, index: NodeIndex) -> Self {
        self.signatures[index] = Some(signature.clone());
        self
    }
}

/// Keybox wrapper which implements MultiKeychain such that a partial multisignature is a list of
/// signatures and a partial multisignature is considered complete if it contains more than 2N/3 signatures.
///
/// Note: this way of multisigning is very inefficient, and should be used only for testing.
#[derive(Debug, Clone)]
pub struct DefaultMultiKeychain<T: Signable, KB: KeyBox<T>> {
    key_box: KB,
}

impl<T: Signable, KB: KeyBox<T>> DefaultMultiKeychain<T, KB> {
    // Create a new `DefaultMultiKeychain` using the provided `KeyBox`.
    pub fn new(key_box: KB) -> Self {
        DefaultMultiKeychain { key_box }
    }

    fn quorum(&self) -> usize {
        2 * self.node_count().0 / 3 + 1
    }
}

impl<T: Signable, KB: KeyBox<T>> Index for DefaultMultiKeychain<T, KB> {
    fn index(&self) -> NodeIndex {
        self.key_box.index()
    }
}

#[async_trait::async_trait]
impl<T: Signable, KB: KeyBox<T>> KeyBox<T> for DefaultMultiKeychain<T, KB> {
    type Signature = KB::Signature;
    type Signed = KB::Signed;
    type UncheckedSigned = KB::UncheckedSigned;
    type Error = ();

    async fn sign(&self, msg: T) -> Self::Signed {
        self.key_box.sign(msg).await
    }

    fn node_count(&self) -> NodeCount {
        self.key_box.node_count()
    }

    async fn verify(
        &self,
        sgn: &Self::UncheckedSigned,
        index: NodeIndex,
    ) -> Result<Self::Signed, (Self::Error, Self::UncheckedSigned)> {
        self.key_box.verify(sgn, index)
    }
}

impl<T, KB: KeyBox<T>> MultiKeychain<T> for DefaultMultiKeychain<T, KB> {
    type PartialMultisignature = SignatureSet<KB::Signature>;

    fn from_signature(
        &self,
        signature: &Self::Signature,
        index: NodeIndex,
    ) -> Self::PartialMultisignature {
        SignatureSet::add_signature(SignatureSet::new(self.node_count()), signature, index)
    }

    fn is_complete(&self, msg: &[u8], partial: &Self::PartialMultisignature) -> bool {
        let signature_count = partial.signatures.iter().flatten().count();
        if signature_count < self.quorum() {
            return false;
        }
        partial.signatures.enumerate().all(|(i, sgn)| {
            sgn.as_ref()
                .map_or(true, |sgn| self.key_box.verify(msg, sgn, i))
        })
    }
}

pub(crate) mod owned_keybox {
    use crate::{
        Index, KeyBox, MultiKeychain, NodeCount, NodeIndex, PartialMultisignature, Signable,
    };
    use codec::{Decode, Encode};
    use std::marker::PhantomData;

    #[derive(Eq, PartialEq, Debug, Decode, Encode)]
    struct InvariantLifetime<'id>(PhantomData<fn(*mut &'id ()) -> ()>);

    impl<'id> InvariantLifetime<'id> {
        fn new() -> Self {
            InvariantLifetime(PhantomData)
        }
    }

    impl<'id> Copy for InvariantLifetime<'id> {}

    impl<'id> Clone for InvariantLifetime<'id> {
        fn clone(&self) -> Self {
            *self
        }
    }

    #[derive(Clone, Eq, PartialEq, Debug, Decode, Encode)]
    pub struct Owned<'a, T> {
        inner: T,
        _marker: InvariantLifetime<'a>,
    }

    impl<'a, T: Clone> Owned<'a, T> {
        pub(crate) fn new<R>(value: T, f: impl for<'new_id> FnOnce(Owned<'new_id, T>) -> R) -> R {
            let owned = Owned {
                inner: value,
                _marker: InvariantLifetime::new(),
            };
            f(owned)
        }

        fn own<TT: Clone>(&self, value: TT) -> Owned<'a, TT> {
            Owned {
                inner: value,
                _marker: InvariantLifetime::new(),
            }
        }

        // TODO zamiast tego konwertuj Owned w jakis bezuzyteczny typ, i.e. NotOwned, ale spelniajacy wszystkie wymagania dla networka i nie posiadajacy zadnych lifetimeow
        pub(crate) fn unsafe_into(self) -> T {
            self.inner
        }
    }

    // impl<'a, T> From<Owned<'a, T>> for NotOwned<T> {
    //     fn from(owned: Owned<'a, T>) -> Self {
    //         Self { inner: owned.inner }
    //     }
    // }

    // #[derive(Clone, Eq, PartialEq, Debug, Decode, Encode)]
    // pub struct NotOwned<T> {
    //     inner: T,
    // }

    // impl<T> NotOwned<T> {
    //     fn new(value: T) -> Self {
    //         Self { inner: value }
    //     }
    // }

    // TODO we can cheat a bit, and new can provide a closure for retrieving Owned::inner, but it shouldn't escape anywhere
    #[async_trait::async_trait]
    impl<'id, T: Signable, KB: KeyBox<T>> KeyBox<T> for Owned<'id, KB> {
        type Signature = KB::Signature;
        type Signed = Owned<'id, KB::Signed>;
        type UncheckedSigned = KB::UncheckedSigned;
        type Error = KB::Error;

        fn node_count(&self) -> NodeCount {
            self.inner.node_count()
        }

        async fn sign(&self, msg: T) -> Self::Signed {
            let signature = self.inner.sign(msg).await;
            Owned::own(self, signature)
        }

        async fn verify(
            &self,
            unchecked: Self::UncheckedSigned,
            index: NodeIndex,
        ) -> Result<Self::Signed, (Self::Error, Self::UncheckedSigned)> {
            self.inner
                .verify(unchecked, index)
                .map(|signed| Owned::own(self, signed))
        }
    }

    impl<'id, T: Signable, KB: KeyBox<T>> Index for Owned<'id, KB> {
        fn index(&self) -> NodeIndex {
            self.inner.index()
        }
    }

    // impl<S: PartialMultisignature> PartialMultisignature for NotOwned<S> {
    //     type Signature = NotOwned<S::Signature>;

    //     fn add_signature(self, signature: &Self::Signature, index: NodeIndex) -> Self {
    //         NotOwned::new(self.inner.add_signature(&signature.inner, index))
    //     }
    // }

    impl<'id, S: PartialMultisignature> PartialMultisignature for Owned<'id, S> {
        type Signature = Owned<'id, S::Signature>;

        fn add_signature(self, signature: &Self::Signature, index: NodeIndex) -> Self {
            let phantom = Owned::own(signature, PhantomData as PhantomData<()>);
            Owned::own(&phantom, self.inner.add_signature(&signature.inner, index))
        }
    }

    impl<'id, T: Signable, KB: MultiKeychain<T>> MultiKeychain<T> for Owned<'id, KB> {
        type PartialMultisignature = Owned<'id, KB::PartialMultisignature>;

        fn from_signature(
            &self,
            signature: &Self::Signature,
            index: NodeIndex,
        ) -> Self::PartialMultisignature {
            let partial_signature = self.inner.from_signature(&signature.inner, index);
            Owned::own(self, partial_signature)
        }

        fn is_complete(&self, msg: &[u8], partial: &Self::PartialMultisignature) -> bool {
            self.inner.is_complete(msg, &partial.inner)
        }
    }
}
