// This file is part of Substrate.

// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The transaction extension trait.

use crate::{
	scale_info::{MetaType, StaticTypeInfo, TypeInfo},
	traits::SignedExtension,
	transaction_validity::{
		InvalidTransaction, TransactionValidity, TransactionValidityError, ValidTransaction,
	},
	DispatchResult,
};
use codec::{Codec, Decode, Encode};
use impl_trait_for_tuples::impl_for_tuples;
use scale_info::Type;
use sp_core::{self, RuntimeDebug};
#[doc(hidden)]
pub use sp_std::marker::PhantomData;
use sp_std::{self, fmt::Debug, prelude::*};
use tuplex::{PopFront, PushBack};

use super::{DispatchInfoOf, Dispatchable, OriginOf, PostDispatchInfoOf};

mod as_transaction_extension;
mod dispatch_transaction;
mod simple_transaction_extension;
pub use as_transaction_extension::AsTransactionExtension;
pub use dispatch_transaction::DispatchTransaction;
pub use simple_transaction_extension::{SimpleTransactionExtension, WithSimple};

/// Shortcut for the result value of the `validate` function.
pub type ValidateResult<TE, Call> = Result<
	(ValidTransaction, <TE as TransactionExtension<Call>>::Val, OriginOf<Call>),
	TransactionValidityError,
>;

/// Means by which a transaction may be extended. This type embodies both the data and the logic
/// that should be additionally associated with the transaction. It should be plain old data.
///
/// The simplest transaction extension would be the Unit type (and empty pipeline) `()`. This
/// executes no additional logic and implies a dispatch of the transaction's call using
/// the inherited origin (either `None` or `Signed`, depending on whether this is a signed or
/// general transaction).
///
/// Transaction extensions are capable of altering certain associated semantics:
///
/// - They may define the origin with which the transaction's call should be dispatched.
/// - They may define various parameters used by the transction queue to determine under what
///   conditions the transaction should be retained and introduced on-chain.
/// - They may define whether this transaction is acceptable for introduction on-chain at all.
///
/// Each of these semantics are defined by the `validate` function.
///
/// **NOTE: Transaction extensions cannot under any circumctances alter the call itself.**
///
/// Transaction extensions are capable of defining logic which is executed additionally to the
/// dispatch of the call:
///
/// - They may define logic which must be executed prior to the dispatch of the call.
/// - They may also define logic which must be executed after the dispatch of the call.
///
/// Each of these semantics are defined by the `prepare` and `post_dispatch` functions respectively.
///
/// Finally, transaction extensions may define additional data to help define the implications of
/// the logic they introduce. This additional data may be explicitly defined by the transaction
/// author (in which case it is included as part of the transaction body), or it may be implicitly
/// defined by the transaction extension based around the on-chain state (which the transaction
/// author is assumed to know). This data may be utilized by the above logic to alter how a node's
/// transaction queue treats this transaction.
///
/// ## Pipelines, Inherited Implications, and Authorized Origins
///
/// Requiring a single transaction extension to define all of the above semantics would be
/// cumbersome and would lead to a lot of boilerplate. Instead, transaction extensions are
/// aggregated into pipelines, which are tuples of transaction extensions. Each extension in the
/// pipeline is executed in order, and the output of each extension is aggregated and/or relayed as
/// the input to the next extension in the pipeline.
///
/// This ordered composition happens with all datatypes ([Val], [Pre] and [Implicit]) as well as
/// all functions. There are important consequences stemming from how the composition affects the
/// meaning of the `origin` and `implication` parameters as well as the results. Whereas the
/// [prepare] and [post_dispatch] functions are clear in their meaning, the [validate] function is
/// sfairly sophisticated and warrants further explanation.
///
/// Firstly, the `origin` parameter. The `origin` passed into the first item in a pipeline is simply
/// that passed into the tuple itself. It represents an authority who has authorized the implication
/// of the transaction, as of the extension it has been passed into *and any further extensions it
/// may pass though, all the way to, and including, the transaction's dispatch call itself.
/// Each following item in the pipeline is passed the origin which the previous item returned. The
/// origin returned from the final item in the pipeline is the origin which is returned by the tuple
/// itself.
///
/// This means that if a constituent extension returns a different origin to the one it was called
/// with, then (assuming no other extension changes it further) *this new origin will be used for
/// all extensions following it in the pipeline, and will be returned from the pipeline to be used
/// as the origin for the call's dispatch*. The call itself as well as all these extensions
/// following may each imply consequence for this origin. We call this the *inherited implication*.
///
/// The *inherited implication* is the cumulated on-chain effects born by whatever origin is
/// returned. It is expressed to the [validate] function only as the `implication` argument which
/// implements the [Encode] trait. A transaction extension may define its own implications through
/// its own fields and the [implicit] function. This is only utilized by extensions which preceed
/// it in a pipeline or, if the transaction is an old-school signed trasnaction, the udnerlying
/// transaction verification logic.
///
/// **The inherited implication passed as the `implication` parameter to [validate] does not
/// include the extension's inner data itself nor does it include the result of the extension's
/// `implicit` function.** If you both provide an implication and rely on the implication, then you
/// need to manually aggregate your extensions implication with the aggregated implication passed
/// in.
pub trait TransactionExtension<Call: Dispatchable>:
	Codec + Debug + Sync + Send + Clone + Eq + PartialEq + StaticTypeInfo
{
	/// Unique identifier of this signed extension.
	///
	/// This will be exposed in the metadata to identify the signed extension used
	/// in an extrinsic.
	const IDENTIFIER: &'static str;

	/// The type that encodes information that can be passed from validate to prepare.
	type Val;

	/// The type that encodes information that can be passed from prepare to post-dispatch.
	type Pre;

	/// Any additional data which was known at the time of transaction construction and
	/// can be useful in authenticating the transaction. This is determined dynamically in part
	/// from the on-chain environment using the `implied` function and not directly contained in
	/// the transction itself and therefore is considered "implicit".
	type Implicit: Encode + StaticTypeInfo;

	/// Determine any additional data which was known at the time of transaction construction and
	/// can be useful in authenticating the transaction. The expected usage of this is to include
	/// in any data which is signed and verified as part of transactiob validation. Also perform
	/// any pre-signature-verification checks and return an error if needed.
	fn implicit(&self) -> Result<Self::Implicit, TransactionValidityError>;

	/// Validate a transaction for the transaction queue.
	///
	/// This function can be called frequently by the transaction queue to obtain transaction
	/// validity against current state. It should perform all checks that determine a valid
	/// transaction, that can pay for its execution and quickly eliminate ones that are stale or
	/// incorrect.
	///
	/// Parameters:
	/// - `origin`: The origin of the transaction which this extension inherited; coming from an
	///   "old-school" *signed transaction*, this will be a system `RawOrigin::Signed` value. If the
	///   transaction is a "new-school" *General Transaction*, then this will be a system
	///   `RawOrigin::None` value. If this extension is an item in a composite, then it could be
	///   anything which was previously returned as an `origin` value in the result of a `validate`
	///   call.
	/// - `call`: The call which will ultimately be dispatched by this transaction.
	/// - `info`: Information concerning, and inherent to, the `call`.
	/// - `len`: The total length of the encoded transaction.
	/// - `implication`: The *implication* which this extension inherits. Coming directly from a
	///   transaction, this is simply the transaction's `call`. However, if this extension is
	///   expressed as part of a composite type, then this is equal to any further implications to
	///   which the returned `origin` could potentially apple. See Pipelines, Inherited
	///   Implications, and Authorized Origins for more information.
	///
	/// Returns a [ValidateResult], which is a [Result] whose success type is a tuple of
	/// [ValidTransaction] (defining useful metadata for the transaction queue), the [Self::Val]
	/// token of this transaction, which gets passed into [prepare], and the origin of the
	/// transaction, which gets passed into [prepare] and is ultimately used for dispatch.
	fn validate(
		&self,
		origin: OriginOf<Call>,
		call: &Call,
		info: &DispatchInfoOf<Call>,
		len: usize,
		self_implicit: Self::Implicit,
		inherited_implication: &impl Encode,
	) -> ValidateResult<Self, Call>;

	/// Do any pre-flight stuff for a transaction after validation.
	///
	/// This is for actions which do not happen in the transaction queue but only immediately prior
	/// to the point of dispatch on-chain. This should not return an error, since errors
	/// should already have been identified during the [validate] call. If an error is returned,
	/// the transaction will be considered invalid.
	///
	/// Unlike `validate`, this function may consume `self`.
	///
	/// Checks made in validation need not be repeated here.
	fn prepare(
		self,
		val: Self::Val,
		origin: &OriginOf<Call>,
		call: &Call,
		info: &DispatchInfoOf<Call>,
		len: usize,
	) -> Result<Self::Pre, TransactionValidityError>;

	/// Do any post-flight stuff for an extrinsic.
	///
	/// `_pre` contains the output of `prepare`.
	///
	/// This gets given the `DispatchResult` `_result` from the extrinsic and can, if desired,
	/// introduce a `TransactionValidityError`, causing the block to become invalid for including
	/// it.
	///
	/// WARNING: It is dangerous to return an error here. To do so will fundamentally invalidate the
	/// transaction and any block that it is included in, causing the block author to not be
	/// compensated for their work in validating the transaction or producing the block so far.
	///
	/// It can only be used safely when you *know* that the extrinsic is one that can only be
	/// introduced by the current block author; generally this implies that it is an inherent and
	/// will come from either an offchain-worker or via `InherentData`.
	fn post_dispatch(
		_pre: Self::Pre,
		_info: &DispatchInfoOf<Call>,
		_post_info: &PostDispatchInfoOf<Call>,
		_len: usize,
		_result: &DispatchResult,
	) -> Result<(), TransactionValidityError> {
		Ok(())
	}

	/// Returns the metadata for this extension.
	///
	/// As a [`TransactionExtension`] can be a tuple of [`TransactionExtension`]s we need to return
	/// a `Vec` that holds the metadata of each one. Each individual `TransactionExtension` must
	/// return *exactly* one [`TransactionExtensionMetadata`].
	///
	/// This method provides a default implementation that returns a vec containing a single
	/// [`TransactionExtensionMetadata`].
	fn metadata() -> Vec<TransactionExtensionMetadata> {
		sp_std::vec![TransactionExtensionMetadata {
			identifier: Self::IDENTIFIER,
			ty: scale_info::meta_type::<Self>(),
			// TODO: Metadata-v16: Rename to "implicit"
			additional_signed: scale_info::meta_type::<Self::Implicit>()
		}]
	}

	/// Compatibility function for supporting the `SignedExtension::validate_unsigned` function.
	///
	/// DO NOT USE! THIS MAY BE REMOVED AT ANY TIME!
	#[deprecated = "Only for compatibility. DO NOT USE."]
	fn validate_bare_compat(
		_call: &Call,
		_info: &DispatchInfoOf<Call>,
		_len: usize,
	) -> TransactionValidity {
		Ok(ValidTransaction::default())
	}

	/// Compatibility function for supporting the `SignedExtension::pre_dispatch_unsigned` function.
	///
	/// DO NOT USE! THIS MAY BE REMOVED AT ANY TIME!
	#[deprecated = "Only for compatibility. DO NOT USE."]
	fn pre_dispatch_bare_compat(
		_call: &Call,
		_info: &DispatchInfoOf<Call>,
		_len: usize,
	) -> Result<(), TransactionValidityError> {
		Ok(())
	}

	/// Compatibility function for supporting the `SignedExtension::post_dispatch` function where
	/// `pre` is `None`.
	///
	/// DO NOT USE! THIS MAY BE REMOVED AT ANY TIME!
	#[deprecated = "Only for compatibility. DO NOT USE."]
	fn post_dispatch_bare_compat(
		_info: &DispatchInfoOf<Call>,
		_post_info: &PostDispatchInfoOf<Call>,
		_len: usize,
		_result: &DispatchResult,
	) -> Result<(), TransactionValidityError> {
		Ok(())
	}
}

/// Implict
#[macro_export]
macro_rules! impl_tx_ext_default {
	($call:ty ; , $( $rest:tt )*) => {
		impl_tx_ext_default!{$call ; $( $rest )*}
	};
	($call:ty ; implicit $( $rest:tt )*) => {
		fn implicit(&self) -> Result<Self::Implicit, $crate::TransactionValidityError> {
			Ok(Default::default())
		}
		impl_tx_ext_default!{$call ; $( $rest )*}
	};
	($call:ty ; validate $( $rest:tt )*) => {
		fn validate(
			&self,
			origin: $crate::traits::OriginOf<$call>,
			_call: &$call,
			_info: &$crate::traits::DispatchInfoOf<$call>,
			_len: usize,
			_self_implicit: Self::Implicit,
			_inherited_implication: &impl $crate::codec::Encode,
		) -> $crate::traits::ValidateResult<Self, $call> {
			Ok((Default::default(), Default::default(), origin))
		}
		impl_tx_ext_default!{$call ; $( $rest )*}
	};
	($call:ty ; prepare $( $rest:tt )*) => {
		fn prepare(
			self,
			_val: Self::Val,
			_origin: &$crate::traits::OriginOf<$call>,
			_call: &$call,
			_info: &$crate::traits::DispatchInfoOf<$call>,
			_len: usize,
		) -> Result<Self::Pre, $crate::TransactionValidityError> {
			Ok(Default::default())
		}
		impl_tx_ext_default!{$call ; $( $rest )*}
	};
	($call:ty ;) => {};
}

/// Information about a [`TransactionExtension`] for the runtime metadata.
pub struct TransactionExtensionMetadata {
	/// The unique identifier of the [`TransactionExtension`].
	pub identifier: &'static str,
	/// The type of the [`TransactionExtension`].
	pub ty: MetaType,
	/// The type of the [`TransactionExtension`] additional signed data for the payload.
	// TODO: Rename "implicit"
	pub additional_signed: MetaType,
}

#[impl_for_tuples(1, 12)]
impl<Call: Dispatchable> TransactionExtension<Call> for Tuple {
	for_tuples!( where #( Tuple: TransactionExtension<Call> )* );
	const IDENTIFIER: &'static str = "Use `metadata()`!";
	for_tuples!( type Val = ( #( Tuple::Val ),* ); );
	for_tuples!( type Pre = ( #( Tuple::Pre ),* ); );
	for_tuples!( type Implicit = ( #( Tuple::Implicit ),* ); );
	fn implicit(&self) -> Result<Self::Implicit, TransactionValidityError> {
		Ok(for_tuples!( ( #( Tuple.implicit()? ),* ) ))
	}

	fn validate(
		&self,
		origin: <Call as Dispatchable>::RuntimeOrigin,
		call: &Call,
		info: &DispatchInfoOf<Call>,
		len: usize,
		self_implicit: Self::Implicit,
		inherited_implication: &impl Encode,
	) -> Result<
		(ValidTransaction, Self::Val, <Call as Dispatchable>::RuntimeOrigin),
		TransactionValidityError,
	> {
		let aggregated_valid = ValidTransaction::default();
		let aggregated_origin = origin;
		let aggregated_val = ();
		let following_explicit_implications = for_tuples!( ( #( &self.Tuple ),* ) );
		let following_implicit_implications = self_implicit;

		for_tuples!(#(
			// Implication of this pipeline element not relevant for later items, so we pop it.
			let (_item, following_explicit_implications) = following_explicit_implications.pop_front();
			let (item_implicit, following_implicit_implications) = following_implicit_implications.pop_front();
			let (valid, val, aggregated_origin) = {
				let aggregate_implications = (
					// This is the implication born of the fact we return the mutated origin
					inherited_implication,
					// This is the explicitly made implication born of the fact the new origin is
					// passed into the next items in this pipeline-tuple.
					&following_explicit_implications,
					// This is the implicitly made implication born of the fact the new origin is
					// passed into the next items in this pipeline-tuple.
					&following_implicit_implications,
				);
				Tuple.validate(aggregated_origin, call, info, len, item_implicit, &aggregate_implications)?
			};
			let aggregated_valid = aggregated_valid.combine_with(valid);
			let aggregated_val = aggregated_val.push_back(val);
		)* );
		Ok((aggregated_valid, aggregated_val, aggregated_origin))
	}

	fn prepare(
		self,
		val: Self::Val,
		origin: &<Call as Dispatchable>::RuntimeOrigin,
		call: &Call,
		info: &DispatchInfoOf<Call>,
		len: usize,
	) -> Result<Self::Pre, TransactionValidityError> {
		Ok(for_tuples!( ( #(
			Tuple::prepare(self.Tuple, val.Tuple, origin, call, info, len)?
		),* ) ))
	}

	fn post_dispatch(
		pre: Self::Pre,
		info: &DispatchInfoOf<Call>,
		post_info: &PostDispatchInfoOf<Call>,
		len: usize,
		result: &DispatchResult,
	) -> Result<(), TransactionValidityError> {
		for_tuples!( #( Tuple::post_dispatch(pre.Tuple, info, post_info, len, result)?; )* );
		Ok(())
	}

	fn metadata() -> Vec<TransactionExtensionMetadata> {
		let mut ids = Vec::new();
		for_tuples!( #( ids.extend(Tuple::metadata()); )* );
		ids
	}
}

impl<Call: Dispatchable> TransactionExtension<Call> for () {
	const IDENTIFIER: &'static str = "UnitTransactionExtension";
	type Val = ();
	type Pre = ();
	type Implicit = ();
	fn implicit(&self) -> sp_std::result::Result<Self::Implicit, TransactionValidityError> {
		Ok(())
	}
	fn validate(
		&self,
		origin: <Call as Dispatchable>::RuntimeOrigin,
		_call: &Call,
		_info: &DispatchInfoOf<Call>,
		_len: usize,
		_self_implicit: Self::Implicit,
		_inherited_implication: &impl Encode,
	) -> Result<
		(ValidTransaction, (), <Call as Dispatchable>::RuntimeOrigin),
		TransactionValidityError,
	> {
		Ok((ValidTransaction::default(), (), origin))
	}
	fn prepare(
		self,
		_val: (),
		_origin: &<Call as Dispatchable>::RuntimeOrigin,
		_call: &Call,
		_info: &DispatchInfoOf<Call>,
		_len: usize,
	) -> Result<(), TransactionValidityError> {
		Ok(())
	}
}
