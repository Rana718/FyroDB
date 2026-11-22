//! Keyed hashing for cache identities.
//!
//! A query's fingerprint decides which cached rows it is served, and the
//! session-context hash keeps one database user's rows apart from another's.
//! Both used to be unkeyed hashes with seeds fixed in the source, which any
//! client can compute offline and so steer: a crafted query or `SET`
//! statement could be made to land on another query's (or user's) entry. A
//! keyed PRF prevents that, because without the key a client cannot compute
//! or predict any of these values (Aumasson, *Serious Cryptography*, ch. 7:
//! SipHash was designed as the PRF for exactly this job).
//!
//! The key is 128 bits from the OS RNG, drawn once per process. Values are
//! never compared across processes (the invalidation bus carries table
//! names), so a per-process key is enough; cached entries written by an
//! earlier process simply miss and expire.

use std::hash::{BuildHasher, DefaultHasher, Hasher, RandomState};
use std::sync::OnceLock;

/// Domain separators: each use hashes under its own prefix, so values from
/// different uses never coincide by construction.
#[derive(Clone, Copy)]
#[repr(u8)]
pub(crate) enum Domain {
   Fingerprint = 1,
   Params = 2,
   TableSeed = 3,
}

fn key() -> &'static RandomState {
   static KEY: OnceLock<RandomState> = OnceLock::new();
   // `RandomState::new` draws SipHash-1-3 keys from the OS RNG.
   KEY.get_or_init(RandomState::new)
}

/// A SipHash-1-3 hasher under the process key, for one domain.
#[inline]
pub(crate) fn hasher(domain: Domain) -> DefaultHasher {
   let mut hasher = key().build_hasher();
   hasher.write_u8(domain as u8);
   hasher
}

/// Keyed hash of `bytes`, whose first byte must be a `Domain`. One write of
/// a contiguous buffer is the cheapest way through the std SipHash.
#[inline]
pub(crate) fn hash_prefixed(bytes: &[u8]) -> u64 {
   debug_assert!(bytes.first().is_some_and(|&b| (1..=3).contains(&b)));
   let mut hasher = key().build_hasher();
   hasher.write(bytes);
   hasher.finish()
}

/// Process-random seed for in-memory hash tables keyed by client-chosen
/// strings: with a seed fixed in the source, colliding keys can be computed
/// offline and a table degraded to linear probing (hash flooding).
pub(crate) fn table_seed() -> u64 {
   static SEED: OnceLock<u64> = OnceLock::new();
   *SEED.get_or_init(|| hasher(Domain::TableSeed).finish())
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn domains_separate_equal_inputs_and_values_are_stable_in_process() {
      let of = |domain| {
         let mut h = hasher(domain);
         h.write(b"select 1");
         h.finish()
      };
      assert_eq!(of(Domain::Fingerprint), of(Domain::Fingerprint));
      assert_ne!(of(Domain::Fingerprint), of(Domain::Params));
      assert_eq!(table_seed(), table_seed());
   }
}
