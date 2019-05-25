// Copyright 2018 foundationdb-rs developers, https://github.com/bluejekyll/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

<<<<<<< HEAD
//! Docs!
//!

use std::sync::Mutex;

use byteorder::ByteOrder;
use futures::future::Future;
use futures::stream::Stream;
use rand::Rng;

use error::Error;
use options::{ConflictRangeType, MutationType, TransactionOption};
use subspace::Subspace;
use transaction::{RangeOptionBuilder, Transaction};
use tuple::Element;
=======
//! Most functions in the FoundationDB API are asynchronous, meaning that they may return to the caller before actually delivering their result.
//!
//! These functions always return FDBFuture*. An FDBFuture object represents a result value or error to be delivered at some future time. You can wait for a Future to be “ready” – to have a value or error delivered – by setting a callback function, or by blocking a thread, or by polling. Once a Future is ready, you can extract either an error code or a value of the appropriate type (the documentation for the original function will tell you which fdb_future_get_*() function you should call).
//!
//! Futures make it easy to do multiple operations in parallel, by calling several asynchronous functions before waiting for any of the results. This can be important for reducing the latency of transactions.
//!
//! The Rust API Client has been implemented to use the Rust futures crate, and should work within that ecosystem (suchas Tokio). See Rust [futures](https://docs.rs/crate/futures/0.1.21) documentation.

use std::io::Bytes;
use std::sync::Mutex;

use byteorder::ByteOrder;
use futures::future::{Future, IntoFuture};
use futures::stream::Stream;
use rand::{random, Rng};

use error::{self, Error, Result};
use future::{KeyValue, KeyValues};
use options::{ConflictRangeType, MutationType, TransactionOption};
use subspace::Subspace;
use transaction::{GetRangeResult, RangeOption, RangeOptionBuilder, RangeStream, Transaction, TrxGetRange};
use tuple::{Decode, Element, Tuple};
>>>>>>> dd08379... hca outline

lazy_static! {
  static ref LOCK: Mutex<i32> = Mutex::new(0);
}

<<<<<<< HEAD
const ONE_BYTES: &[u8] = &[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

/// High Contention Allocator
=======
static ONE_BYTES : &[u8] = &[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

/// Represents a well-defined region of keyspace in a FoundationDB database
///
/// It provides a convenient way to use FoundationDB tuples to define namespaces for
/// different categories of data. The namespace is specified by a prefix tuple which is prepended
/// to all tuples packed by the subspace. When unpacking a key with the subspace, the prefix tuple
/// will be removed from the result.
///
/// As a best practice, API clients should use at least one subspace for application data. For
/// general guidance on subspace usage, see the Subspaces section of the [Developer Guide].
///
/// [Developer Guide]: https://apple.github.io/foundationdb/developer-guide.html#subspaces
///
>>>>>>> dd08379... hca outline
///
#[derive(Debug, Clone)]
pub struct HighContentionAllocator {
    counters: Subspace,
    recent: Subspace,
}

impl HighContentionAllocator {
    fn new(subspace: Subspace) -> HighContentionAllocator {
        HighContentionAllocator {
            counters: subspace.subspace(0),
            recent: subspace.subspace(1),
        }
    }

<<<<<<< HEAD
=======
    fn window_size(start : i64) -> i64 {
        // Larger window sizes are better for high contention, smaller sizes for
        // keeping the keys small.  But if there are many allocations, the keys
        // can't be too small.  So start small and scale up.  We don't want this to
        // ever get *too* big because we have to store about window_size/2 recent
        // items.
        if start < 255 {
            return 64
        }
        if start < 65535 {
            return 1024
        }
        return 8192
    }

>>>>>>> dd08379... hca outline
    fn allocate(
        &self,
        transaction: &mut Transaction,
        subspace: &Subspace,
<<<<<<< HEAD
    ) -> Result<Subspace, Error> {
        loop {
=======
    ) -> Result{
        let range_option = RangeOptionBuilder::from(self.counters.range())
            .reverse(true)
            .limit(1)
            .snapshot(true)
            .build();

        let kvs : Vec<i64> = transaction.get_ranges(range_option)
            .map_err(|(_, e)| e)
            .filter_map(|range_result| {
                for kv in range_result.key_values().as_ref() {
                    if let Element::I64(counter) = self.counters.unpack(kv.key()).expect("hello") {
                        return Some(counter);
                    }
                }

                return None;
            })
            .collect()
            .wait()
            .expect("failed to fetch HCA counter range");

        let mut start : i64 = 0;
        let mut window : i64 = 0;

        if kvs.len() == 1 {
            start = kvs[0];
        }

        let mut window_advanced = false;

        loop {
            let mutex_guard = LOCK.lock().unwrap();

            if window_advanced {
                transaction.clear_range(self.counters.bytes(), self.counters.subspace(start).bytes());
                transaction.set_option(TransactionOption::NextWriteNoWriteConflictRange);
                transaction.clear_range(self.recent.bytes(), self.recent.subspace(start).bytes());
            }

            let counters_subspace_with_start = self.counters.subspace(start);

            // Increment the allocation count for the current window
            transaction.atomic_op(counters_subspace_with_start.bytes(), ONE_BYTES, MutationType::Add);

            let get_result = transaction.get(counters_subspace_with_start.bytes(), true)
                .wait()
                .expect("get request failed");

            let count : i64 = match get_result.value() {
                Some(x) => byteorder::LittleEndian::read_i64(x),
                None => 2 // return failure
            };

            drop(mutex_guard);

            window = HighContentionAllocator::window_size(start);

            if count * 2 < window {
                break
            }

            start += window;
            window_advanced = true;
        }

        loop {
            // As of the snapshot being read from, the window is less than half
            // full, so this should be expected to take 2 tries.  Under high
            // contention (and when the window advances), there is an additional
            // subsequent risk of conflict for this transaction.
            let mut rng = rand::thread_rng();

            let candidate = rng.gen::<i64>() + start;
            let key = self.recent.subspace(candidate);
            let key_bytes = key.bytes();

            let mutex_guard = LOCK.lock().unwrap();

>>>>>>> dd08379... hca outline
            let range_option = RangeOptionBuilder::from(self.counters.range())
                .reverse(true)
                .limit(1)
                .snapshot(true)
                .build();

<<<<<<< HEAD
            let kvs: Vec<i64> = transaction.get_ranges(range_option)
                .map_err(|(_, e)| e)
                .filter_map(|range_result| {
                    for kv in range_result.key_values().as_ref() {
                        if let Element::I64(counter) = self.counters.unpack(kv.key()).expect("unable to unpack counter key") {
=======
            let kvs : Vec<i64> = transaction.get_ranges(range_option)
                .map_err(|(_, e)| e)
                .filter_map(|range_result| {
                    for kv in range_result.key_values().as_ref() {
                        if let Element::I64(counter) = self.counters.unpack(kv.key()).expect("hello") {
>>>>>>> dd08379... hca outline
                            return Some(counter);
                        }
                    }

                    return None;
                })
                .collect()
<<<<<<< HEAD
                .wait()?;

            let mut start: i64 = 0;
            let mut window: i64 = 0;

            if kvs.len() == 1 {
                start = kvs[0];
            }

            let mut window_advanced = false;

            loop {
                let mutex_guard = LOCK.lock().unwrap();

                if window_advanced {
                    transaction.clear_range(self.counters.bytes(), self.counters.subspace(start).bytes());
                    transaction.set_option(TransactionOption::NextWriteNoWriteConflictRange)?;
                    transaction.clear_range(self.recent.bytes(), self.recent.subspace(start).bytes());
                }

                let counters_subspace_with_start = self.counters.subspace(start);

                // Increment the allocation count for the current window
                transaction.atomic_op(counters_subspace_with_start.bytes(), ONE_BYTES, MutationType::Add);

                let subspace_start_trx = transaction.get(counters_subspace_with_start.bytes(), true).wait()?;
                let count = byteorder::LittleEndian::read_i64(subspace_start_trx.value().unwrap());

                drop(mutex_guard);

                window = HighContentionAllocator::window_size(start);

                if count * 2 < window {
                    break;
                }

                start += window;
                window_advanced = true;
            }

            loop {
                // As of the snapshot being read from, the window is less than half
                // full, so this should be expected to take 2 tries.  Under high
                // contention (and when the window advances), there is an additional
                // subsequent risk of conflict for this transaction.
                let mut rng = rand::thread_rng();

                let candidate = self.recent.subspace(rng.gen::<i64>() + start);
                let candidate_subspace = candidate.bytes();

                let mutex_guard = LOCK.lock().unwrap();

                let range_option = RangeOptionBuilder::from(self.counters.range())
                    .reverse(true)
                    .limit(1)
                    .snapshot(true)
                    .build();

                let kvs: Vec<i64> = transaction.get_ranges(range_option)
                    .map_err(|(_, e)| e)
                    .filter_map(|range_result| {
                        for kv in range_result.key_values().as_ref() {
                            if let Element::I64(counter) = self.counters.unpack(kv.key()).expect("unable to unpack counter key") {
                                return Some(counter);
                            }
                        }
                        return None;
                    })
                    .collect()
                    .wait()?;

                let candidate_value_trx = transaction.get(candidate_subspace, false);

                transaction.set_option(TransactionOption::NextWriteNoWriteConflictRange)?;
                transaction.set(candidate_subspace, &[]);

                drop(mutex_guard);

                if kvs.len() > 0 {
                    let current_start = kvs[0];

                    if current_start > start {
                        break;
                    }
                }

                let candidate_value = candidate_value_trx.wait()?;

                match candidate_value.value() {
                    Some(x) => {
                        if x.len() == 0 {
                            transaction.add_conflict_range(candidate_subspace, candidate_subspace, ConflictRangeType::Write)?;
                            return Ok(self.counters.subspace(candidate_subspace));
                        }
                    }
                    None => {
                        continue;
                    }
                };
            }
        }
    }

    fn window_size(start: i64) -> i64 {
        // Larger window sizes are better for high contention, smaller sizes for
        // keeping the keys small.  But if there are many allocations, the keys
        // can't be too small.  So start small and scale up.  We don't want this to
        // ever get *too* big because we have to store about window_size/2 recent
        // items.
        if start < 255 {
            return 64;
        }
        if start < 65535 {
            return 1024;
        }
        return 8192;
=======
                .wait()
                .expect("failed to fetch HCA counter range");

            let candidate_value_trx = transaction.get(key_bytes, false);

            transaction.set_option(TransactionOption::NextWriteNoWriteConflictRange);
            transaction.set(key_bytes, &[]);

            drop(mutex_guard);

            if kvs.len() > 0 {
                let current_start = kvs[0];

                if current_start > start {
                    break
                }
            }

            let candidate_value = candidate_value_trx.wait().expect("unable to get candidate value");

            match candidate_value.value() {
                Some(x) => {
                    continue
                },
                None => {
                    transaction.add_conflict_range(key_bytes, key_bytes, ConflictRangeType::Write);
                    // return subspace
                }
            };

            /*
            			kvs, e = latestCounter.GetSliceWithError()
			if e != nil {
				return nil, e
			}
			if len(kvs) > 0 {
				t, e := hca.counters.Unpack(kvs[0].Key)
				if e != nil {
					return nil, e
				}
				currentStart := t[0].(int64)
				if currentStart > start {
					break
				}
			}

			v, e := candidateValue.Get()
			if e != nil {
				return nil, e
			}
			if v == nil {
				tr.AddWriteConflictKey(key)
				return s.Sub(candidate), nil
			}
			*/

        }

>>>>>>> dd08379... hca outline
    }
}
