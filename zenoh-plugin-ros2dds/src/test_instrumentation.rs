//
// Copyright (c) 2024 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

//! Test-only instrumentation to make the route-activation failure observable.
//!
//! The production bug surfaces as `error!` logs reporting failed DDS entity
//! activation (e.g. "failed to activate DDS Reader: Error getting GUID of DDS
//! entity - retcode=-3" / "Error creating DDS Reader: Bad Parameter"). Logs are
//! awkward to assert on, so under the `test-instrumentation` feature we also
//! bump a process-wide atomic counter at each *activation* failure site.
//!
//! IMPORTANT — what is and isn't counted:
//! - Counted: failures on the activation / (re)activation / creation path. These
//!   are the `error!`-level events seen in the real logs.
//! - NOT counted: failures on the *deactivation* cleanup path (e.g. `get_guid`
//!   on an entity we are deliberately deleting). Those are expected, logged at
//!   `warn!`, and are not the bug under investigation. Counting them would make
//!   the reproduction test fire on its own teardown — a false positive.
//!
//! Each increment also records a short context string (route + path) so a red
//! test can be adjudicated: real bug vs. test harness artifact.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

static ACTIVATION_FAILURES: AtomicU64 = AtomicU64::new(0);

static LAST_FAILURES: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Record one DDS-entity activation failure, with a context string identifying
/// the route and the code path (e.g. "route_service_cli:add_remote_route").
///
/// `detail` should include the route id / key expression and the underlying
/// error so a red reproduction test can be inspected without re-running.
pub(crate) fn record_activation_failure(path: &str, detail: &str) {
    ACTIVATION_FAILURES.fetch_add(1, Ordering::Relaxed);
    if let Ok(mut v) = LAST_FAILURES.lock() {
        // Bound the buffer so a long stress run does not grow without limit.
        if v.len() < 1024 {
            v.push(format!("{path}: {detail}"));
        }
    }
}

/// Total number of DDS-entity activation failures recorded since process start
/// (or since the last [`reset`]). The reproduction harness asserts this is 0.
pub fn test_dds_activation_failures() -> u64 {
    ACTIVATION_FAILURES.load(Ordering::Relaxed)
}

/// Drain and return the recorded failure context strings.
pub fn test_dds_activation_failure_details() -> Vec<String> {
    LAST_FAILURES.lock().map(|v| v.clone()).unwrap_or_default()
}

/// Reset both the counter and the detail buffer. Useful to ignore startup noise
/// before beginning the churn loop.
pub fn test_reset_activation_failures() {
    ACTIVATION_FAILURES.store(0, Ordering::Relaxed);
    if let Ok(mut v) = LAST_FAILURES.lock() {
        v.clear();
    }
}
