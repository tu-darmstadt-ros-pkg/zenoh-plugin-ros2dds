//
// Copyright (c) 2022 ZettaScale Technology
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
use cyclors::{qos::*, DDS_LENGTH_UNLIMITED};

pub fn get_history_or_default(qos: &Qos) -> History {
    match &qos.history {
        None => History::default(),
        Some(history) => history.clone(),
    }
}

pub fn get_durability_service_or_default(qos: &Qos) -> DurabilityService {
    match &qos.durability_service {
        None => DurabilityService::default(),
        Some(durability_service) => durability_service.clone(),
    }
}

pub fn is_reliable(qos: &Qos) -> bool {
    qos.reliability
        .as_ref()
        .is_some_and(|reliability| reliability.kind == ReliabilityKind::RELIABLE)
}

/// TRANSIENT and PERSISTENT offer TRANSIENT_LOCAL too.
pub fn is_transient_local(qos: &Qos) -> bool {
    qos.durability
        .as_ref()
        .is_some_and(|durability| durability.kind != DurabilityKind::VOLATILE)
}

// Copy and adapt Writer's QoS for creation of a matching Reader
pub fn adapt_writer_qos_for_reader(qos: &Qos) -> Qos {
    let mut reader_qos = qos.clone();

    // Unset any writer QoS that doesn't apply to data readers
    reader_qos.durability_service = None;
    reader_qos.ownership_strength = None;
    reader_qos.transport_priority = None;
    reader_qos.lifespan = None;
    reader_qos.writer_data_lifecycle = None;
    reader_qos.writer_batching = None;

    // Unset proprietary QoS which shouldn't apply
    reader_qos.properties = None;
    reader_qos.entity_name = None;
    reader_qos.ignore_local = Some(IgnoreLocal {
        kind: IgnoreLocalKind::PARTICIPANT,
    });

    // Set default Reliability QoS if not set for writer
    if reader_qos.reliability.is_none() {
        reader_qos.reliability = Some({
            Reliability {
                kind: ReliabilityKind::BEST_EFFORT,
                max_blocking_time: DDS_100MS_DURATION,
            }
        });
    }

    reader_qos
}

// Copy and adapt Reader's QoS for creation of a matching Writer
pub fn adapt_reader_qos_for_writer(qos: &Qos) -> Qos {
    let mut writer_qos = qos.clone();

    // Unset any reader QoS that doesn't apply to data writers
    writer_qos.time_based_filter = None;
    writer_qos.reader_data_lifecycle = None;
    writer_qos.properties = None;
    writer_qos.entity_name = None;

    // Don't match with readers with the same participant
    writer_qos.ignore_local = Some(IgnoreLocal {
        kind: IgnoreLocalKind::PARTICIPANT,
    });

    // if Reader is TRANSIENT_LOCAL, configure durability_service QoS with same history as the Reader.
    // This is because CycloneDDS is actually using durability_service.history for transient_local historical data.
    if is_transient_local(qos) {
        let history = qos
            .history
            .as_ref()
            .map_or(History::default(), |history| history.clone());

        writer_qos.durability_service = Some(DurabilityService {
            service_cleanup_delay: 60 * DDS_1S_DURATION,
            history_kind: history.kind,
            history_depth: history.depth,
            max_samples: DDS_LENGTH_UNLIMITED,
            max_instances: DDS_LENGTH_UNLIMITED,
            max_samples_per_instance: DDS_LENGTH_UNLIMITED,
        });
    }
    // Workaround for the DDS Writer to correctly match with a FastRTPS Reader
    writer_qos.reliability = match writer_qos.reliability {
        Some(mut reliability) => {
            reliability.max_blocking_time = reliability.max_blocking_time.saturating_add(1);
            Some(reliability)
        }
        _ => {
            let mut reliability = Reliability {
                kind: ReliabilityKind::RELIABLE,
                max_blocking_time: DDS_100MS_DURATION,
            };
            reliability.max_blocking_time = reliability.max_blocking_time.saturating_add(1);
            Some(reliability)
        }
    };

    writer_qos
}

/// Cap of a summed TRANSIENT_LOCAL depth, Cyclone preallocates the history index per instance.
pub const MAX_TL_DEPTH: i32 = 10_000;

/// History of `qos`, with KEEP_LAST depth clamped to >= 1 (depth 0 is invalid).
fn history_of(qos: &Qos) -> History {
    let mut h = get_history_or_default(qos);
    if h.kind == HistoryKind::KEEP_LAST {
        h.depth = h.depth.max(1);
    }
    h
}

/// Strongest history of `qoses` (KEEP_ALL wins, else the deepest KEEP_LAST).
fn max_history<'a>(qoses: impl Iterator<Item = &'a Qos>) -> History {
    qoses
        .map(history_of)
        .reduce(|a, b| match (a.kind, b.kind) {
            (HistoryKind::KEEP_ALL, _) => a,
            (_, HistoryKind::KEEP_ALL) => b,
            _ if b.depth > a.depth => b,
            _ => a,
        })
        .unwrap_or_else(|| history_of(&Qos::default()))
}

/// History of a route serving `qoses`. TRANSIENT_LOCAL depths add up so that one route keeps every publisher's history.
/// Several TRANSIENT_LOCAL publishers share one history, `tl_cache_multiplier` keeps a fast or replayed one from evicting the others.
fn route_history(qoses: &[Qos], tl_cache_multiplier: usize) -> History {
    let max = max_history(qoses.iter());
    let tl: Vec<i64> = qoses
        .iter()
        .filter(|q| is_transient_local(q))
        .map(|q| history_of(q).depth as i64)
        .collect();
    if max.kind == HistoryKind::KEEP_ALL || tl.is_empty() {
        return max;
    }
    let multiplier = if tl.len() > 1 {
        tl_cache_multiplier.max(1) as i64
    } else {
        1
    };
    let depth = tl.iter().sum::<i64>().saturating_mul(multiplier);
    History {
        kind: HistoryKind::KEEP_LAST,
        depth: depth.min(MAX_TL_DEPTH as i64) as i32,
    }
}

fn reliability(kind: ReliabilityKind) -> Option<Reliability> {
    Some(Reliability {
        kind,
        max_blocking_time: DDS_100MS_DURATION,
    })
}

fn durability(tl: bool) -> Option<Durability> {
    Some(Durability {
        kind: if tl {
            DurabilityKind::TRANSIENT_LOCAL
        } else {
            DurabilityKind::VOLATILE
        },
    })
}

/// Reader QoS of a DDS to zenoh route, the weakest QoS all `local_writers` offer.
/// Without local Writers it is a BEST_EFFORT VOLATILE placeholder matching any Writer,
/// with the type hash from `fallback`.
pub fn reader_qos_for_local_writers(
    local_writers: &[Qos],
    fallback: &Qos,
    tl_cache_multiplier: usize,
) -> Qos {
    let mut qos = adapt_writer_qos_for_reader(local_writers.first().unwrap_or(fallback));
    let all_reliable = !local_writers.is_empty() && local_writers.iter().all(is_reliable);
    let all_tl = !local_writers.is_empty() && local_writers.iter().all(is_transient_local);

    qos.reliability = reliability(if all_reliable {
        ReliabilityKind::RELIABLE
    } else {
        ReliabilityKind::BEST_EFFORT
    });
    qos.durability = durability(all_tl);
    qos.history = Some(route_history(local_writers, tl_cache_multiplier));
    // most permissive RxO values so that no local Writer is rejected
    qos.deadline = None;
    qos.liveliness = None;
    qos.latency_budget = Some(LatencyBudget {
        duration: DDS_INFINITE_TIME,
    });
    qos.data_representation = None;
    qos.destination_order = None;
    qos.presentation = None;
    qos.resource_limits = None;
    qos.time_based_filter = None;
    qos.reader_data_lifecycle = None;
    qos
}

/// The TRANSIENT_LOCAL and the VOLATILE ones of `writers`, None unless both kinds are present.
/// Such a topic is bridged twice so that the VOLATILE samples never evict the TRANSIENT_LOCAL history.
pub fn split_mixed_durability(writers: &[Qos]) -> Option<(Vec<Qos>, Vec<Qos>)> {
    let (tl, volatile): (Vec<Qos>, Vec<Qos>) =
        writers.iter().cloned().partition(is_transient_local);
    (!tl.is_empty() && !volatile.is_empty()).then_some((tl, volatile))
}

/// Reader QoS of a DDS to zenoh route, plus a second VOLATILE Reader's QoS if TRANSIENT_LOCAL and VOLATILE Writers mix.
/// The first Reader then serves only the TRANSIENT_LOCAL Writers so that their history is kept.
pub fn readers_qos_for_local_writers(
    local_writers: &[Qos],
    fallback: &Qos,
    tl_cache_multiplier: usize,
) -> (Qos, Option<Qos>) {
    let reader = |w: &[Qos]| reader_qos_for_local_writers(w, fallback, tl_cache_multiplier);
    match split_mixed_durability(local_writers) {
        Some((tl, volatile)) => (reader(&tl), Some(reader(&volatile))),
        None => (reader(local_writers), None),
    }
}

/// QoS a DDS to zenoh route announces, plus a second announcement for the VOLATILE Writers if durabilities mix.
pub fn announced_qoses_for_local_writers(
    local_writers: &[Qos],
    tl_cache_multiplier: usize,
) -> (Qos, Option<Qos>) {
    let announce = |w: &[Qos]| announced_qos_for_local_writers(w, tl_cache_multiplier);
    match split_mixed_durability(local_writers) {
        Some((tl, volatile)) => (announce(&tl), Some(announce(&volatile))),
        None => (announce(local_writers), None),
    }
}

/// Writer QoS of a zenoh to DDS route for the announcements `(bridge, qos)`, plus a second VOLATILE Writer's QoS.
/// Only a bridge announcing both durabilities marks its VOLATILE samples, so only its VOLATILE announcements move.
pub fn writers_qos_for_remote_writers(remote_writers: &[(&str, &Qos)]) -> (Qos, Option<Qos>) {
    let mixed = |bridge: &str| {
        let (mut tl, mut volatile) = (false, false);
        for (_, q) in remote_writers.iter().filter(|(b, _)| *b == bridge) {
            *if is_transient_local(q) {
                &mut tl
            } else {
                &mut volatile
            } = true;
        }
        tl && volatile
    };
    let (moved, kept): (Vec<_>, Vec<_>) = remote_writers
        .iter()
        .partition(|(bridge, q)| !is_transient_local(q) && mixed(bridge));
    let qoses =
        |w: Vec<&(&str, &Qos)>| -> Vec<Qos> { w.into_iter().map(|(_, q)| (*q).clone()).collect() };
    let volatile = (!moved.is_empty()).then(|| writer_qos_for_remote_writers(&qoses(moved)));
    (writer_qos_for_remote_writers(&qoses(kept)), volatile)
}

/// QoS a DDS to zenoh route announces, the strongest offer of all its local Writers.
pub fn announced_qos_for_local_writers(local_writers: &[Qos], tl_cache_multiplier: usize) -> Qos {
    let mut qos = local_writers.first().cloned().unwrap_or_default();
    qos.reliability = reliability(if local_writers.iter().any(is_reliable) {
        ReliabilityKind::RELIABLE
    } else {
        ReliabilityKind::BEST_EFFORT
    });
    qos.durability = durability(local_writers.iter().any(is_transient_local));
    qos.history = Some(route_history(local_writers, tl_cache_multiplier));
    qos
}

/// Writer QoS of a zenoh to DDS route, the strongest offer of the announced `remote_writers`.
/// One Writer stands in for all remote publishers (e.g. static_transform_publishers),
/// Cyclone serves their summed TRANSIENT_LOCAL history from `durability_service`.
pub fn writer_qos_for_remote_writers(remote_writers: &[Qos]) -> Qos {
    let mut qos = remote_writers.first().cloned().unwrap_or_default();
    let any_tl = remote_writers.iter().any(is_transient_local);
    // each announcement already includes its bridge's multiplier
    let history = route_history(remote_writers, 1);

    // RELIABLE serves BEST_EFFORT Readers too (#23), +1 is the FastRTPS workaround of adapt_reader_qos_for_writer
    qos.reliability = Some(Reliability {
        kind: ReliabilityKind::RELIABLE,
        max_blocking_time: DDS_100MS_DURATION.saturating_add(1),
    });
    qos.durability = durability(any_tl);
    qos.durability_service = any_tl.then_some(DurabilityService {
        service_cleanup_delay: 60 * DDS_1S_DURATION,
        history_kind: history.kind,
        history_depth: history.depth,
        max_samples: DDS_LENGTH_UNLIMITED,
        max_instances: DDS_LENGTH_UNLIMITED,
        max_samples_per_instance: DDS_LENGTH_UNLIMITED,
    });
    qos.history = Some(history);
    qos.ignore_local = Some(IgnoreLocal {
        kind: IgnoreLocalKind::PARTICIPANT,
    });
    qos
}

/// Whether a route can keep its DDS endpoint for a recomputed QoS.
/// Unset policies equal their DDS default, `reader` selects the reliability default.
pub fn same_route_qos(a: &Qos, b: &Qos, reader: bool) -> bool {
    let default_rel = if reader {
        ReliabilityKind::BEST_EFFORT
    } else {
        ReliabilityKind::RELIABLE
    };
    let rel = |q: &Qos| q.reliability.as_ref().map_or(default_rel, |r| r.kind) as isize;
    let tl = |q: &Qos| is_transient_local(q);
    let ds = |q: &Qos| {
        q.durability_service
            .as_ref()
            .map(|d| (d.history_kind as isize, d.history_depth))
    };
    let hist = |q: &Qos| {
        let h = history_of(q);
        (h.kind as isize, h.depth)
    };
    rel(a) == rel(b) && tl(a) == tl(b) && hist(a) == hist(b) && ds(a) == ds(b)
}

/// Whether local Writers of one durability disagree on reliability, so their Reader is BEST_EFFORT for all.
pub fn local_writers_disagree(local_writers: &[Qos]) -> bool {
    [true, false].into_iter().any(|tl| {
        let group = local_writers.iter().filter(|q| is_transient_local(q) == tl);
        let (n, rel) = group.fold((0, 0), |(n, rel), q| (n + 1, rel + is_reliable(q) as usize));
        rel != 0 && rel != n
    })
}

/// Compact human-readable form of the route-relevant QoS (admin space).
pub fn route_qos_summary(qos: &Qos) -> String {
    let rel = if is_reliable(qos) {
        "RELIABLE"
    } else {
        "BEST_EFFORT"
    };
    let dur = if is_transient_local(qos) {
        "TRANSIENT_LOCAL"
    } else {
        "VOLATILE"
    };
    let h = history_of(qos);
    let hist = match h.kind {
        HistoryKind::KEEP_ALL => "KEEP_ALL".to_string(),
        HistoryKind::KEEP_LAST => format!("KEEP_LAST({})", h.depth),
    };
    match &qos.durability_service {
        Some(ds) => format!("{rel} {dur} {hist} tl_depth={}", ds.history_depth),
        None => format!("{rel} {dur} {hist}"),
    }
}

#[cfg(test)]
mod route_qos_tests {
    use super::*;

    fn q(reliable: Option<bool>, tl: bool, history: Option<(HistoryKind, i32)>) -> Qos {
        Qos {
            reliability: reliable.map(|r| Reliability {
                kind: if r {
                    ReliabilityKind::RELIABLE
                } else {
                    ReliabilityKind::BEST_EFFORT
                },
                max_blocking_time: DDS_100MS_DURATION,
            }),
            durability: durability(tl),
            history: history.map(|(kind, depth)| History { kind, depth }),
            user_data: Some(b"typehash=X;".to_vec()),
            ..Default::default()
        }
    }
    fn rel(q: &Qos) -> ReliabilityKind {
        q.reliability.as_ref().unwrap().kind
    }
    fn dur(q: &Qos) -> DurabilityKind {
        q.durability.as_ref().unwrap().kind
    }
    fn hist(q: &Qos) -> (HistoryKind, i32) {
        let h = q.history.as_ref().unwrap();
        (h.kind, h.depth)
    }
    const KL: HistoryKind = HistoryKind::KEEP_LAST;
    const KA: HistoryKind = HistoryKind::KEEP_ALL;

    /// Fields that a Writer can set and a Reader can reject on (RxO).
    fn demanding_writer() -> Qos {
        let mut w = q(Some(true), true, Some((KL, 3)));
        w.deadline = Some(Deadline {
            period: DDS_1S_DURATION,
        });
        w.liveliness = Some(Liveliness {
            kind: LivelinessKind::MANUAL_BY_TOPIC,
            lease_duration: DDS_1S_DURATION,
        });
        w.latency_budget = Some(LatencyBudget {
            duration: DDS_1S_DURATION,
        });
        w.destination_order = Some(DestinationOrder {
            kind: DestinationOrderKind::BY_SOURCE_TIMESTAMP,
        });
        w.presentation = Some(Presentation {
            access_scope: PresentationAccessScopeKind::TOPIC,
            coherent_access: true,
            ordered_access: true,
        });
        w.data_representation = Some(vec![2]);
        w.resource_limits = Some(ResourceLimits {
            max_samples: 3,
            max_instances: 1,
            max_samples_per_instance: 3,
        });
        w
    }

    fn assert_permissive_reader(r: &Qos) {
        assert!(r.deadline.is_none(), "deadline");
        assert!(r.liveliness.is_none(), "liveliness");
        assert_eq!(
            r.latency_budget.as_ref().map(|l| l.duration),
            Some(DDS_INFINITE_TIME),
            "latency_budget must be infinite (0 rejects any Writer with a budget)"
        );
        assert!(r.destination_order.is_none(), "destination_order");
        assert!(r.presentation.is_none(), "presentation");
        assert!(r.data_representation.is_none(), "data_representation");
        assert!(r.resource_limits.is_none(), "resource_limits");
    }

    #[test]
    fn reader_placeholder_matches_any_writer() {
        // remote demanding subscriber (RELIABLE, TL, depth 100) announced first
        let mut announced = demanding_writer();
        announced.history = Some(History {
            kind: KL,
            depth: 100,
        });
        let r = reader_qos_for_local_writers(&[], &announced, 1);
        assert_eq!(rel(&r), ReliabilityKind::BEST_EFFORT);
        assert_eq!(dur(&r), DurabilityKind::VOLATILE);
        assert_permissive_reader(&r);
        assert_eq!(r.user_data, announced.user_data, "type hash must be kept");
    }

    #[test]
    fn reader_follows_single_writer() {
        let w = q(Some(true), true, Some((KL, 7)));
        let r = reader_qos_for_local_writers(&[w], &Qos::default(), 1);
        assert_eq!(rel(&r), ReliabilityKind::RELIABLE);
        assert_eq!(dur(&r), DurabilityKind::TRANSIENT_LOCAL);
        assert_eq!(hist(&r), (KL, 7));
    }

    #[test]
    fn reader_requests_weakest_common_qos() {
        let ws = [
            q(Some(true), true, Some((KL, 3))),
            q(Some(false), true, Some((KL, 10))),
            q(Some(true), false, Some((KL, 1))),
        ];
        let r = reader_qos_for_local_writers(&ws, &Qos::default(), 1);
        assert_eq!(rel(&r), ReliabilityKind::BEST_EFFORT);
        assert_eq!(dur(&r), DurabilityKind::VOLATILE);
        assert_eq!(hist(&r), (KL, 13));
        assert!(local_writers_disagree(&ws));
        assert!(!local_writers_disagree(&ws[..1]));
    }

    #[test]
    fn only_a_mixed_bridge_gets_a_volatile_writer() {
        let tl = q(Some(true), true, Some((KL, 3)));
        let volatile = q(Some(true), false, Some((KL, 10)));
        let (main, second) = writers_qos_for_remote_writers(&[("a", &volatile), ("b", &tl)]);
        assert_eq!(dur(&main), DurabilityKind::TRANSIENT_LOCAL);
        assert!(second.is_none(), "bridge a does not mark its samples");

        let (main, second) =
            writers_qos_for_remote_writers(&[("a", &volatile), ("b", &tl), ("b", &volatile)]);
        assert_eq!(dur(&main), DurabilityKind::TRANSIENT_LOCAL);
        assert_eq!(hist(&main), (KL, 3));
        let second = second.expect("bridge b marks its VOLATILE samples");
        assert_eq!(dur(&second), DurabilityKind::VOLATILE);
    }

    #[test]
    fn mixed_durability_gets_a_volatile_reader() {
        let tl = q(Some(true), true, Some((KL, 3)));
        let volatile = q(Some(false), false, Some((KL, 10)));
        let (main, second) =
            readers_qos_for_local_writers(&[volatile.clone(), tl.clone()], &Qos::default(), 1);
        assert_eq!(rel(&main), ReliabilityKind::RELIABLE);
        assert_eq!(dur(&main), DurabilityKind::TRANSIENT_LOCAL);
        assert_eq!(hist(&main), (KL, 3));
        let second = second.expect("VOLATILE Writers need their own Reader");
        assert_eq!(rel(&second), ReliabilityKind::BEST_EFFORT);
        assert_eq!(dur(&second), DurabilityKind::VOLATILE);
        assert!(!local_writers_disagree(&[tl.clone(), volatile]));

        let (main, second) = readers_qos_for_local_writers(&[tl.clone(), tl], &Qos::default(), 1);
        assert_eq!(hist(&main), (KL, 6));
        assert!(second.is_none());
    }

    #[test]
    fn reader_is_independent_of_writer_order() {
        let a = demanding_writer();
        let b = q(Some(false), false, Some((KA, 0)));
        let r1 = reader_qos_for_local_writers(&[a.clone(), b.clone()], &Qos::default(), 1);
        let r2 = reader_qos_for_local_writers(&[b, a], &Qos::default(), 1);
        assert!(same_route_qos(&r1, &r2, true));
        assert_permissive_reader(&r1);
        assert_permissive_reader(&r2);
        assert_eq!(hist(&r1).0, KA);
    }

    #[test]
    fn reader_does_not_inherit_first_writer_constraints() {
        let r = reader_qos_for_local_writers(&[demanding_writer()], &Qos::default(), 1);
        assert_permissive_reader(&r);
    }

    #[test]
    fn reader_treats_unset_writer_reliability_as_best_effort() {
        // as adapt_writer_qos_for_reader always did
        let r = reader_qos_for_local_writers(&[q(None, false, None)], &Qos::default(), 1);
        assert_eq!(rel(&r), ReliabilityKind::BEST_EFFORT);
    }

    #[test]
    fn reader_counts_transient_as_transient_local() {
        let mut w = q(Some(true), true, None);
        w.durability = Some(Durability {
            kind: DurabilityKind::TRANSIENT,
        });
        let r = reader_qos_for_local_writers(&[w], &Qos::default(), 1);
        assert_eq!(dur(&r), DurabilityKind::TRANSIENT_LOCAL);
    }

    #[test]
    fn announcement_is_strongest_offer_of_all_local_writers() {
        // first-discovered Writer is VOLATILE, the static TF publishers are TL
        let ws = [
            q(Some(true), false, Some((KL, 10))),
            q(Some(true), true, Some((KL, 1))),
            q(Some(false), true, Some((KL, 1))),
        ];
        let a = announced_qos_for_local_writers(&ws, 1);
        assert_eq!(rel(&a), ReliabilityKind::RELIABLE);
        assert_eq!(dur(&a), DurabilityKind::TRANSIENT_LOCAL);
        // the remote Writer keeps the TL publishers' history, the VOLATILE one has none
        assert_eq!(hist(&a), (KL, 2));
        let rev: Vec<Qos> = ws.iter().rev().cloned().collect();
        assert!(same_route_qos(
            &a,
            &announced_qos_for_local_writers(&rev, 1),
            false
        ));
    }

    #[test]
    fn transient_local_history_is_summed_over_publishers() {
        // tf_static: 20 static_transform_publishers, each TL KEEP_LAST(1)
        let tf: Vec<Qos> = (0..20)
            .map(|_| q(Some(true), true, Some((KL, 1))))
            .collect();
        let a = announced_qos_for_local_writers(&tf, 1);
        assert_eq!(hist(&a), (KL, 20));
        assert_eq!(
            hist(&reader_qos_for_local_writers(&tf, &Qos::default(), 1)),
            (KL, 20)
        );
        let w = writer_qos_for_remote_writers(&[a]);
        assert_eq!(rel(&w), ReliabilityKind::RELIABLE);
        assert_eq!(dur(&w), DurabilityKind::TRANSIENT_LOCAL);
        assert_eq!(hist(&w), (KL, 20));
        let ds = w.durability_service.as_ref().expect("durability_service");
        assert_eq!((ds.history_kind, ds.history_depth), (KL, 20));
        // slack for publishers sharing one history, applied once by the announcing bridge
        let a = announced_qos_for_local_writers(&tf, 10);
        assert_eq!(hist(&a), (KL, 200));
        assert_eq!(hist(&writer_qos_for_remote_writers(&[a])), (KL, 200));
    }

    #[test]
    fn single_transient_local_publisher_keeps_its_own_depth() {
        // a late subscriber of a latched /map gets the one map plain DDS would deliver
        let map = [q(Some(true), true, Some((KL, 1)))];
        assert_eq!(
            hist(&reader_qos_for_local_writers(&map, &Qos::default(), 10)),
            (KL, 1)
        );
        let w = writer_qos_for_remote_writers(&[announced_qos_for_local_writers(&map, 10)]);
        assert_eq!(hist(&w), (KL, 1));
    }

    #[test]
    fn writer_sums_history_of_several_remote_bridges() {
        let w = writer_qos_for_remote_writers(&[
            q(Some(true), true, Some((KL, 20))),
            q(Some(true), true, Some((KL, 5))),
        ]);
        assert_eq!(hist(&w), (KL, 25));
    }

    #[test]
    fn writer_offers_strongest_remote_qos_in_any_order() {
        let ws = [
            q(Some(false), false, Some((KL, 5))),
            q(Some(true), true, Some((KL, 2))),
        ];
        let w = writer_qos_for_remote_writers(&ws);
        assert_eq!(rel(&w), ReliabilityKind::RELIABLE);
        assert_eq!(dur(&w), DurabilityKind::TRANSIENT_LOCAL);
        assert_eq!(hist(&w), (KL, 2));
        let rev = [ws[1].clone(), ws[0].clone()];
        assert!(same_route_qos(
            &w,
            &writer_qos_for_remote_writers(&rev),
            false
        ));
    }

    #[test]
    fn writer_follows_dds_durability_semantics() {
        // VOLATILE remote publishers -> VOLATILE Writer (a TL Reader must not match)
        let w = writer_qos_for_remote_writers(&[q(Some(true), false, Some((KL, 5)))]);
        assert_eq!(dur(&w), DurabilityKind::VOLATILE);
        assert!(w.durability_service.is_none());
        assert_eq!(hist(&w), (KL, 5));
    }

    #[test]
    fn writer_is_reliable_even_for_best_effort_publishers() {
        let w = writer_qos_for_remote_writers(&[q(Some(false), false, None)]);
        assert_eq!(rel(&w), ReliabilityKind::RELIABLE);
        assert!(w.ignore_local.is_some());
    }

    #[test]
    fn summed_depth_is_capped() {
        // /rosout: every node has a TL KEEP_LAST(1000) Writer
        let rosout: Vec<Qos> = (0..30)
            .map(|_| q(Some(true), true, Some((KL, 1000))))
            .collect();
        let a = announced_qos_for_local_writers(&rosout, 1);
        assert_eq!(hist(&a), (KL, MAX_TL_DEPTH));
        assert_eq!(
            hist(&writer_qos_for_remote_writers(&[a])),
            (KL, MAX_TL_DEPTH)
        );
        // the announced depth comes from a remote bridge, it is capped too
        let w = writer_qos_for_remote_writers(&[
            q(Some(true), true, Some((KL, i32::MAX))),
            q(Some(true), true, Some((KL, i32::MAX))),
        ]);
        assert_eq!(hist(&w), (KL, MAX_TL_DEPTH));
        let w = writer_qos_for_remote_writers(&[
            q(Some(true), true, Some((KL, 5))),
            q(Some(true), true, Some((KA, 0))),
        ]);
        assert_eq!(hist(&w).0, KA);
    }

    #[test]
    fn zero_or_negative_keep_last_depth_is_clamped() {
        let w = writer_qos_for_remote_writers(&[q(Some(true), false, Some((KL, 0)))]);
        assert_eq!(hist(&w), (KL, 1));
        let r = reader_qos_for_local_writers(
            &[q(Some(true), false, Some((KL, -3)))],
            &Qos::default(),
            1,
        );
        assert_eq!(hist(&r), (KL, 1));
    }

    #[test]
    fn same_route_qos_detects_relevant_changes_only() {
        let a = q(Some(true), true, Some((KL, 1)));
        let mut b = a.clone();
        b.user_data = Some(b"other".to_vec());
        b.reliability.as_mut().unwrap().max_blocking_time += 1;
        assert!(
            same_route_qos(&a, &b, false),
            "user_data and blocking time alone must not force a rebuild"
        );
        assert!(!same_route_qos(
            &a,
            &q(Some(false), true, Some((KL, 1))),
            false
        ));
        assert!(!same_route_qos(
            &a,
            &q(Some(true), false, Some((KL, 1))),
            false
        ));
        assert!(!same_route_qos(
            &a,
            &q(Some(true), true, Some((KL, 2))),
            false
        ));
    }

    #[test]
    fn same_route_qos_treats_unset_as_dds_default() {
        // unset reliability: RELIABLE for a Writer, BEST_EFFORT for a Reader
        assert!(same_route_qos(
            &q(None, false, None),
            &q(Some(true), false, Some((KL, 1))),
            false
        ));
        assert!(same_route_qos(
            &q(None, false, None),
            &q(Some(false), false, Some((KL, 1))),
            true
        ));
        // unset durability == VOLATILE, unset history == KEEP_LAST(1)
        let mut unset = q(Some(true), false, None);
        unset.durability = None;
        assert!(same_route_qos(
            &unset,
            &q(Some(true), false, Some((KL, 1))),
            false
        ));
    }
}
