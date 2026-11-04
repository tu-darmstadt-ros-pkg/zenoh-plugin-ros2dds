//
// A bridged topic's QoS must not depend on the order in which ROS endpoints and bridges start.
//
// A "robot" bridge (publisher side) and an "operator" bridge (subscriber side) run on separate DDS domains.
// For every publisher QoS, first subscriber QoS and start order
//   sub1 gets data exactly when plain DDS would match it, except that the bridge always offers RELIABLE (#23),
//   sub2, started last with the publisher's QoS, always gets data and, for RELIABLE TRANSIENT_LOCAL, the history.
// plain_dds_oracle pins the plain DDS behaviour these expectations come from.
// A TL sub1 must receive nothing from a VOLATILE publisher, so the fix must never create a TL Writer that a VOLATILE publisher replaces later.
// BridgesLast depends on discovery timing, PubFirst and SubFirst are deterministic.
// QOS_MATRIX_FILTER=<substring of a case name> runs a subset.

pub mod common;

use std::{sync::Arc, time::Duration};

use common::{operator_bridge_cfg, robot_bridge_cfg, Endpoint};

const TOPIC: &str = "/qos_matrix";
const HISTORY_MSGS: usize = 5;
const SETTLE: Duration = Duration::from_secs(3);
const PARALLEL_CASES: usize = 8;
// the publisher keeps publishing at 10 Hz, its history must outlast the case
const DEPTH: &str = "1000";
// a bridge sizing anything from the first subscriber evicts the history before sub2 joins
const SUB1_DEPTH: &str = "1";
const FIRST_DOMAIN: u32 = 100;
const FIRST_PORT: u16 = 7700;

#[derive(Clone, Copy, Debug, PartialEq)]
struct Q {
    reliable: bool,
    transient_local: bool,
}

impl Q {
    fn rel(&self) -> &'static str {
        if self.reliable {
            "rel"
        } else {
            "be"
        }
    }
    fn dur(&self) -> &'static str {
        if self.transient_local {
            "tl"
        } else {
            "vol"
        }
    }
    fn name(&self) -> String {
        format!("{}/{}", self.rel(), self.dur())
    }
    fn reliable_tl(&self) -> bool {
        self.reliable && self.transient_local
    }
}

/// Whether plain DDS delivers a publisher's history to a late subscriber.
fn dds_delivers_history(publ: Q, sub: Q) -> bool {
    publ.reliable_tl() && sub.reliable_tl()
}

#[derive(Clone, Copy, Debug)]
enum Order {
    /// publisher, then bridges, then subscriber ("robot was up before RViz")
    PubFirst,
    /// bridges, then subscriber, then publisher ("RViz open, robot boots")
    SubFirst,
    /// publisher and subscriber, then bridges ("link comes up last")
    BridgesLast,
}

const ALL_Q: [Q; 4] = [
    Q {
        reliable: true,
        transient_local: true,
    },
    Q {
        reliable: true,
        transient_local: false,
    },
    Q {
        reliable: false,
        transient_local: true,
    },
    Q {
        reliable: false,
        transient_local: false,
    },
];
const ALL_ORDERS: [Order; 3] = [Order::PubFirst, Order::SubFirst, Order::BridgesLast];

struct Case {
    index: usize,
    publ: Q,
    sub1: Q,
    order: Order,
}

impl Case {
    fn name(&self) -> String {
        format!(
            "pub={} sub1={} order={:?}",
            self.publ.name(),
            self.sub1.name(),
            self.order
        )
    }
}

async fn run_case(case: &Case) -> Vec<String> {
    let robot_domain = FIRST_DOMAIN + 2 * case.index as u32;
    let operator_domain = robot_domain + 1;
    let port = FIRST_PORT + case.index as u16;
    let pub_args = ["pub", TOPIC, case.publ.rel(), case.publ.dur(), DEPTH, "p"];
    let sub1_args = ["sub", TOPIC, case.sub1.rel(), case.sub1.dur(), SUB1_DEPTH];
    let sub2_args = ["sub", TOPIC, case.publ.rel(), case.publ.dur(), DEPTH];

    let name = case.name();
    let (l_pub, l_sub1, l_sub2) = (
        format!("[{name}] pub"),
        format!("[{name}] sub1"),
        format!("[{name}] sub2"),
    );
    let (publ, sub1, robot, operator);
    match case.order {
        Order::PubFirst => {
            publ = Endpoint::spawn(&l_pub, robot_domain, &pub_args).await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            robot = common::create_bridge_with(&robot_bridge_cfg(robot_domain, port)).await;
            operator =
                common::create_bridge_with(&operator_bridge_cfg(operator_domain, port)).await;
            tokio::time::sleep(SETTLE).await;
            sub1 = Endpoint::spawn(&l_sub1, operator_domain, &sub1_args).await;
        }
        Order::SubFirst => {
            robot = common::create_bridge_with(&robot_bridge_cfg(robot_domain, port)).await;
            operator =
                common::create_bridge_with(&operator_bridge_cfg(operator_domain, port)).await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            sub1 = Endpoint::spawn(&l_sub1, operator_domain, &sub1_args).await;
            tokio::time::sleep(SETTLE).await;
            publ = Endpoint::spawn(&l_pub, robot_domain, &pub_args).await;
        }
        Order::BridgesLast => {
            publ = Endpoint::spawn(&l_pub, robot_domain, &pub_args).await;
            sub1 = Endpoint::spawn(&l_sub1, operator_domain, &sub1_args).await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            robot = common::create_bridge_with(&robot_bridge_cfg(robot_domain, port)).await;
            operator =
                common::create_bridge_with(&operator_bridge_cfg(operator_domain, port)).await;
        }
    }
    tokio::time::sleep(SETTLE).await;
    let sub2 = Endpoint::spawn(&l_sub2, operator_domain, &sub2_args).await;
    tokio::time::sleep(SETTLE + Duration::from_secs(1)).await;

    let mut failures = Vec::new();
    let ctx = format!(
        "\n      sub1: {}\n      sub2: {}",
        sub1.raw_result(),
        sub2.raw_result()
    );

    // DDS durability semantics, the bridge always offers RELIABLE
    let sub1_should_match = !(case.sub1.transient_local && !case.publ.transient_local);
    if sub1_should_match && sub1.live() == 0 {
        failures.push(format!(
            "{name}: sub1 got no live data but should match{ctx}"
        ));
    }
    if !sub1_should_match && sub1.live() + sub1.history() > 0 {
        failures.push(format!(
            "{name}: sub1 (TL) received data from a VOLATILE publisher; DDS semantics say it is incompatible{ctx}"
        ));
    }
    for (label, e) in [("sub1", &sub1), ("sub2", &sub2)] {
        if !e.is_reporting() {
            failures.push(format!("{name}: {label} died or never reported{ctx}"));
        }
    }

    // sub2 has the publisher's QoS and always matches in plain DDS
    if sub2.live() == 0 {
        failures.push(format!(
            "{name}: late sub2 with the publisher's own QoS got no live data (route poisoned){ctx}"
        ));
    }
    if dds_delivers_history(case.publ, case.publ) && sub2.history() != HISTORY_MSGS {
        failures.push(format!(
            "{name}: late TL sub2 got {}/{HISTORY_MSGS} history messages{ctx}",
            sub2.history()
        ));
    }

    sub2.stop().await;
    sub1.stop().await;
    publ.stop().await;
    common::close_bridge(operator).await;
    common::close_bridge(robot).await;
    failures
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bridged_qos_is_independent_of_start_order() {
    common::init_env();
    let filter = std::env::var("QOS_MATRIX_FILTER").unwrap_or_default();

    let mut cases = Vec::new();
    for publ in ALL_Q {
        for sub1 in ALL_Q {
            for order in ALL_ORDERS {
                let index = cases.len();
                cases.push(Case {
                    index,
                    publ,
                    sub1,
                    order,
                });
            }
        }
    }
    cases.retain(|c| c.name().contains(&filter));
    assert!(!cases.is_empty(), "QOS_MATRIX_FILTER matched no case");

    let sem = Arc::new(tokio::sync::Semaphore::new(PARALLEL_CASES));
    let mut tasks = Vec::new();
    for case in cases {
        let sem = sem.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let failures = run_case(&case).await;
            eprintln!(
                "[{}] {}",
                if failures.is_empty() { "PASS" } else { "FAIL" },
                case.name()
            );
            failures
        }));
    }
    let total = tasks.len();
    let mut failures = Vec::new();
    for t in tasks {
        // a panicking case must not hide the others
        match t.await {
            Ok(f) => failures.extend(f),
            Err(e) => failures.push(format!("case panicked: {e}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} problem(s) across {total} cases:\n  - {}",
        failures.len(),
        failures.join("\n  - ")
    );
}

/// Pins the plain-DDS behaviour the matrix expectations are derived from: on a
/// single domain (no bridge), a late subscriber receives the history only when
/// publisher and subscriber are both RELIABLE + TRANSIENT_LOCAL, and live data
/// only when the QoS are compatible.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_dds_oracle() {
    common::init_env();
    const ORACLE_FIRST_DOMAIN: u32 = 223;
    let mut tasks = Vec::new();
    for (i, publ) in [ALL_Q[0], ALL_Q[2]].into_iter().enumerate() {
        for (j, sub) in ALL_Q.into_iter().enumerate() {
            let domain = ORACLE_FIRST_DOMAIN + (i * ALL_Q.len() + j) as u32;
            tasks.push(tokio::spawn(async move {
                let p = Endpoint::spawn(
                    "pub",
                    domain,
                    &["pub", TOPIC, publ.rel(), publ.dur(), DEPTH, "p"],
                )
                .await;
                tokio::time::sleep(Duration::from_secs(2)).await;
                let s =
                    Endpoint::spawn("sub", domain, &["sub", TOPIC, sub.rel(), sub.dur(), DEPTH])
                        .await;
                tokio::time::sleep(SETTLE).await;
                let (history, live, raw) = (s.history(), s.live(), s.raw_result());
                s.stop().await;
                p.stop().await;
                let compatible = (publ.reliable || !sub.reliable)
                    && (publ.transient_local || !sub.transient_local);
                let mut f = Vec::new();
                let want_history = if dds_delivers_history(publ, sub) {
                    HISTORY_MSGS
                } else {
                    0
                };
                if history != want_history {
                    f.push(format!(
                        "pub={} sub={}: history {history} != {want_history}: {raw}",
                        publ.name(),
                        sub.name()
                    ));
                }
                if (live > 0) != compatible {
                    f.push(format!(
                        "pub={} sub={}: live={live} but compatible={compatible}: {raw}",
                        publ.name(),
                        sub.name()
                    ));
                }
                f
            }));
        }
    }
    let mut failures = Vec::new();
    for t in tasks {
        failures.extend(t.await.expect("oracle task panicked"));
    }
    assert!(failures.is_empty(), "\n  - {}", failures.join("\n  - "));
}
