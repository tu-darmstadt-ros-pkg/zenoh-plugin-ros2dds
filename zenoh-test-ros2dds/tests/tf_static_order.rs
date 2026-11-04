//
// A late /tf_static listener must get every static transform whatever the start order.
// One bridge Writer stands in for all static_transform_publishers, each TRANSIENT_LOCAL KEEP_LAST(1).
// 20 publishers exceed the cache multiplier 10, so the bridges must size the history from the publisher count.

pub mod common;

use std::{collections::BTreeSet, time::Duration};

use common::{operator_bridge_cfg, robot_bridge_cfg, Endpoint};

const SETTLE: Duration = Duration::from_secs(3);
const FIRST_DOMAIN: u32 = 200;
const FIRST_PORT: u16 = 7800;

#[derive(Clone, Copy, Debug)]
enum Order {
    PubsFirst,
    SubFirst,
    BridgesLast,
}

async fn spawn_tf_pubs(domain: u32, n: usize) -> Vec<Endpoint> {
    let mut v = Vec::new();
    for i in 0..n {
        let frame = format!("frame_{i:02}");
        v.push(
            Endpoint::spawn(
                &frame,
                domain,
                &["tf_pub", "/tf_static", "rel", "tl", "1", &frame],
            )
            .await,
        );
    }
    v
}

const LISTENER: [&str; 5] = ["tf_sub", "/tf_static", "rel", "tl", "100"];

async fn run_case(index: usize, n: usize, order: Order) -> Vec<String> {
    let robot_domain = FIRST_DOMAIN + 2 * index as u32;
    let operator_domain = robot_domain + 1;
    let port = FIRST_PORT + index as u16;

    let (pubs, a, robot, operator);
    match order {
        Order::PubsFirst => {
            pubs = spawn_tf_pubs(robot_domain, n).await;
            robot = common::create_bridge_with(&robot_bridge_cfg(robot_domain, port)).await;
            operator =
                common::create_bridge_with(&operator_bridge_cfg(operator_domain, port)).await;
            tokio::time::sleep(SETTLE).await;
            a = Endpoint::spawn("A", operator_domain, &LISTENER).await;
        }
        Order::SubFirst => {
            robot = common::create_bridge_with(&robot_bridge_cfg(robot_domain, port)).await;
            operator =
                common::create_bridge_with(&operator_bridge_cfg(operator_domain, port)).await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            a = Endpoint::spawn("A", operator_domain, &LISTENER).await;
            tokio::time::sleep(SETTLE).await;
            pubs = spawn_tf_pubs(robot_domain, n).await;
        }
        Order::BridgesLast => {
            pubs = spawn_tf_pubs(robot_domain, n).await;
            a = Endpoint::spawn("A", operator_domain, &LISTENER).await;
            robot = common::create_bridge_with(&robot_bridge_cfg(robot_domain, port)).await;
            operator =
                common::create_bridge_with(&operator_bridge_cfg(operator_domain, port)).await;
        }
    }
    tokio::time::sleep(SETTLE).await;
    // B joins while A keeps the route active
    let b = Endpoint::spawn("B", operator_domain, &LISTENER).await;
    tokio::time::sleep(SETTLE).await;

    let expected: BTreeSet<String> = (0..n).map(|i| format!("frame_{i:02}")).collect();
    let mut failures = Vec::new();
    for l in [&a, &b] {
        let got = l.frames();
        if got != expected {
            let missing: Vec<&String> = expected.difference(&got).collect();
            failures.push(format!(
                "n={n} order={order:?}: listener {} got {}/{n} frames, missing {missing:?}",
                l.label,
                got.len()
            ));
        }
    }

    b.stop().await;
    a.stop().await;
    for p in pubs {
        p.stop().await;
    }
    common::close_bridge(operator).await;
    common::close_bridge(robot).await;
    failures
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn late_tf_static_listener_gets_all_frames_in_any_order() {
    common::init_env();
    let mut tasks = Vec::new();
    let mut index = 0;
    for n in [5usize, 20] {
        for order in [Order::PubsFirst, Order::SubFirst, Order::BridgesLast] {
            let i = index;
            index += 1;
            tasks.push(tokio::spawn(async move {
                let f = run_case(i, n, order).await;
                eprintln!(
                    "[{}] n={n} order={order:?}",
                    if f.is_empty() { "PASS" } else { "FAIL" }
                );
                f
            }));
        }
    }
    let mut failures = Vec::new();
    for t in tasks {
        // a panicking case must not hide the others
        match t.await {
            Ok(f) => failures.extend(f),
            Err(e) => failures.push(format!("case panicked: {e}")),
        }
    }
    assert!(failures.is_empty(), "\n  - {}", failures.join("\n  - "));
}

/// A VOLATILE /tf_static publisher (e.g. `ros2 topic pub`) must cost late listeners no static frame.
/// A checks the robot bridge's cache, B the operator bridge's Writer history, both hold 5 x multiplier 10 samples.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn volatile_publisher_keeps_static_frames() {
    common::init_env();
    let (robot_domain, operator_domain, port) = (52, 53, 7806);
    // 50 Hz outnumbers the 50 sample histories within a second
    const FLOOD: Duration = Duration::from_secs(3);
    let pubs = spawn_tf_pubs(robot_domain, 5).await;
    // a mixed topic with a fixed Writer set, each TRANSIENT_LOCAL sample must reach a VOLATILE listener once
    let dedup_pubs = [
        Endpoint::spawn(
            "tick",
            robot_domain,
            &["tf_stream", "/tf_dedup", "rel", "tl", "1", "tick"],
        )
        .await,
        Endpoint::spawn(
            "v",
            robot_domain,
            &["tf_pub", "/tf_dedup", "rel", "vol", "1", "v"],
        )
        .await,
    ];
    let robot = common::create_bridge_with(&robot_bridge_cfg(robot_domain, port)).await;
    let flood = Endpoint::spawn(
        "flood",
        robot_domain,
        &["tf_stream", "/tf_static", "rel", "vol", "1", "flood"],
    )
    .await;
    tokio::time::sleep(FLOOD).await;

    let operator = common::create_bridge_with(&operator_bridge_cfg(operator_domain, port)).await;
    let a = Endpoint::spawn("A", operator_domain, &LISTENER).await;
    let live = Endpoint::spawn(
        "live",
        operator_domain,
        &["tf_sub", "/tf_static", "rel", "vol", "100"],
    )
    .await;
    let ticks = Endpoint::spawn(
        "ticks",
        operator_domain,
        &["tf_sub", "/tf_dedup", "rel", "vol", "100"],
    )
    .await;
    tokio::time::sleep(SETTLE + FLOOD).await;
    let b = Endpoint::spawn("B", operator_domain, &LISTENER).await;
    let ticks_before = ticks.frame_count("tick");
    tokio::time::sleep(SETTLE).await;
    // 50 Hz for 3 s, twice as many if TRANSIENT_LOCAL samples also take the VOLATILE path
    let tick_rate = (ticks.frame_count("tick") - ticks_before) as f64 / SETTLE.as_secs_f64();

    let expected: BTreeSet<String> = (0..5).map(|i| format!("frame_{i:02}")).collect();
    let mut failures = Vec::new();
    for l in [&a, &b] {
        let missing: Vec<String> = expected.difference(&l.frames()).cloned().collect();
        if !missing.is_empty() {
            failures.push(format!("listener {} misses {missing:?}", l.label));
        }
    }
    if !live.frames().contains("flood") {
        failures.push("VOLATILE listener got nothing from the VOLATILE publisher".to_string());
    }
    if !(20.0..=75.0).contains(&tick_rate) {
        failures.push(format!(
            "VOLATILE listener got {tick_rate:.0} ticks/s of 50/s"
        ));
    }

    for e in [ticks, b, live, a, flood] {
        e.stop().await;
    }
    for e in dedup_pubs {
        e.stop().await;
    }
    for p in pubs {
        p.stop().await;
    }
    common::close_bridge(operator).await;
    common::close_bridge(robot).await;
    assert!(failures.is_empty(), "\n  - {}", failures.join("\n  - "));
}
