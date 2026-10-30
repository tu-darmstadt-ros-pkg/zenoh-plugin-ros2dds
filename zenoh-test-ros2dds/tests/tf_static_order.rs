//
// A late /tf_static listener must get every static transform whatever the start order.
// One bridge Writer stands in for all static_transform_publishers, each TRANSIENT_LOCAL KEEP_LAST(1).
// 20 publishers exceed the old cache multiplier 10 and fit the new 25.

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
