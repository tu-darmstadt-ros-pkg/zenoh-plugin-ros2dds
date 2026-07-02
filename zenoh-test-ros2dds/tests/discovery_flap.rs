//
// Discovery-integrity test: RViz-style subscription FLAP
// (create -> destroy within milliseconds -> recreate).
//
// CLAIMS UNDER TEST (audit findings, both filmed in the production log
// 2026-07-02T04:09:14.775-.871 on /athena/global_costmap/voxel_layer):
//   1. A DiscoveredMsgSub event can be processed after its reader was already
//      destroyed -> routes_mgr logs "Failed to get DDS info for any Reader"
//      and the event is dropped WITHOUT retry.
//   2. The failure leaves NodeInfo.msg_sub committed, so ONLY a full
//      destroy(+cleanup)/recreate cycle can ever re-emit the event; if the
//      remove/add interleaving goes the wrong way the topic is permanently
//      suppressed for that node ("subscription exists, bridge never routes").
//
// The test asserts DESIRED behavior: after the flap settles, every topic that
// has a LIVE subscription is served by an active route and delivers data.
// RED under current code verifies the claims; GREEN after a fix = regression
// test.

pub mod common;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    time::Duration,
};

use common::BridgeConfig;
use futures::StreamExt;
use r2r::QosProfile;

const BRIDGE_EP: &str = "tcp/127.0.0.1:7472";
fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn num_topics() -> usize {
    env_usize("FLAP_TOPICS", 60)
}
fn flap_cycles() -> usize {
    env_usize("FLAP_CYCLES", 8)
}
const NODE_FULLNAME: &str = "/flap_sub";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flapped_subscriptions_all_get_active_routes() {
    common::init_env();
    std::env::set_var("ROS_DOMAIN_ID", "0");

    // Capture the bridge's own log output: the discovery race manifests as
    // "Error updating route: Failed to get DDS info for any Reader" (the
    // production signature from 2026-07-02T04:09:14.775) even when the final
    // state self-heals via the re-created subscription.
    let logbuf = common::LogBuf::install();

    let _bridge = common::create_bridge_with(&BridgeConfig::new(0).listen(BRIDGE_EP)).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let (msg_tx, msg_rx) = mpsc::channel::<usize>();
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    let done_flapping = Arc::new(AtomicBool::new(false));
    let done2 = done_flapping.clone();

    std::thread::spawn(move || {
        let ctx = r2r::Context::create().unwrap();
        let mut node = r2r::Node::create(ctx, "flap_sub", "").unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            // Background spinner cadence is interleaved manually below because
            // r2r finalizes dropped subscriptions during spin_once: the flap
            // needs spins BETWEEN drop and re-create to actually destroy the
            // DDS reader (mirrors RViz destroy@.778 / recreate@.870).
            for i in 0..num_topics() {
                let topic = format!("/flap_topic_{i}");
                for _cycle in 0..flap_cycles() {
                    let s = node
                        .subscribe::<r2r::std_msgs::msg::String>(&topic, QosProfile::default())
                        .unwrap();
                    // let discovery see the creation
                    node.spin_once(Duration::from_millis(1));
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    drop(s);
                    // let the destroy hit DDS
                    node.spin_once(Duration::from_millis(1));
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                // Final keeper subscription — this one stays alive.
                let keeper = node
                    .subscribe::<r2r::std_msgs::msg::String>(&topic, QosProfile::default())
                    .unwrap();
                let tx = msg_tx.clone();
                tokio::spawn(async move {
                    keeper
                        .for_each(|_msg| {
                            let _ = tx.send(i);
                            futures::future::ready(())
                        })
                        .await
                });
                node.spin_once(Duration::from_millis(1));
            }
            done2.store(true, Ordering::Relaxed);
            while !stop2.load(Ordering::Relaxed) {
                node.spin_once(Duration::from_millis(10));
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
    });

    while !done_flapping.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_secs(12)).await;

    let mut scfg = zenoh::Config::default();
    scfg.insert_json5("connect/endpoints", &format!("[\"{BRIDGE_EP}\"]"))
        .unwrap();
    scfg.insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    let session = zenoh::open(scfg).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // 1) Bookkeeping: every topic's route lists the node.
    let routes = common::fetch_routes(&session, "topic/sub").await;
    let mut missing_route = Vec::new();
    for i in 0..num_topics() {
        let key = format!("flap_topic_{i}");
        match routes.get(&key) {
            Some(r) if common::route_serves_node(r, NODE_FULLNAME) => {}
            Some(r) => missing_route.push(format!(
                "{key}: route exists but local_nodes={:?}",
                r.get("local_nodes")
            )),
            None => missing_route.push(format!("{key}: NO route in admin space")),
        }
    }

    // 2) Functional: publish on every topic, verify the keeper receives.
    let mut publishers = Vec::new();
    for i in 0..num_topics() {
        publishers.push(
            session
                .declare_publisher(format!("flap_topic_{i}"))
                .await
                .unwrap(),
        );
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let payload =
        cdr::serialize::<_, _, cdr::CdrLe>(&"ping".to_string(), cdr::size::Infinite).unwrap();
    for _round in 0..5 {
        for p in &publishers {
            p.put(payload.clone()).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut received: HashMap<usize, usize> = HashMap::new();
    while let Ok(i) = msg_rx.try_recv() {
        *received.entry(i).or_default() += 1;
    }
    let dead: Vec<String> = (0..num_topics())
        .filter(|i| !received.contains_key(i))
        .map(|i| format!("flap_topic_{i}"))
        .collect();

    stop.store(true, Ordering::Relaxed);

    // Race evidence: the no-retry drop fired even if the end state healed.
    let race_errors = logbuf.lines_containing("Error updating route");
    println!(
        "=== race evidence: {} 'Error updating route' occurrences (event dropped, no retry) ===",
        race_errors.len()
    );
    for l in race_errors.iter().take(10) {
        println!("  {l}");
    }
    if !race_errors.is_empty() {
        println!(
            "CLAIM (no-retry discovery drop) VERIFIED: the race fired {} time(s); \
             any healing was solely due to the subsequent re-create.",
            race_errors.len()
        );
    }

    println!(
        "=== flap result: {}/{} topics delivered data; {} route-bookkeeping problems ===",
        num_topics() - dead.len(),
        num_topics(),
        missing_route.len()
    );
    for m in &missing_route {
        println!("  route problem: {m}");
    }
    for d in &dead {
        println!("  no data:       {d}");
    }

    assert!(
        missing_route.is_empty() && dead.is_empty(),
        "CLAIM VERIFIED (test red): after create/destroy/recreate flaps, \
         {} topics have no/incomplete route and {} deliver no data — \
         live subscriptions permanently unserved (the RViz/echo symptom)",
        missing_route.len(),
        dead.len()
    );
}
