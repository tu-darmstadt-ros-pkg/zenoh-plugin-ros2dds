//
// A burst of SEDP endpoints larger than one dds_take batch must all be discovered.
// Every live subscription must be served by a route that lists its node and delivers data.

pub mod common;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    time::Duration,
};

use common::{BridgeConfig, Endpoint};
use futures::StreamExt;
use r2r::QosProfile;

const BRIDGE_EP: &str = "tcp/127.0.0.1:7471";
const OPERATOR_DOMAIN: u32 = 1;
fn num_topics() -> usize {
    std::env::var("BURST_TOPICS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300) // ~10x the 32-sample take batch
}
const NODE_FULLNAME: &str = "/burst_sub";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn burst_of_subscriptions_all_get_active_routes() {
    common::init_env();
    std::env::set_var("ROS_DOMAIN_ID", "0");

    let bridge = common::create_bridge_with(&BridgeConfig::new(0).listen(BRIDGE_EP)).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ROS node creating num_topics() subscriptions as fast as possible (the
    // burst). Each received message is forwarded with its topic index so we
    // can verify end-to-end delivery per topic.
    let (msg_tx, msg_rx) = mpsc::channel::<usize>();
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    let ready = Arc::new(AtomicBool::new(false));
    let ready2 = ready.clone();

    let node_thread = std::thread::spawn(move || {
        let ctx = r2r::Context::create().unwrap();
        let mut node = r2r::Node::create(ctx, "burst_sub", "").unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            // THE BURST: no sleeps between creations.
            for i in 0..num_topics() {
                let sub = node
                    .subscribe::<r2r::std_msgs::msg::String>(
                        &format!("/burst_topic_{i}"),
                        QosProfile::default(),
                    )
                    .unwrap();
                let tx = msg_tx.clone();
                tokio::spawn(async move {
                    sub.for_each(|_msg| {
                        let _ = tx.send(i);
                        futures::future::ready(())
                    })
                    .await
                });
            }
            ready2.store(true, Ordering::Relaxed);
            while !stop2.load(Ordering::Relaxed) {
                node.spin_once(Duration::from_millis(10));
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
    });

    while !ready.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Generous settle: rdi poll is 100ms; give discovery every chance.
    tokio::time::sleep(Duration::from_secs(12)).await;

    // Client session connected to the bridge (scouting is off on the bridge).
    let mut scfg = zenoh::Config::default();
    scfg.insert_json5("connect/endpoints", &format!("[\"{BRIDGE_EP}\"]"))
        .unwrap();
    scfg.insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    let session = zenoh::open(scfg).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // 1) Bookkeeping check: every topic has a subscriber route listing the node.
    let routes = common::fetch_routes(&session, "topic/sub").await;
    let mut missing_route = Vec::new();
    for i in 0..num_topics() {
        let key = format!("burst_topic_{i}");
        match routes.get(&key) {
            Some(r) if common::route_serves_node(r, NODE_FULLNAME) => {}
            Some(r) => missing_route.push(format!(
                "{key}: route exists but local_nodes={:?} (node not registered)",
                r.get("local_nodes")
            )),
            None => missing_route.push(format!("{key}: NO route in admin space")),
        }
    }

    // 2) Functional check: a ROS publisher behind a remote bridge publishes on every topic, verify delivery.
    let operator =
        common::create_bridge_with(&BridgeConfig::new(OPERATOR_DOMAIN).connect(BRIDGE_EP)).await;
    let count = num_topics().to_string();
    let publ = Endpoint::spawn(
        "pub_many",
        OPERATOR_DOMAIN,
        &["pub_many", "/burst_topic", "rel", "vol", "10", &count],
    )
    .await;
    let mut received: HashMap<usize, usize> = HashMap::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while received.len() < num_topics() && std::time::Instant::now() < deadline {
        while let Ok(i) = msg_rx.try_recv() {
            *received.entry(i).or_default() += 1;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    publ.stop().await;
    common::close_bridge(operator).await;
    let dead: Vec<String> = (0..num_topics())
        .filter(|i| !received.contains_key(i))
        .map(|i| format!("burst_topic_{i}"))
        .collect();

    stop.store(true, Ordering::Relaxed);
    // a node or bridge still running while the process exits can crash its teardown
    node_thread.join().expect("node thread panicked");
    let _ = session.close().await;
    common::close_bridge(bridge).await;

    println!(
        "=== burst result: {}/{} topics delivered data; {} route-bookkeeping problems ===",
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
        "CLAIM VERIFIED (test red): {} subscriptions have no/incomplete route, \
         {} topics deliver no data after a {}-endpoint discovery burst",
        missing_route.len(),
        dead.len(),
        num_topics()
    );
}
