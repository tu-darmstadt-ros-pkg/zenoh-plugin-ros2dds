//
// Discovery-integrity test: SEDP endpoint BURST.
//
// CLAIM UNDER TEST (audit finding; suspected mechanism behind "UI subscribes a
// point cloud but the bridge never learns about it"):
//   dds_discovery.rs `on_data` is an edge-triggered DATA_AVAILABLE listener
//   doing a single bounded `dds_take` of 32 samples per invocation with no
//   loop-until-empty. A burst of endpoint creations larger than one batch can
//   leave discovery samples unprocessed, so some subscriptions never produce a
//   DiscoveredMsgSub event -> no route serves them -> the topic is dead until
//   another local subscriber appears (the "echo revives it" symptom).
//
// The test asserts DESIRED behavior (every live subscription is served by a
// route that lists its node AND actually delivers data). A RED result under
// the current code verifies the claim; after a fix this becomes a regression
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

const BRIDGE_EP: &str = "tcp/127.0.0.1:7471";
const NUM_TOPICS: usize = 120; // well above the 32-sample take batch
const NODE_FULLNAME: &str = "/burst_sub";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn burst_of_subscriptions_all_get_active_routes() {
    common::init_env();
    std::env::set_var("ROS_DOMAIN_ID", "0");

    let _bridge = common::create_bridge_with(&BridgeConfig::new(0).listen(BRIDGE_EP)).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ROS node creating NUM_TOPICS subscriptions as fast as possible (the
    // burst). Each received message is forwarded with its topic index so we
    // can verify end-to-end delivery per topic.
    let (msg_tx, msg_rx) = mpsc::channel::<usize>();
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    let ready = Arc::new(AtomicBool::new(false));
    let ready2 = ready.clone();

    std::thread::spawn(move || {
        let ctx = r2r::Context::create().unwrap();
        let mut node = r2r::Node::create(ctx, "burst_sub", "").unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            // THE BURST: no sleeps between creations.
            for i in 0..NUM_TOPICS {
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
    for i in 0..NUM_TOPICS {
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

    // 2) Functional check: publish on every topic via zenoh, verify delivery.
    let mut publishers = Vec::new();
    for i in 0..NUM_TOPICS {
        publishers.push(
            session
                .declare_publisher(format!("burst_topic_{i}"))
                .await
                .unwrap(),
        );
    }
    tokio::time::sleep(Duration::from_secs(2)).await; // matching
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
    let dead: Vec<String> = (0..NUM_TOPICS)
        .filter(|i| !received.contains_key(i))
        .map(|i| format!("burst_topic_{i}"))
        .collect();

    stop.store(true, Ordering::Relaxed);

    println!(
        "=== burst result: {}/{NUM_TOPICS} topics delivered data; {} route-bookkeeping problems ===",
        NUM_TOPICS - dead.len(),
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
         {} topics deliver no data after a {NUM_TOPICS}-endpoint discovery burst",
        missing_route.len(),
        dead.len()
    );
}
