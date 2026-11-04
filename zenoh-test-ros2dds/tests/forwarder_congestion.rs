//
// A stalled link must not freeze the bridge's async runtime.
//
// RELIABLE routes put with CongestionControl::Block, which blocks the calling thread
// until zenoh closes the transport after wait_before_close (5 s). The bridge runs its
// routes manager, liveliness and service tasks on the same tokio runtime as the test,
// so a heartbeat task on 2 workers shows whether blocking puts starve it.
//
//   r2r publishers (domain 30, 4 RELIABLE topics) -> bridge -> FreezableProxy -> zenoh session

pub mod common;

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use common::{BridgeConfig, FreezableProxy, LogBuf};
use r2r::QosProfile;

const DOMAIN: u32 = 30;
const SESSION_PORT: u16 = 7900;
const PROXY_PORT: u16 = 7901;
const TOPICS: usize = 4;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_link_does_not_block_runtime() {
    common::init_env();
    std::env::set_var("ROS_DOMAIN_ID", DOMAIN.to_string());
    let logbuf = LogBuf::install();

    let mut scfg = zenoh::Config::default();
    scfg.insert_json5(
        "listen/endpoints",
        &format!("[\"tcp/127.0.0.1:{SESSION_PORT}\"]"),
    )
    .unwrap();
    scfg.insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    let session = zenoh::open(scfg).await.unwrap();
    let received = Arc::new(AtomicU64::new(0));
    for i in 0..TOPICS {
        let received = received.clone();
        session
            .declare_subscriber(format!("blob{i}"))
            .callback(move |_| {
                received.fetch_add(1, Ordering::Relaxed);
            })
            .background()
            .await
            .unwrap();
    }

    let proxy = FreezableProxy::start(PROXY_PORT, SESSION_PORT).await;
    let bridge = common::create_bridge_with(
        &BridgeConfig::new(DOMAIN).connect(format!("tcp/127.0.0.1:{PROXY_PORT}")),
    )
    .await;

    let stop = Arc::new(AtomicBool::new(false));
    let stop_pub = stop.clone();
    std::thread::spawn(move || {
        let ctx = r2r::Context::create().unwrap();
        let mut node = r2r::Node::create(ctx, "congested_pubs", "").unwrap();
        let publishers: Vec<_> = (0..TOPICS)
            .map(|i| {
                node.create_publisher::<r2r::std_msgs::msg::String>(
                    &format!("/blob{i}"),
                    QosProfile::default(),
                )
                .unwrap()
            })
            .collect();
        let blob = r2r::std_msgs::msg::String {
            data: "x".repeat(512 * 1024),
        };
        while !stop_pub.load(Ordering::Relaxed) {
            for p in &publishers {
                let _ = p.publish(&blob);
            }
            node.spin_once(Duration::from_millis(0));
            std::thread::sleep(Duration::from_millis(20));
        }
    });

    let deadline = Instant::now() + Duration::from_secs(20);
    while received.load(Ordering::Relaxed) == 0 {
        assert!(
            Instant::now() < deadline,
            "no data crossed the bridge before the stall"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let max_gap = Arc::new(Mutex::new(Duration::ZERO));
    let heartbeat = tokio::spawn({
        let max_gap = max_gap.clone();
        async move {
            let mut last = Instant::now();
            loop {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let now = Instant::now();
                let mut gap = max_gap.lock().unwrap();
                *gap = (*gap).max(now - last);
                last = now;
            }
        }
    });

    proxy.freeze();
    // wait_before_close is 5 s, the transport closes within this window
    std::thread::sleep(Duration::from_secs(8));
    tokio::time::sleep(Duration::from_millis(200)).await;
    heartbeat.abort();
    stop.store(true, Ordering::Relaxed);

    let congested = !logbuf.lines_containing("non droppable").is_empty();
    let max_gap = *max_gap.lock().unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(10), common::close_bridge(bridge)).await;

    assert!(
        congested,
        "the stalled link never blocked a RELIABLE put, the test has no teeth"
    );
    assert!(
        max_gap < Duration::from_secs(1),
        "the bridge's runtime stalled for {max_gap:?} while the link was frozen"
    );
}
