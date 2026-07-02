//
// Functional smoke test for the DDS->Zenoh RoutePublisher data path,
// using explicit endpoints (NO multicast scouting - runs alongside live ROS
// systems that hold the default scouting port).
//
// Also serves as the regression test for the forwarder-queue rework: the DDS
// reader callback must enqueue only, the per-route forwarder task performs the
// zenoh put, and the admin space must expose fwd_msg_count/dropped_msg_count.

pub mod common;

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use common::BridgeConfig;
use r2r::QosProfile;

const BRIDGE_EP: &str = "tcp/127.0.0.1:7475";
const NUM_MSGS: usize = 100;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dds_to_zenoh_route_forwards_and_counts() {
    common::init_env();
    std::env::set_var("ROS_DOMAIN_ID", "0");

    let _bridge = common::create_bridge_with(&BridgeConfig::new(0).listen(BRIDGE_EP)).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Zenoh subscriber via explicit endpoint (this also makes the bridge's
    // publisher "matching", which activates the route's DDS reader).
    let mut scfg = zenoh::Config::default();
    scfg.insert_json5("connect/endpoints", &format!("[\"{BRIDGE_EP}\"]"))
        .unwrap();
    scfg.insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    let session = zenoh::open(scfg).await.unwrap();
    let received = Arc::new(AtomicU64::new(0));
    {
        let received = received.clone();
        session
            .declare_subscriber("smoke_topic")
            .callback(move |_s| {
                received.fetch_add(1, Ordering::Relaxed);
            })
            .background()
            .await
            .unwrap();
    }

    // ROS publisher.
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    let published = Arc::new(AtomicU64::new(0));
    let published2 = published.clone();
    std::thread::spawn(move || {
        let ctx = r2r::Context::create().unwrap();
        let mut node = r2r::Node::create(ctx, "smoke_pub", "").unwrap();
        let publisher = node
            .create_publisher::<r2r::std_msgs::msg::String>("/smoke_topic", QosProfile::default())
            .unwrap();
        let msg = r2r::std_msgs::msg::String {
            data: "smoke".to_string(),
        };
        // Wait for discovery + route activation before publishing.
        std::thread::sleep(Duration::from_secs(4));
        for _ in 0..NUM_MSGS {
            if publisher.publish(&msg).is_ok() {
                published2.fetch_add(1, Ordering::Relaxed);
            }
            node.spin_once(Duration::from_millis(0));
            std::thread::sleep(Duration::from_millis(20));
        }
        while !stop2.load(Ordering::Relaxed) {
            node.spin_once(Duration::from_millis(10));
            std::thread::sleep(Duration::from_millis(10));
        }
    });

    // Wait for the publishing to finish plus settle.
    tokio::time::sleep(Duration::from_secs(10)).await;
    stop.store(true, Ordering::Relaxed);

    let pub_count = published.load(Ordering::Relaxed);
    let recv_count = received.load(Ordering::Relaxed);

    // Admin space: the route must expose the forward counters.
    let routes = common::fetch_routes(&session, "topic/pub").await;
    println!("admin space: {} publisher routes", routes.len());
    let route = routes.get("smoke_topic");
    println!("smoke_topic route JSON: {route:?}");
    let fwd = route
        .and_then(|r| r.get("fwd_msg_count"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let dropped = route
        .and_then(|r| r.get("dropped_msg_count"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    println!(
        "published={pub_count} received={recv_count} route.fwd_msg_count={fwd} route.dropped_msg_count={dropped}"
    );

    assert!(pub_count as usize >= NUM_MSGS, "publisher failed");
    // Allow for a few messages published before matching completed.
    assert!(
        recv_count >= (NUM_MSGS as u64) * 9 / 10,
        "zenoh subscriber received only {recv_count}/{pub_count} messages - \
         DDS->Zenoh forwarder path broken"
    );
    assert!(
        fwd >= recv_count,
        "admin space fwd_msg_count ({fwd}) below received count ({recv_count})"
    );
    assert_eq!(dropped, 0, "unexpected drops on an idle local link");
}
