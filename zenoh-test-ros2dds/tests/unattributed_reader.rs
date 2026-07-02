//
// THE production UI scenario, distilled: a DDS reader that exists at the DDS
// level but is NEVER declared in ros_discovery_info (hector-UI subscriptions
// and ros2_control endpoints show as _NODE_NAME_UNKNOWN_ in
// `ros2 topic info -v`). Verified live on 2026-07-02: the UI's PointCloud2
// reader (GID ...5e04) was known to the bridge's DDS discovery but absent
// from the ROS graph, so no route ever served it - clouds only flowed while a
// terminal echo (a properly-declared reader) kept the route active.
//
// With publication-matched activation, the route's DDS writer detecting ANY
// matched reader is authoritative local interest. This test builds:
//
//   r2r publisher (domain 10) -> bridge R -> tcp -> bridge O (domain 0)
//   -> raw cyclors reader on domain 0 (blob topic, NO ros_discovery_info)
//
// and asserts the raw reader receives data and the route reports the
// synthetic "<matched_dds_readers>" local node.

pub mod common;

use std::{
    ffi::CString,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use common::BridgeConfig;
use r2r::QosProfile;

const R_EP: &str = "tcp/127.0.0.1:7476";
const O_EP: &str = "tcp/127.0.0.1:7477";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unattributed_dds_reader_gets_served() {
    common::init_env();
    std::env::set_var("ROS_DOMAIN_ID", "10"); // r2r publisher side

    let _r = common::create_bridge_with(&BridgeConfig::new(10).listen(R_EP)).await;
    let _o = common::create_bridge_with(&BridgeConfig::new(0).connect(R_EP).listen(O_EP)).await;

    // "Robot"-side ROS publisher on domain 10.
    let stop = Arc::new(AtomicBool::new(false));
    let stop_pub = stop.clone();
    std::thread::spawn(move || {
        let ctx = r2r::Context::create().unwrap();
        let mut node = r2r::Node::create(ctx, "hidden_pub", "").unwrap();
        let publisher = node
            .create_publisher::<r2r::std_msgs::msg::String>("/hidden_cloud", QosProfile::default())
            .unwrap();
        let msg = r2r::std_msgs::msg::String {
            data: "cloud".to_string(),
        };
        while !stop_pub.load(Ordering::Relaxed) {
            let _ = publisher.publish(&msg);
            node.spin_once(Duration::from_millis(0));
            std::thread::sleep(Duration::from_millis(50)); // 20 Hz
        }
    });

    // Let both bridges interconnect and the MsgPub announcement create the
    // operator-side RouteSubscriber (whose DDS writer we will match).
    tokio::time::sleep(Duration::from_secs(5)).await;

    // "Operator"-side RAW cyclors reader on domain 0: a DDS endpoint with NO
    // ROS node and NO ros_discovery_info - invisible to the ROS graph.
    let received = Arc::new(AtomicU64::new(0));
    let received2 = received.clone();
    let stop_reader = stop.clone();
    std::thread::spawn(move || unsafe {
        let dp = cyclors::dds_create_participant(0, std::ptr::null(), std::ptr::null());
        assert!(dp > 0, "raw participant creation failed: {dp}");
        let cton = CString::new("rt/hidden_cloud").unwrap().into_raw();
        let ctyn = CString::new("std_msgs::msg::dds_::String_")
            .unwrap()
            .into_raw();
        let topic = cyclors::cdds_create_blob_topic(dp, cton, ctyn, true);
        assert!(topic > 0, "raw topic creation failed: {topic}");
        let reader = cyclors::dds_create_reader(dp, topic, std::ptr::null(), std::ptr::null());
        assert!(reader > 0, "raw reader creation failed: {reader}");
        drop(CString::from_raw(cton));
        drop(CString::from_raw(ctyn));

        let mut si = std::mem::MaybeUninit::<[cyclors::dds_sample_info_t; 1]>::uninit();
        while !stop_reader.load(Ordering::Relaxed) {
            loop {
                let mut zp: *mut cyclors::ddsi_serdata = std::ptr::null_mut();
                let n = cyclors::dds_takecdr(
                    reader,
                    &mut zp,
                    1,
                    si.as_mut_ptr() as *mut cyclors::dds_sample_info_t,
                    cyclors::DDS_ANY_STATE,
                );
                if n <= 0 {
                    break;
                }
                let info = si.assume_init();
                if info[0].valid_data {
                    received2.fetch_add(1, Ordering::Relaxed);
                }
                cyclors::ddsi_serdata_unref(zp);
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        cyclors::dds_delete(dp);
    });

    // Wait for the matched-reader activation to propagate end-to-end and data
    // to flow: raw reader matches bridge writer -> publication_matched ->
    // route announces -> robot activates -> data.
    let mut got = 0u64;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        got = received.load(Ordering::Relaxed);
        if got >= 20 {
            break;
        }
    }

    // Admin space: the route must list the synthetic matched-readers entry.
    let mut scfg = zenoh::Config::default();
    scfg.insert_json5("connect/endpoints", &format!("[\"{O_EP}\"]"))
        .unwrap();
    scfg.insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    let session = zenoh::open(scfg).await.unwrap();
    let routes = common::fetch_routes(&session, "topic/sub").await;
    let route = routes.get("hidden_cloud");
    println!("hidden_cloud route JSON: {route:?}");
    let synthetic = route
        .map(|r| common::route_serves_node(r, "<matched_dds_readers>"))
        .unwrap_or(false);

    println!("raw reader received={got} synthetic_local_node={synthetic}");
    stop.store(true, Ordering::Relaxed);

    assert!(
        got >= 20,
        "unattributed DDS reader received only {got} messages - \
         publication-matched activation not working (the hector-UI symptom)"
    );
    assert!(
        synthetic,
        "route does not list <matched_dds_readers> in local_nodes"
    );
}
