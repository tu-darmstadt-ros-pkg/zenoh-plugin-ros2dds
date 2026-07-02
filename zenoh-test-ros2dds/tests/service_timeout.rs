//
// Service round-trip vs. the bridge's default queries_timeout (5 s).
//
// CLAIM UNDER TEST (audit finding B3; matches the production symptom "the
// request visibly executes but the response never arrives"):
//   The client-side bridge's RouteServiceCli Querier uses queries_timeout
//   (default 5.0 s, config.rs DEFAULT_QUERIES_TIMEOUT). Any service slower
//   than that executes on the server side, but the zenoh query is finalized
//   before the reply returns, so the ROS client never receives a response.
//   (Actions' get_result was special-cased to 300 s upstream for exactly this
//   reason; plain services were not.)
//
// Topology: caller (domain 11, separate process) -> bridge O (domain 11)
//           -> tcp -> bridge R (domain 10) -> server (domain 10, in-process).
//
// Two probes:
//   fast_add  (replies immediately) — sanity: MUST work, else the pipeline is
//              broken and the slow verdict would be meaningless.
//   slow_add  (replies after 8 s)   — DESIRED behavior: response arrives
//              (caller prints CALLER_GOT_RESPONSE). RED verifies the claim:
//              server's request counter is 1 but caller prints CALLER_TIMEOUT.

pub mod common;

use std::{
    process::Stdio,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use common::BridgeConfig;
use futures::StreamExt;
use r2r::QosProfile;

const R_EP: &str = "tcp/127.0.0.1:7481";
const SLOW_SERVICE_DELAY: Duration = Duration::from_secs(8);

async fn run_caller(service: &str) -> String {
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_service_caller"))
        .env("ROS_DOMAIN_ID", "11")
        .env("SERVICE_NAME", service)
        .env("CALL_TIMEOUT_S", "25")
        .env("WAIT_AVAILABLE_S", "30")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .output();
    let out = tokio::time::timeout(Duration::from_secs(70), out)
        .await
        .expect("caller process timed out")
        .expect("caller process failed to run");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_service_response_survives_the_bridge() {
    common::init_env();
    std::env::set_var("ROS_DOMAIN_ID", "10"); // server side (this process)

    // Robot-side bridge (listens) and operator-side bridge (connects).
    let _r = common::create_bridge_with(&BridgeConfig::new(10).listen(R_EP)).await;
    let _o = common::create_bridge_with(&BridgeConfig::new(11).connect(R_EP)).await;

    // In-process ROS server hosting fast_add + slow_add on domain 10.
    let fast_count = Arc::new(AtomicUsize::new(0));
    let slow_count = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let (fast_count2, slow_count2, stop2) = (fast_count.clone(), slow_count.clone(), stop.clone());

    std::thread::spawn(move || {
        let ctx = r2r::Context::create().unwrap();
        let mut node = r2r::Node::create(ctx, "slow_server", "").unwrap();
        let mut fast = node
            .create_service::<r2r::example_interfaces::srv::AddTwoInts::Service>(
                "/fast_add",
                QosProfile::services_default(),
            )
            .unwrap();
        let mut slow = node
            .create_service::<r2r::example_interfaces::srv::AddTwoInts::Service>(
                "/slow_add",
                QosProfile::services_default(),
            )
            .unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let fc = fast_count2.clone();
            tokio::spawn(async move {
                while let Some(req) = fast.next().await {
                    fc.fetch_add(1, Ordering::SeqCst);
                    let sum = req.message.a + req.message.b;
                    let _ = req.respond(r2r::example_interfaces::srv::AddTwoInts::Response { sum });
                }
            });
            let sc = slow_count2.clone();
            tokio::spawn(async move {
                while let Some(req) = slow.next().await {
                    sc.fetch_add(1, Ordering::SeqCst);
                    // The service "executes" (visible side effect in prod),
                    // then takes longer than the bridge's 5 s query timeout.
                    tokio::time::sleep(SLOW_SERVICE_DELAY).await;
                    let sum = req.message.a + req.message.b;
                    let _ = req.respond(r2r::example_interfaces::srv::AddTwoInts::Response { sum });
                }
            });
            while !stop2.load(Ordering::Relaxed) {
                node.spin_once(Duration::from_millis(10));
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
    });

    // Let bridges interconnect and the service routes announce.
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Sanity: fast service must round-trip, otherwise the harness is broken.
    let fast_verdict = run_caller("/fast_add").await;
    println!(
        "fast_add: caller said: {fast_verdict}; server executions={}",
        fast_count.load(Ordering::SeqCst)
    );
    assert!(
        fast_verdict.contains("CALLER_GOT_RESPONSE"),
        "HARNESS BROKEN (not the claim): fast service did not round-trip: {fast_verdict}"
    );

    // The probe: slow service.
    let slow_verdict = run_caller("/slow_add").await;
    let executed = slow_count.load(Ordering::SeqCst);
    println!("slow_add: caller said: {slow_verdict}; server executions={executed}");

    stop.store(true, Ordering::Relaxed);

    assert_eq!(
        executed, 1,
        "server never received the slow request (different problem)"
    );
    assert!(
        slow_verdict.contains("CALLER_GOT_RESPONSE"),
        "CLAIM VERIFIED (test red): request EXECUTED on the server (count={executed}) \
         but the response never reached the caller ({slow_verdict}) — the bridge's \
         default 5 s queries_timeout drops responses of slow services"
    );
}
