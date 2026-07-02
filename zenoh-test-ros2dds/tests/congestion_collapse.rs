//
// Congestion collapse on a constrained link (the production killer).
//
// CLAIMS UNDER TEST (audit chain, observed live in production logs):
//   1. With the default `reliable_routes_blocking: true`, RELIABLE ROS topics
//      publish as non-droppable (CongestionControl::Block). When the link
//      cannot drain for longer than zenoh's `wait_before_close` (default 5 s),
//      zenoh CLOSES THE WHOLE TRANSPORT:
//      "Unable to push non droppable network message ... Closing transport!"
//      -> every topic dies at once, reconnect storm, repeat.
//   2. While a reliable route blocks, best-effort topics sharing the single
//      default Data priority queue are zero-rated (canary starves), and all
//      DDS->Zenoh forwarding serializes behind Cyclone's one delivery thread.
//   3. A/B: `reliable_routes_blocking: false` prevents the transport close and
//      keeps (some) data flowing on the same constrained link.
//
// Topology:  ROS pubs (domain 0, in-process)
//            -> bridge (connects to throttled TCP proxy)
//            -> proxy (250 KB/s robot->operator)
//            -> test zenoh session (listens; subscribes blob/tf/canary)
//
// Phase A asserts the REPRODUCTION (defaults must close the transport on a
// stalled link - pins the claim). Phase B asserts the MITIGATION holds
// (reliable_routes_blocking=false must survive the same link). Both green =
// the A/B claim stays verified.
//
// Run in RELEASE (zenoh-core `bug!` macros panic in debug builds during
// transport churn):
//   cargo test --release --test congestion_collapse -- --ignored --nocapture

pub mod common;

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use common::{BridgeConfig, LogBuf};
use r2r::QosProfile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SESSION_EP: &str = "tcp/127.0.0.1:7491";
const PROXY_A: &str = "tcp/127.0.0.1:7492";
const PROXY_B: &str = "tcp/127.0.0.1:7494";
const THROTTLE_BYTES_PER_SEC: f64 = 250_000.0;
// Degraded-WiFi model: the link alternates between a slow-but-alive phase and
// a COMPLETE stall. The stall (7 s) is longer than zenoh's non-droppable
// wait_before_close (5 s) but shorter than the transport lease (10 s), so a
// close in phase A is attributable to the congestion-control policy, not to
// lease expiry.
const PASS_SECS: u64 = 8;
const STALL_SECS: u64 = 7;
const MEASURE: Duration = Duration::from_secs(75);
const WARMUP: Duration = Duration::from_secs(10);

// ---------- throttled TCP proxy ----------

/// Forward listen_addr -> target_addr. The robot->operator direction is
/// throttled AND periodically stalled completely (PASS_SECS at bytes_per_sec,
/// then STALL_SECS of nothing) — the degraded-WiFi pattern. The reverse
/// direction is untouched.
async fn run_throttle_proxy(listen: &str, target: &str, bytes_per_sec: f64) {
    let listener = tokio::net::TcpListener::bind(listen).await.unwrap();
    loop {
        let Ok((inbound, _)) = listener.accept().await else {
            break;
        };
        let target = target.to_string();
        tokio::spawn(async move {
            let Ok(outbound) = tokio::net::TcpStream::connect(&target).await else {
                return;
            };
            inbound.set_nodelay(true).ok();
            outbound.set_nodelay(true).ok();
            let (mut ri, mut wi) = inbound.into_split();
            let (mut ro, mut wo) = outbound.into_split();
            // robot -> operator: throttled with periodic full stalls
            let up = tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                let mut phase_start = tokio::time::Instant::now();
                loop {
                    if phase_start.elapsed() >= Duration::from_secs(PASS_SECS) {
                        // Full stall: stop reading entirely (TCP backpressure
                        // propagates to the bridge within its socket buffer).
                        tokio::time::sleep(Duration::from_secs(STALL_SECS)).await;
                        phase_start = tokio::time::Instant::now();
                    }
                    match ri.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if wo.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                            let delay = n as f64 / bytes_per_sec;
                            tokio::time::sleep(Duration::from_secs_f64(delay)).await;
                        }
                    }
                }
            });
            // operator -> robot: unthrottled
            let down = tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                loop {
                    match ro.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if wi.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });
            let _ = tokio::join!(up, down);
        });
    }
}

// ---------- ROS data generators ----------

fn spawn_generators(stop: Arc<AtomicBool>) {
    std::thread::spawn(move || {
        let ctx = r2r::Context::create().unwrap();
        let mut node = r2r::Node::create(ctx, "congestion_pubs", "").unwrap();

        let blob_pub = node
            .create_publisher::<r2r::std_msgs::msg::String>("/blob", QosProfile::default())
            .unwrap();
        let tf_pub = node
            .create_publisher::<r2r::std_msgs::msg::String>("/tf_like", QosProfile::default())
            .unwrap();
        let canary_pub = node
            .create_publisher::<r2r::std_msgs::msg::String>("/canary", QosProfile::sensor_data())
            .unwrap();

        let blob = r2r::std_msgs::msg::String {
            data: "x".repeat(512 * 1024), // 512 KiB, RELIABLE, 20 Hz => ~10 MB/s
        };
        let small = r2r::std_msgs::msg::String {
            data: "tick".to_string(),
        };

        let mut i: u64 = 0;
        while !stop.load(Ordering::Relaxed) {
            // 100 Hz base tick
            if i % 5 == 0 {
                let _ = blob_pub.publish(&blob); // 20 Hz
                let _ = canary_pub.publish(&small); // 20 Hz
            }
            let _ = tf_pub.publish(&small); // 100 Hz
            node.spin_once(Duration::from_millis(0));
            std::thread::sleep(Duration::from_millis(10));
            i += 1;
        }
    });
}

// ---------- one measured phase ----------

struct PhaseOutcome {
    transport_close: bool,
    close_evidence: Vec<String>,
    canary_last30: u64,
    tf_last30: u64,
    canary_total: u64,
    tf_total: u64,
    blob_total: u64,
}

async fn measure_phase(
    logbuf: &LogBuf,
    canary: &AtomicU64,
    tf: &AtomicU64,
    blob: &AtomicU64,
) -> PhaseOutcome {
    tokio::time::sleep(WARMUP).await;
    let (c0, t0, b0) = (
        canary.load(Ordering::Relaxed),
        tf.load(Ordering::Relaxed),
        blob.load(Ordering::Relaxed),
    );
    tokio::time::sleep(MEASURE - Duration::from_secs(30)).await;
    let (c30, t30) = (canary.load(Ordering::Relaxed), tf.load(Ordering::Relaxed));
    tokio::time::sleep(Duration::from_secs(30)).await;

    let logs = logbuf.contents();
    let close_evidence: Vec<String> = logs
        .lines()
        .filter(|l| {
            l.contains("Unable to push non droppable")
                || l.contains("Closing transport")
                || l.contains("Remote zenoh transport lost")
                || l.contains("closed transport")
        })
        .map(|s| s.to_string())
        .collect();

    PhaseOutcome {
        transport_close: !close_evidence.is_empty(),
        close_evidence,
        canary_last30: canary.load(Ordering::Relaxed) - c30,
        tf_last30: tf.load(Ordering::Relaxed) - t30,
        canary_total: canary.load(Ordering::Relaxed) - c0,
        tf_total: tf.load(Ordering::Relaxed) - t0,
        blob_total: blob.load(Ordering::Relaxed) - b0,
    }
}

fn report(name: &str, o: &PhaseOutcome) {
    println!("=== {name} ===");
    println!(
        "  transport_close={} canary(last30s)={} tf(last30s)={} totals: canary={} tf={} blob={}",
        o.transport_close, o.canary_last30, o.tf_last30, o.canary_total, o.tf_total, o.blob_total
    );
    for e in o.close_evidence.iter().take(5) {
        println!("  evidence: {e}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore] // heavy (~3 min) — run explicitly, in RELEASE
async fn congestion_collapse_and_mitigation() {
    common::init_env();
    std::env::set_var("ROS_DOMAIN_ID", "0");

    // Capture all tracing output (bridge + zenoh core run in-process).
    let logbuf = LogBuf::install();

    // Operator stand-in: plain zenoh session, listening; counts receptions.
    let mut scfg = zenoh::Config::default();
    scfg.insert_json5("listen/endpoints", &format!("[\"{SESSION_EP}\"]"))
        .unwrap();
    scfg.insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    let session = zenoh::open(scfg).await.unwrap();

    let canary_count = Arc::new(AtomicU64::new(0));
    let tf_count = Arc::new(AtomicU64::new(0));
    let blob_count = Arc::new(AtomicU64::new(0));
    for (topic, counter) in [
        ("canary", canary_count.clone()),
        ("tf_like", tf_count.clone()),
        ("blob", blob_count.clone()),
    ] {
        session
            .declare_subscriber(topic)
            .callback(move |_s| {
                counter.fetch_add(1, Ordering::Relaxed);
            })
            .background()
            .await
            .unwrap();
    }

    // Data generators (both phases share them).
    let stop = Arc::new(AtomicBool::new(false));
    spawn_generators(stop.clone());

    // ---------------- Phase A: production defaults ----------------
    tokio::spawn(async {
        run_throttle_proxy("127.0.0.1:7492", "127.0.0.1:7491", THROTTLE_BYTES_PER_SEC).await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let bridge_a = common::create_bridge_with(&BridgeConfig::new(0).connect(PROXY_A)).await;
    let outcome_a = measure_phase(&logbuf, &canary_count, &tf_count, &blob_count).await;
    report(
        "Phase A (reliable_routes_blocking=true, throttled link)",
        &outcome_a,
    );

    // Tear down phase A. close() can hang if the bridge is wedged in a
    // blocking put — that hang is itself finding-consistent; fall back to
    // leaking the runtime.
    let closed =
        tokio::time::timeout(Duration::from_secs(10), common::close_bridge(bridge_a)).await;
    if closed.is_err() {
        println!("NOTE: phase-A bridge close() HUNG >10s (wedge-consistent); leaking it");
    }
    tokio::time::sleep(Duration::from_secs(8)).await;
    logbuf.clear();

    // ---------------- Phase B: mitigation ----------------
    tokio::spawn(async {
        run_throttle_proxy("127.0.0.1:7494", "127.0.0.1:7491", THROTTLE_BYTES_PER_SEC).await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let bridge_b = common::create_bridge_with(
        &BridgeConfig::new(0)
            .connect(PROXY_B)
            .plugin_opt("reliable_routes_blocking", "false"),
    )
    .await;
    let outcome_b = measure_phase(&logbuf, &canary_count, &tf_count, &blob_count).await;
    report(
        "Phase B (reliable_routes_blocking=false, same link)",
        &outcome_b,
    );

    stop.store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(Duration::from_secs(10), common::close_bridge(bridge_b)).await;

    // Phase B first (mitigation must hold for the A/B claim to be actionable).
    assert!(
        !outcome_b.transport_close && outcome_b.canary_last30 > 0 && outcome_b.tf_last30 > 0,
        "MITIGATION INSUFFICIENT: reliable_routes_blocking=false still closed the \
         transport or starved traffic: close={} canary={} tf={}",
        outcome_b.transport_close,
        outcome_b.canary_last30,
        outcome_b.tf_last30
    );

    // Phase A pins the KNOWN default behavior: with reliable_routes_blocking
    // = true, a stall longer than wait_before_close MUST close the transport
    // (that is zenoh policy, not a plugin bug - the plugin-side defense is the
    // config used in phase B). If this stops reproducing, either zenoh changed
    // its congestion policy or the harness lost its teeth - investigate both.
    assert!(
        outcome_a.transport_close,
        "phase A no longer reproduces the transport close - zenoh policy or \
         harness changed, re-validate the A/B claim"
    );
}
