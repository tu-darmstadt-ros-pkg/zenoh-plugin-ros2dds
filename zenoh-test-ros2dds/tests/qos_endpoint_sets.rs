//
// A bridge's DDS endpoint QoS must follow the current set of endpoints it bridges.
//

pub mod common;

use std::time::Duration;

use common::{operator_bridge_cfg, robot_bridge_cfg, BridgeConfig, Endpoint};

/// Admin-space view of the operator bridge's subscriber route for `zenoh_ke`.
async fn sub_route(session: &zenoh::Session, zenoh_ke: &str) -> serde_json::Value {
    common::fetch_routes(session, "topic/sub")
        .await
        .remove(zenoh_ke)
        .unwrap_or(serde_json::Value::Null)
}

fn field(route: &serde_json::Value, name: &str) -> String {
    match route.get(name) {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
        None => String::new(),
    }
}

/// TRANSIENT_LOCAL history depth of a route's `writer_qos`.
fn tl_depth(route: &serde_json::Value) -> Option<i32> {
    field(route, "writer_qos")
        .split_once("tl_depth=")
        .and_then(|(_, d)| d.parse().ok())
}

async fn admin_session(port: u16) -> zenoh::Session {
    let mut cfg = zenoh::Config::default();
    cfg.insert_json5("mode", "\"client\"").unwrap();
    cfg.insert_json5("connect/endpoints", &format!("[\"tcp/127.0.0.1:{port}\"]"))
        .unwrap();
    cfg.insert_json5("scouting/multicast/enabled", "false").unwrap();
    zenoh::open(cfg).await.expect("admin session")
}

const SETTLE: Duration = Duration::from_secs(3);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lazy_publisher_sees_bridge_reader_after_demanding_remote_sub() {
    common::init_env();
    let (rd, od, port) = (214, 215, 7820);
    let robot = common::create_bridge_with(&robot_bridge_cfg(rd, port)).await;
    let operator = common::create_bridge_with(&operator_bridge_cfg(od, port)).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // RViz-like TL subscriber announced before the publisher exists
    let rviz = Endpoint::spawn("rviz", od, &["sub", "/lazy", "rel", "tl", "10"]).await;
    tokio::time::sleep(SETTLE).await;
    let lazy = Endpoint::spawn("lazy", rd, &["lazy_pub", "/lazy", "be", "vol", "5", "z"]).await;
    let witness = Endpoint::spawn("witness", od, &["sub", "/lazy", "be", "vol", "10"]).await;
    tokio::time::sleep(SETTLE + SETTLE).await;

    let (w, r, w_live) = (witness.raw_result(), rviz.raw_result(), witness.live());
    witness.stop().await;
    rviz.stop().await;
    lazy.stop().await;
    common::close_bridge(operator).await;
    common::close_bridge(robot).await;
    assert!(
        w_live > 0,
        "lazy BEST_EFFORT publisher never published through the bridge.\n  witness: {w}\n  rviz: {r}"
    );
}

async fn mixed_publishers_case(rd: u32, od: u32, port: u16, first: &str, second: &str) {
    let qos = |tag: &str| if tag == "rel" { "rel" } else { "be" };
    let robot = common::create_bridge_with(&robot_bridge_cfg(rd, port)).await;
    let operator = common::create_bridge_with(&operator_bridge_cfg(od, port)).await;
    let first_pub =
        Endpoint::spawn(first, rd, &["pub", "/mixed", qos(first), "vol", "10", first]).await;
    // the bridge offers RELIABLE, so both subscribers get both publishers
    let subs = [
        Endpoint::spawn("sub_be", od, &["sub", "/mixed", "be", "vol", "10"]).await,
        Endpoint::spawn("sub_rel", od, &["sub", "/mixed", "rel", "vol", "10"]).await,
    ];
    tokio::time::sleep(SETTLE).await;
    let first_before: Vec<usize> = subs.iter().map(|s| s.live_from(first)).collect();

    let second_pub =
        Endpoint::spawn(second, rd, &["pub", "/mixed", qos(second), "vol", "10", second]).await;
    tokio::time::sleep(SETTLE).await;
    let first_mid: Vec<usize> = subs.iter().map(|s| s.live_from(first)).collect();
    let second_mid: Vec<usize> = subs.iter().map(|s| s.live_from(second)).collect();


    second_pub.stop().await;
    tokio::time::sleep(SETTLE).await;
    let first_after: Vec<usize> = subs.iter().map(|s| s.live_from(first)).collect();
    let raws: Vec<String> = subs.iter().map(|s| format!("{}: {}", s.label, s.raw_result())).collect();

    for s in subs {
        s.stop().await;
    }
    first_pub.stop().await;
    common::close_bridge(operator).await;
    common::close_bridge(robot).await;

    for (i, raw) in raws.iter().enumerate() {
        let ctx = format!("first={first} second={second}, {raw}");
        assert!(first_before[i] > 0, "first publisher never forwarded ({ctx})");
        assert!(second_mid[i] > 0, "second publisher never forwarded ({ctx})");
        assert!(
            first_mid[i] > first_before[i],
            "first publisher stopped flowing when the second joined ({ctx})"
        );
        assert!(
            first_after[i] > first_mid[i],
            "first publisher stopped flowing when the second left ({ctx})"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_reliability_publishers_best_effort_joins_later() {
    common::init_env();
    mixed_publishers_case(216, 217, 7821, "rel", "be").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_reliability_publishers_reliable_joins_later() {
    common::init_env();
    mixed_publishers_case(218, 219, 7822, "be", "rel").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_publisher_bridges_with_different_durability() {
    common::init_env();
    let (robot_a_d, robot_b_d, od, port) = (220, 221, 222, 7823);
    let operator =
        common::create_bridge_with(&BridgeConfig::new(od).listen(format!("tcp/127.0.0.1:{port}")))
            .await;
    let robot_a = common::create_bridge_with(
        &BridgeConfig::new(robot_a_d).connect(format!("tcp/127.0.0.1:{port}")),
    )
    .await;
    let robot_b = common::create_bridge_with(
        &BridgeConfig::new(robot_b_d).connect(format!("tcp/127.0.0.1:{port}")),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // VOLATILE publisher's announcement arrives first
    let pub_a = Endpoint::spawn("a", robot_a_d, &["pub", "/multi", "rel", "vol", "10", "a"]).await;
    let sub_vol = Endpoint::spawn("sub_vol", od, &["sub", "/multi", "rel", "vol", "10"]).await;
    tokio::time::sleep(SETTLE).await;
    let pub_b = Endpoint::spawn("b", robot_b_d, &["pub", "/multi", "rel", "tl", "1000", "b"]).await;
    tokio::time::sleep(SETTLE).await;
    let sub_tl = Endpoint::spawn("sub_tl", od, &["sub", "/multi", "rel", "tl", "1000"]).await;
    tokio::time::sleep(SETTLE).await;
    // one TL Writer stands in for both publishers, so sub_tl also gets a's live data, a limit of one Writer per topic
    let (tl_raw, tl_hist, tl_b, tl_hist_a) = (
        sub_tl.raw_result(),
        sub_tl.history_from("b"),
        sub_tl.live_from("b"),
        sub_tl.history_from("a"),
    );

    // the TL publisher's bridge goes away, a must keep flowing
    pub_b.stop().await;
    common::close_bridge(robot_b).await;
    tokio::time::sleep(SETTLE).await;
    let a_mid = sub_vol.live_from("a");
    tokio::time::sleep(SETTLE).await;
    let a_after = sub_vol.live_from("a");
    let vol_raw = sub_vol.raw_result();

    sub_tl.stop().await;
    sub_vol.stop().await;
    pub_a.stop().await;
    common::close_bridge(robot_a).await;
    common::close_bridge(operator).await;

    // collect, so that a failure does not hide the later checks
    let mut failures = Vec::new();
    if tl_hist != 5 {
        failures.push(format!("late TL subscriber missed the TL publisher's history: {tl_raw}"));
    }
    if tl_b == 0 {
        failures.push(format!("late TL subscriber got no live data from b: {tl_raw}"));
    }
    if tl_hist_a != 0 {
        failures.push(format!("VOLATILE publisher a's messages arrived as history: {tl_raw}"));
    }
    if a_after <= a_mid {
        failures.push(format!("volatile publisher stopped flowing after the TL bridge left: {vol_raw}"));
    }
    assert!(failures.is_empty(), "\n  - {}", failures.join("\n  - "));
}

/// After the first TL subscriber leaves, a new TL subscriber still gets the TL publisher's history and data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscriber_departure_keeps_route_serving_remaining_qos() {
    common::init_env();
    let (rd, od, port) = (231, 232, 7824);
    let robot = common::create_bridge_with(&robot_bridge_cfg(rd, port)).await;
    let operator = common::create_bridge_with(&operator_bridge_cfg(od, port)).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let tl1 = Endpoint::spawn("tl1", od, &["sub", "/leave", "rel", "tl", "1000"]).await;
    tokio::time::sleep(SETTLE).await;
    let publ = Endpoint::spawn("pub", rd, &["pub", "/leave", "rel", "tl", "1000", "p"]).await;
    let vol = Endpoint::spawn("vol", od, &["sub", "/leave", "rel", "vol", "10"]).await;
    tokio::time::sleep(SETTLE).await;
    tl1.stop().await;
    tokio::time::sleep(SETTLE).await;
    let tl2 = Endpoint::spawn("tl2", od, &["sub", "/leave", "rel", "tl", "1000"]).await;
    tokio::time::sleep(SETTLE).await;
    let (h, l, raw, vol_raw, vol_live) =
        (tl2.history(), tl2.live(), tl2.raw_result(), vol.raw_result(), vol.live());

    tl2.stop().await;
    vol.stop().await;
    publ.stop().await;
    common::close_bridge(operator).await;
    common::close_bridge(robot).await;

    assert!(vol_live > 0, "volatile subscriber got no data: {vol_raw}");
    assert_eq!(h, 5, "TL subscriber joining after another left missed the history: {raw}");
    assert!(l > 0, "TL subscriber joining after another left got no live data: {raw}");
}

/// The subscriber-side Writer follows the remote publishers while subscribers stay connected.
/// Its GUID in the admin space shows each rebuild.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscriber_writer_rebuilds_follow_remote_publishers() {
    common::init_env();
    let (rd, od, port, admin_port) = (196, 197, 7748, 7749);
    let robot = common::create_bridge_with(&robot_bridge_cfg(rd, port)).await;
    let operator = common::create_bridge_with(
        &operator_bridge_cfg(od, port).listen(format!("tcp/127.0.0.1:{admin_port}")),
    )
    .await;
    let admin = admin_session(admin_port).await;
    let mut failures: Vec<String> = Vec::new();
    let mut check = |ok: bool, msg: String| {
        if !ok {
            failures.push(msg)
        }
    };

    let tl_sub = Endpoint::spawn("tl_sub", od, &["sub", "/rebuild", "rel", "tl", "1000"]).await;
    let vol_sub = Endpoint::spawn("vol_sub", od, &["sub", "/rebuild", "rel", "vol", "10"]).await;

    // 1. VOLATILE publisher: VOLATILE Writer, the TL subscriber can't match
    let pub_v = Endpoint::spawn("v", rd, &["pub", "/rebuild", "rel", "vol", "10", "v"]).await;
    tokio::time::sleep(SETTLE).await;
    let r = sub_route(&admin, "rebuild").await;
    let g1 = field(&r, "dds_writer");
    check(!g1.is_empty(), format!("no Writer for a VOLATILE publisher: {r}"));
    check(field(&r, "writer_qos").contains("VOLATILE"), format!("Writer not VOLATILE: {r}"));
    check(tl_sub.live() == 0, format!("TL sub matched a VOLATILE-only route: {}", tl_sub.raw_result()));

    // 2. publisher gone, a VOLATILE Writer is kept
    pub_v.stop().await;
    tokio::time::sleep(SETTLE).await;
    let r = sub_route(&admin, "rebuild").await;
    check(field(&r, "dds_writer") == g1, format!("Writer rebuilt with no publisher left: {r}"));

    // 3. TL publisher: upgrade while the TL subscriber waits
    let pub_t = Endpoint::spawn("t", rd, &["pub", "/rebuild", "rel", "tl", "100", "t"]).await;
    tokio::time::sleep(SETTLE + SETTLE).await;
    let r = sub_route(&admin, "rebuild").await;
    let g2 = field(&r, "dds_writer");
    check(g2 != g1 && !g2.is_empty(), format!("no rebuild VOLATILE -> TL: {r}"));
    check(field(&r, "writer_qos").contains("TRANSIENT_LOCAL"), format!("Writer not TL: {r}"));
    check(field(&r, "is_active") == "true", format!("route not active: {r}"));
    check(tl_sub.history_from("t") == 5, format!("waiting TL sub missed t's history: {}", tl_sub.raw_result()));
    check(tl_sub.live_from("t") > 0, format!("waiting TL sub got no data from t: {}", tl_sub.raw_result()));
    check(vol_sub.live_from("t") > 0, format!("VOLATILE sub got no data from t: {}", vol_sub.raw_result()));

    // 4. deeper TL publisher on the same robot: depth upgrade while matched
    let t_before = tl_sub.live_from("t");
    let pub_u = Endpoint::spawn("u", rd, &["pub", "/rebuild", "rel", "tl", "1000", "u"]).await;
    tokio::time::sleep(SETTLE + SETTLE).await;
    let r = sub_route(&admin, "rebuild").await;
    let g3 = field(&r, "dds_writer");
    check(g3 != g2, format!("no rebuild on deeper remote history: {r}"));
    check(tl_sub.history_from("u") == 5, format!("TL sub missed u's history: {}", tl_sub.raw_result()));
    check(tl_sub.live_from("t") > t_before, format!("t stopped flowing across the rebuild: {}", tl_sub.raw_result()));

    // 5. deeper publisher leaves: downgrade, t keeps flowing
    pub_u.stop().await;
    tokio::time::sleep(SETTLE).await;
    let t_mid = tl_sub.live_from("t");
    tokio::time::sleep(SETTLE).await;
    let r = sub_route(&admin, "rebuild").await;
    let g4 = field(&r, "dds_writer");
    check(g4 != g3, format!("no rebuild after the deeper publisher left: {r}"));
    check(tl_sub.live_from("t") > t_mid, format!("t stopped flowing after the downgrade: {}", tl_sub.raw_result()));
    // each rebuild on either bridge may replay the history once, 3 transitions x 2 bridges x 2 tags x 5
    let dups = tl_sub.history_dups();
    check(dups <= 60, format!("excessive history re-delivery: {}", tl_sub.raw_result()));
    tokio::time::sleep(SETTLE + SETTLE).await;
    check(
        tl_sub.history_dups() == dups,
        format!("history re-delivered while nothing changed (loop): {dups} -> {}", tl_sub.raw_result()),
    );

    // 6. a publisher with t's QoS joins, the shared history is (100 + 100) x multiplier 10
    check(tl_depth(&r) == Some(100), format!("Writer not sized for t alone: {r}"));
    let pub_t2 = Endpoint::spawn("t2", rd, &["pub", "/rebuild", "rel", "tl", "100", "t2"]).await;
    tokio::time::sleep(SETTLE + SETTLE).await;
    let r = sub_route(&admin, "rebuild").await;
    check(tl_depth(&r) == Some(2000), format!("Writer not sized for t and t2: {r}"));
    let late = Endpoint::spawn("late", od, &["sub", "/rebuild", "rel", "tl", "1000"]).await;
    tokio::time::sleep(SETTLE).await;
    // t's own history was evicted by its live messages long ago
    check(late.history_from("t2") == 5, format!("late TL sub missed t2's history: {}", late.raw_result()));
    check(late.live_from("t") > 0, format!("late TL sub got no data from t: {}", late.raw_result()));

    // 7. no publisher left, as in DDS the history leaves with its publishers
    pub_t.stop().await;
    pub_t2.stop().await;
    tokio::time::sleep(SETTLE).await;
    let r = sub_route(&admin, "rebuild").await;
    check(field(&r, "writer_qos").contains("VOLATILE"), format!("TL Writer kept without publisher: {r}"));

    for e in [late, vol_sub, tl_sub] {
        e.stop().await;
    }
    let _ = admin.close().await;
    common::close_bridge(operator).await;
    common::close_bridge(robot).await;
    assert!(failures.is_empty(), "\n  - {}", failures.join("\n  - "));
}

/// After a TL to VOLATILE Writer rebuild, the route retires once its last (now unmatched) TL subscriber leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn route_retires_after_downgrade_when_last_subscriber_leaves() {
    common::init_env();
    let (rd, od, port, admin_port) = (198, 199, 7750, 7751);
    let robot = common::create_bridge_with(&robot_bridge_cfg(rd, port)).await;
    let operator = common::create_bridge_with(
        &operator_bridge_cfg(od, port).listen(format!("tcp/127.0.0.1:{admin_port}")),
    )
    .await;
    let admin = admin_session(admin_port).await;

    let tl_sub = Endpoint::spawn("tl_sub", od, &["sub", "/downgrade", "rel", "tl", "1000"]).await;
    let pub_t = Endpoint::spawn("t", rd, &["pub", "/downgrade", "rel", "tl", "10", "t"]).await;
    tokio::time::sleep(SETTLE).await;
    let got_t = tl_sub.live_from("t");
    pub_t.stop().await;
    let pub_v = Endpoint::spawn("v", rd, &["pub", "/downgrade", "rel", "vol", "10", "v"]).await;
    tokio::time::sleep(SETTLE + SETTLE).await;
    let got_v = tl_sub.live_from("v");
    let sub_raw = tl_sub.raw_result();
    tl_sub.stop().await;
    tokio::time::sleep(SETTLE + SETTLE).await;
    let r = sub_route(&admin, "downgrade").await;

    pub_v.stop().await;
    let _ = admin.close().await;
    common::close_bridge(operator).await;
    common::close_bridge(robot).await;

    assert!(got_t > 0, "TL subscriber got nothing from the TL publisher: {sub_raw}");
    assert_eq!(got_v, 0, "TL subscriber matched a VOLATILE-only route: {sub_raw}");
    assert!(
        field(&r, "is_active") != "true" && !common::route_serves_node(&r, "<matched_dds_readers>"),
        "route still active / serving matched readers after its last subscriber left: {r}"
    );
}

/// Data from a plain zenoh publisher (which announces nothing) keeps flowing
/// after a bridge publisher of the same topic leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_zenoh_publisher_survives_bridge_publisher_leaving() {
    common::init_env();
    let (rd, od, port, client_port) = (212, 213, 7752, 7753);
    let robot = common::create_bridge_with(&robot_bridge_cfg(rd, port)).await;
    let operator = common::create_bridge_with(
        &operator_bridge_cfg(od, port).listen(format!("tcp/127.0.0.1:{client_port}")),
    )
    .await;
    let sub = Endpoint::spawn("sub", od, &["sub", "/plain", "rel", "vol", "10"]).await;
    let pub_b = Endpoint::spawn("b", rd, &["pub", "/plain", "rel", "tl", "100", "b"]).await;

    let zenoh_pub = admin_session(client_port).await;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let publisher = {
        let stop = stop.clone();
        let zenoh_pub = zenoh_pub.clone();
        tokio::spawn(async move {
            let mut i = 0;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let msg = format!("z:l{i}");
                let buf = cdr::serialize::<_, _, cdr::CdrLe>(&msg, cdr::size::Infinite).unwrap();
                zenoh_pub.put("plain", buf).await.unwrap();
                i += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };
    tokio::time::sleep(SETTLE + SETTLE).await;
    let (z_before, b_before) = (sub.live_from("z"), sub.live_from("b"));
    pub_b.stop().await;
    tokio::time::sleep(SETTLE).await;
    let z_mid = sub.live_from("z");
    tokio::time::sleep(SETTLE).await;
    let (z_after, raw) = (sub.live_from("z"), sub.raw_result());

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = publisher.await;
    sub.stop().await;
    let _ = zenoh_pub.close().await;
    common::close_bridge(operator).await;
    common::close_bridge(robot).await;

    assert!(z_before > 0 && b_before > 0, "no data from both publishers: {raw}");
    assert!(z_after > z_mid, "plain zenoh data stopped after the bridge publisher left: {raw}");
}

/// After an ungraceful loss of the only TL publisher's bridge, the subscriber-side Writer follows the remaining VOLATILE publisher.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_follows_remaining_publishers_after_bridge_loss() {
    common::init_env();
    let (od, robot_a_d, robot_b_d, port, proxy_port) = (40, 41, 42, 7760, 7761);
    let operator =
        common::create_bridge_with(&BridgeConfig::new(od).listen(format!("tcp/127.0.0.1:{port}")))
            .await;
    let robot_a = common::create_bridge_with(
        &BridgeConfig::new(robot_a_d).connect(format!("tcp/127.0.0.1:{port}")),
    )
    .await;
    let proxy = common::FreezableProxy::start(proxy_port, port).await;
    let robot_b = common::create_bridge_with(
        &BridgeConfig::new(robot_b_d).connect(format!("tcp/127.0.0.1:{proxy_port}")),
    )
    .await;
    let admin = admin_session(port).await;

    let sub = Endpoint::spawn("sub", od, &["sub", "/lost", "rel", "vol", "10"]).await;
    let pub_a = Endpoint::spawn("a", robot_a_d, &["pub", "/lost", "rel", "vol", "10", "a"]).await;
    let pub_b = Endpoint::spawn("b", robot_b_d, &["pub", "/lost", "rel", "tl", "10", "b"]).await;
    tokio::time::sleep(SETTLE + SETTLE).await;
    let before = sub_route(&admin, "lost").await;

    proxy.freeze();
    // lease expiry plus one retry tick
    let deadline = std::time::Instant::now() + Duration::from_secs(40);
    let mut after = sub_route(&admin, "lost").await;
    while !field(&after, "writer_qos").contains("VOLATILE") && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(1)).await;
        after = sub_route(&admin, "lost").await;
    }
    let a_mid = sub.live_from("a");
    tokio::time::sleep(SETTLE).await;
    let (a_after, raw) = (sub.live_from("a"), sub.raw_result());

    for e in [sub, pub_a, pub_b] {
        e.stop().await;
    }
    let _ = admin.close().await;
    common::close_bridge(robot_b).await;
    common::close_bridge(robot_a).await;
    common::close_bridge(operator).await;

    assert!(field(&before, "writer_qos").contains("TRANSIENT_LOCAL"), "Writer not TL with a TL publisher: {before}");
    assert!(field(&after, "writer_qos").contains("VOLATILE"), "Writer still TL after its TL publisher's bridge was lost: {after}");
    assert!(a_after > a_mid, "a stopped flowing after the other bridge was lost: {raw}");
}

/// A node's second TL Writer on a topic counts for the bridged history, joining and leaving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_writer_of_a_node_sizes_the_history() {
    common::init_env();
    let (rd, od, port, admin_port) = (50, 51, 7825, 7826);
    let robot = common::create_bridge_with(&robot_bridge_cfg(rd, port)).await;
    let operator = common::create_bridge_with(
        &operator_bridge_cfg(od, port).listen(format!("tcp/127.0.0.1:{admin_port}")),
    )
    .await;
    let admin = admin_session(admin_port).await;

    let sub = Endpoint::spawn("sub", od, &["sub", "/pair", "rel", "tl", "100"]).await;
    let mut pair = Endpoint::spawn("pair", rd, &["pub_pair", "/pair", "rel", "tl", "3"]).await;
    tokio::time::sleep(SETTLE + SETTLE).await;
    let first = sub_route(&admin, "pair").await;
    // the node is already known, so only the DDS Writer discovery reports the second Writer
    pair.send("ADD").await;
    tokio::time::sleep(SETTLE + SETTLE).await;
    let both = sub_route(&admin, "pair").await;
    pair.send("DROP").await;
    tokio::time::sleep(SETTLE + SETTLE).await;
    let one = sub_route(&admin, "pair").await;

    pair.stop().await;
    sub.stop().await;
    let _ = admin.close().await;
    common::close_bridge(operator).await;
    common::close_bridge(robot).await;

    // a single Writer keeps its own depth, (3 + 3) x multiplier 10 for two
    assert_eq!(tl_depth(&first), Some(3), "Writer not sized for one Writer: {first}");
    assert_eq!(tl_depth(&both), Some(60), "Writer not sized for both Writers: {both}");
    assert_eq!(tl_depth(&one), Some(3), "Writer not resized after one Writer left: {one}");
}
