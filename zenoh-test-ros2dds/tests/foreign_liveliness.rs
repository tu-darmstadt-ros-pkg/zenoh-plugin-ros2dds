//
// A foreign liveliness token under the bridge prefix must not kill the plugin task.
// "éé" is 4 bytes, so a byte slice of its first 3 bytes ends inside a character.

pub mod common;

use std::time::Duration;

use common::BridgeConfig;

const DOMAIN: u32 = 31;
const PORT: u16 = 7902;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_ascii_liveliness_token_is_ignored() {
    common::init_env();
    let bridge = common::create_bridge_with(
        &BridgeConfig::new(DOMAIN).listen(format!("tcp/127.0.0.1:{PORT}")),
    )
    .await;

    let mut cfg = zenoh::Config::default();
    cfg.insert_json5("connect/endpoints", &format!("[\"tcp/127.0.0.1:{PORT}\"]"))
        .unwrap();
    cfg.insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    let peer = zenoh::open(cfg).await.unwrap();
    let _token = peer
        .liveliness()
        .declare_token(format!("@/{}/@ros2_lv/éé", peer.zid()))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    let local = zenoh::session::init(bridge.clone().into()).await.unwrap();
    let replies = local
        .get(format!("@/{}/ros2/version", local.zid()))
        .timeout(Duration::from_secs(2))
        .await
        .unwrap();
    let alive = matches!(replies.recv_async().await, Ok(r) if r.result().is_ok());
    let _ = local.close().await;
    let _ = peer.close().await;
    common::close_bridge(bridge).await;

    assert!(alive, "the plugin stopped answering after a non-ASCII liveliness token");
}
