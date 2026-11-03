//
// Copyright (c) 2024 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

//! Shared helpers for the zenoh-bridge-ros2dds test suite.
//!
//! `create_bridge()` is the historical single-bridge helper used by the
//! upstream-style tests (default config, multicast scouting on).
//!
//! `BridgeConfig`/`create_bridge_with()`/`close_bridge()` support the
//! multi-bridge topologies (churn/chaos/repro/congestion tests): every bridge
//! gets an explicit DDS domain and explicit listen/connect endpoints, with
//! multicast scouting AND gossip disabled so the topology is EXACTLY the
//! declared edges and nothing else (operators must not discover each other).

use std::time;

use zenoh::{
    config::Config,
    internal::{
        plugins::PluginsManager,
        runtime::{Runtime, RuntimeBuilder},
    },
};
use zenoh_config::ModeDependentValue;

pub static DEFAULT_TIMEOUT: time::Duration = time::Duration::from_secs(60);

pub fn init_env() {
    std::env::set_var("RMW_IMPLEMENTATION", "rmw_cyclonedds_cpp");
    base_cyclonedds_uri();
}

/// CYCLONEDDS_URI before any in-process bridge prepended its discovery fragment to it.
/// The plugin mutates this process-global variable, so bridges start one at a time from this value.
pub fn base_cyclonedds_uri() -> &'static str {
    static BASE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    BASE.get_or_init(|| std::env::var("CYCLONEDDS_URI").unwrap_or_default())
}

static BRIDGE_START: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Historical helper: single in-process bridge, default config (DDS domain 0,
/// multicast scouting enabled so a default `zenoh::open` session finds it).
pub async fn create_bridge() {
    let mut plugins_mgr = PluginsManager::static_plugins_only();
    plugins_mgr.declare_static_plugin::<zenoh_plugin_ros2dds::ROS2Plugin, &str>("ros2dds", true);
    let mut config = Config::default();
    config.insert_json5("plugins/ros2dds", "{}").unwrap();
    config
        .timestamping
        .set_enabled(Some(ModeDependentValue::Unique(true)))
        .unwrap();
    config.adminspace.set_enabled(true).unwrap();
    config.plugins_loading.set_enabled(true).unwrap();
    let mut runtime = RuntimeBuilder::new(config)
        .plugins_manager(plugins_mgr)
        .build()
        .await
        .unwrap();
    runtime.start().await.unwrap();
}

/// Declarative config for one in-process bridge instance.
#[derive(Default, Clone)]
pub struct BridgeConfig {
    domain: u32,
    listen: Vec<String>,
    connect: Vec<String>,
    /// Extra `key: json` entries merged into the `plugins/ros2dds` object,
    /// e.g. `("reliable_routes_blocking", "false")`.
    plugin_opts: Vec<(String, String)>,
}

impl BridgeConfig {
    pub fn new(domain: u32) -> Self {
        BridgeConfig {
            domain,
            ..Default::default()
        }
    }

    pub fn listen(mut self, endpoint: impl Into<String>) -> Self {
        self.listen.push(endpoint.into());
        self
    }

    pub fn connect(mut self, endpoint: impl Into<String>) -> Self {
        self.connect.push(endpoint.into());
        self
    }

    /// Add a plugin config entry, e.g. `.plugin_opt("reliable_routes_blocking", "false")`.
    pub fn plugin_opt(mut self, key: impl Into<String>, json_value: impl Into<String>) -> Self {
        self.plugin_opts.push((key.into(), json_value.into()));
        self
    }
}

/// Create and start a bridge from `cfg`. The returned `Runtime` keeps the bridge alive.
/// Pass it to [`close_bridge`], dropping it without close leaks sockets.
pub async fn create_bridge_with(cfg: &BridgeConfig) -> Runtime {
    // see base_cyclonedds_uri
    let _start_guard = BRIDGE_START.lock().await;
    std::env::set_var("CYCLONEDDS_URI", base_cyclonedds_uri());
    let mut plugins_mgr = PluginsManager::static_plugins_only();
    plugins_mgr.declare_static_plugin::<zenoh_plugin_ros2dds::ROS2Plugin, &str>("ros2dds", true);

    let mut plugin_json = format!("{{ domain: {}", cfg.domain);
    for (k, v) in &cfg.plugin_opts {
        plugin_json.push_str(&format!(", {k}: {v}"));
    }
    plugin_json.push_str(" }");

    let mut config = Config::default();
    config
        .insert_json5("plugins/ros2dds", &plugin_json)
        .unwrap();

    // Topology is exactly the declared edges: no multicast scouting, no gossip.
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    config
        .insert_json5("scouting/gossip/enabled", "false")
        .unwrap();

    if !cfg.listen.is_empty() {
        let eps = serde_json::to_string(&cfg.listen).unwrap();
        config.insert_json5("listen/endpoints", &eps).unwrap();
    }
    if !cfg.connect.is_empty() {
        let eps = serde_json::to_string(&cfg.connect).unwrap();
        config.insert_json5("connect/endpoints", &eps).unwrap();
    }

    config
        .timestamping
        .set_enabled(Some(ModeDependentValue::Unique(true)))
        .unwrap();
    config.adminspace.set_enabled(true).unwrap();
    config.plugins_loading.set_enabled(true).unwrap();

    let mut runtime = RuntimeBuilder::new(config)
        .plugins_manager(plugins_mgr)
        .build()
        .await
        .unwrap();
    runtime.start().await.unwrap();
    wait_for_plugin_participant(&runtime, cfg.domain).await;
    runtime
}

/// Waits for the plugin admin space, declared after the DDS participant consumed CYCLONEDDS_URI.
async fn wait_for_plugin_participant(runtime: &Runtime, domain: u32) {
    let session = zenoh::session::init(runtime.clone().into())
        .await
        .expect("open session on bridge runtime");
    let selector = format!("@/{}/ros2/version", session.zid());
    let deadline = time::Instant::now() + time::Duration::from_secs(30);
    loop {
        let replies = session
            .get(&selector)
            .timeout(time::Duration::from_millis(500))
            .await
            .expect("admin space query");
        if let Ok(reply) = replies.recv_async().await {
            if reply.result().is_ok() {
                break;
            }
        }
        assert!(
            time::Instant::now() < deadline,
            "bridge on DDS domain {domain} never started its ros2dds plugin"
        );
        tokio::time::sleep(time::Duration::from_millis(50)).await;
    }
    let _ = session.close().await;
}

/// Cleanly close a bridge runtime (releases its sockets and DDS entities).
pub async fn close_bridge(runtime: Runtime) {
    if let Err(e) = runtime.close().await {
        eprintln!("close_bridge: error closing runtime: {e}");
    }
}

/// Fetch all routes of one kind ("topic/sub", "topic/pub", "service/srv", ...)
/// from a bridge's admin space, keyed by the route's zenoh key expression
/// (i.e. the part after `route/<kind>/`), value = the route's JSON
/// serialization (includes `local_nodes` and `remote_routes`).
pub async fn fetch_routes(
    session: &zenoh::Session,
    kind: &str,
) -> std::collections::HashMap<String, serde_json::Value> {
    let mut out = std::collections::HashMap::new();
    let selector = format!("@/*/ros2/route/{kind}/**");
    let replies = session
        .get(&selector)
        .timeout(time::Duration::from_secs(10))
        .await
        .expect("admin space query failed");
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let key = sample.key_expr().to_string();
            let marker = format!("route/{kind}/");
            if let Some(pos) = key.find(&marker) {
                let suffix = key[pos + marker.len()..].to_string();
                let value: serde_json::Value = serde_json::from_slice(&sample.payload().to_bytes())
                    .unwrap_or(serde_json::Value::Null);
                out.insert(suffix, value);
            }
        }
    }
    out
}

/// True when the route JSON lists `node_fullname` among its `local_nodes`.
pub fn route_serves_node(route: &serde_json::Value, node_fullname: &str) -> bool {
    route
        .get("local_nodes")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().any(|n| n.as_str() == Some(node_fullname)))
        .unwrap_or(false)
}

/// In-memory sink for the process's tracing output (bridge + zenoh core run
/// in-process, so their logs are capturable). Install once per process via
/// [`LogBuf::install`]; grep with [`LogBuf::lines_containing`].
#[derive(Clone)]
pub struct LogBuf(pub std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LogBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
    type Writer = LogBuf;
    fn make_writer(&'a self) -> LogBuf {
        self.clone()
    }
}

impl LogBuf {
    /// Create the buffer and install it as the process's tracing subscriber
    /// (INFO level and up). Must run before the bridge starts.
    pub fn install() -> LogBuf {
        let buf = LogBuf(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())));
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
            .with_writer(buf.clone())
            .with_ansi(false)
            .try_init()
            .ok();
        buf
    }

    pub fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).to_string()
    }

    pub fn clear(&self) {
        self.0.lock().unwrap().clear();
    }

    pub fn lines_containing(&self, needle: &str) -> Vec<String> {
        self.contents()
            .lines()
            .filter(|l| l.contains(needle))
            .map(|s| s.to_string())
            .collect()
    }
}

/// A `qos_endpoint` child process on its own DDS domain, killed on drop.
pub struct Endpoint {
    pub label: String,
    child: tokio::process::Child,
    stdin: Option<tokio::process::ChildStdin>,
    latest: std::sync::Arc<std::sync::Mutex<String>>,
    exited: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Endpoint {
    /// Spawns `qos_endpoint <args>` on `domain` and waits until its entities exist.
    pub async fn spawn(label: &str, domain: u32, args: &[&str]) -> Endpoint {
        use tokio::io::AsyncBufReadExt;
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_qos_endpoint"))
            .args(args)
            .env("ROS_DOMAIN_ID", domain.to_string())
            .env("RMW_IMPLEMENTATION", "rmw_cyclonedds_cpp")
            .env("CYCLONEDDS_URI", base_cyclonedds_uri())
            // the child runs until its stdin closes (see qos_endpoint.rs)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn qos_endpoint");
        let stdin = child.stdin.take();
        let mut lines = tokio::io::BufReader::new(child.stdout.take().unwrap()).lines();
        let latest = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let exited = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        {
            let latest = latest.clone();
            let exited = exited.clone();
            tokio::spawn(async move {
                let mut ready_tx = Some(ready_tx);
                while let Ok(Some(line)) = lines.next_line().await {
                    if line == "READY" {
                        if let Some(tx) = ready_tx.take() {
                            let _ = tx.send(());
                        }
                    } else if line.starts_with("RESULT ") {
                        *latest.lock().unwrap() = line;
                    }
                }
                exited.store(true, std::sync::atomic::Ordering::Relaxed);
            });
        }
        tokio::time::timeout(time::Duration::from_secs(30), ready_rx)
            .await
            .unwrap_or_else(|_| panic!("{label}: qos_endpoint {args:?} never became READY"))
            .unwrap_or_else(|_| panic!("{label}: qos_endpoint {args:?} exited before READY"));
        Endpoint {
            label: label.to_string(),
            child,
            stdin,
            latest,
            exited,
        }
    }

    /// Latest cumulative status line, parsed into `key -> value`.
    pub fn result(&self) -> std::collections::HashMap<String, String> {
        self.latest
            .lock()
            .unwrap()
            .split_whitespace()
            .skip(1)
            .filter_map(|kv| kv.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    pub fn raw_result(&self) -> String {
        let mut r = self.latest.lock().unwrap().clone();
        if self.exited.load(std::sync::atomic::Ordering::Relaxed) {
            r.push_str(" [ENDPOINT EXITED - counts frozen]");
        }
        r
    }

    /// `sub` role: number of distinct history messages received.
    pub fn history(&self) -> usize {
        self.result().get("history").and_then(|v| v.parse().ok()).unwrap_or(0)
    }

    /// `sub` role: number of live messages received.
    pub fn live(&self) -> usize {
        self.result().get("live").and_then(|v| v.parse().ok()).unwrap_or(0)
    }

    /// `sub` role: number of live messages received from publisher `tag`.
    pub fn live_from(&self, tag: &str) -> usize {
        self.count_by_tag("live_by_tag", tag)
    }

    /// `sub` role: number of distinct history messages from publisher `tag`.
    pub fn history_from(&self, tag: &str) -> usize {
        self.count_by_tag("history_by_tag", tag)
    }

    /// `sub` role: number of history messages received more than once.
    pub fn history_dups(&self) -> usize {
        self.result().get("history_dups").and_then(|v| v.parse().ok()).unwrap_or(0)
    }

    /// Whether the endpoint runs and has reported, so that zero counts are real.
    pub fn is_reporting(&self) -> bool {
        !self.exited.load(std::sync::atomic::Ordering::Relaxed)
            && !self.latest.lock().unwrap().is_empty()
    }

    fn count_by_tag(&self, field: &str, tag: &str) -> usize {
        self.result()
            .get(field)
            .and_then(|v| {
                v.split(',')
                    .filter_map(|e| e.split_once(':'))
                    .find(|(t, _)| *t == tag)
                    .and_then(|(_, n)| n.parse().ok())
            })
            .unwrap_or(0)
    }

    /// `tf_sub` role: child frame ids received so far.
    pub fn frames(&self) -> std::collections::BTreeSet<String> {
        self.result()
            .get("frames")
            .map(|v| v.split(',').filter(|s| !s.is_empty()).map(String::from).collect())
            .unwrap_or_default()
    }

    /// `tf_sub` role: messages received with child frame `frame`.
    pub fn frame_count(&self, frame: &str) -> usize {
        self.result()
            .get("counts")
            .and_then(|v| {
                v.split(',')
                    .filter_map(|e| e.split_once(':'))
                    .find(|(f, _)| *f == frame)
                    .and_then(|(_, n)| n.parse().ok())
            })
            .unwrap_or(0)
    }

    /// Sends one command line to the endpoint.
    pub async fn send(&mut self, line: &str) {
        use tokio::io::AsyncWriteExt;
        let stdin = self.stdin.as_mut().expect("endpoint stdin");
        stdin
            .write_all(format!("{line}\n").as_bytes())
            .await
            .expect("write endpoint stdin");
        stdin.flush().await.expect("flush endpoint stdin");
    }

    /// Closes stdin so the endpoint deletes its entities, SIGKILL after 5 s.
    pub async fn stop(mut self) {
        drop(self.stdin.take());
        if tokio::time::timeout(time::Duration::from_secs(5), self.child.wait())
            .await
            .is_err()
        {
            let _ = self.child.kill().await;
        }
    }
}

/// The "robot" bridge listens on `port`, the "operator" bridge connects to it. Both are symmetric.
pub fn robot_bridge_cfg(robot_domain: u32, port: u16) -> BridgeConfig {
    BridgeConfig::new(robot_domain).listen(format!("tcp/127.0.0.1:{port}"))
}

pub fn operator_bridge_cfg(operator_domain: u32, port: u16) -> BridgeConfig {
    BridgeConfig::new(operator_domain).connect(format!("tcp/127.0.0.1:{port}"))
}

/// TCP proxy from `listen_port` to `target_port` that can freeze the link without closing it,
/// so that only the zenoh lease detects the loss (as with a stalled WiFi link).
pub struct FreezableProxy {
    frozen: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl FreezableProxy {
    pub async fn start(listen_port: u16, target_port: u16) -> FreezableProxy {
        let frozen = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", listen_port))
            .await
            .expect("proxy bind");
        let f = frozen.clone();
        tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                let Ok(outbound) = tokio::net::TcpStream::connect(("127.0.0.1", target_port)).await
                else {
                    continue;
                };
                let (ri, wi) = inbound.into_split();
                let (ro, wo) = outbound.into_split();
                tokio::spawn(Self::pipe(ri, wo, f.clone()));
                tokio::spawn(Self::pipe(ro, wi, f.clone()));
            }
        });
        FreezableProxy { frozen }
    }

    async fn pipe(
        mut from: tokio::net::tcp::OwnedReadHalf,
        mut to: tokio::net::tcp::OwnedWriteHalf,
        frozen: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buf = vec![0u8; 65536];
        loop {
            while frozen.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(time::Duration::from_millis(50)).await;
            }
            match from.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    if to.write_all(&buf[..n]).await.is_err() {
                        return;
                    }
                }
            }
        }
    }

    pub fn freeze(&self) {
        self.frozen.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}
