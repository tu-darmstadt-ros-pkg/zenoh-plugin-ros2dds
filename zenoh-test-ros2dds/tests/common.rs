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
}

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

/// Create and start a bridge from `cfg`. The returned `Runtime` keeps the
/// bridge alive; pass it to [`close_bridge`] for a clean shutdown (dropping it
/// without close leaks sockets — that was a measured artifact in earlier runs).
pub async fn create_bridge_with(cfg: &BridgeConfig) -> Runtime {
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
    runtime
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
