//
// Copyright (c) 2022 ZettaScale Technology
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

use std::{
    collections::{BTreeMap, HashSet},
    ffi::CStr,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use cyclors::{
    dds_entity_t, dds_get_entity_sertype, dds_strretcode, dds_writecdr, ddsi_serdata_from_ser_iov,
    ddsi_serdata_kind_SDK_DATA, ddsi_sertype, ddsrt_iov_len_t, ddsrt_iovec_t,
};
use serde::Serialize;
use zenoh::{
    key_expr::{keyexpr, OwnedKeyExpr},
    liveliness::LivelinessToken,
    pubsub::Subscriber,
    sample::{Locality, Sample},
    Wait,
};
use zenoh_ext::{AdvancedSubscriber, AdvancedSubscriberBuilderExt, HistoryConfig};

use crate::{
    dds_utils::{
        attach_publication_matched_listener, create_dds_writer, ddsrt_iov_len_from_usize,
        delete_dds_entity, detach_listener, get_guid, publication_matched_count,
        serialize_atomic_entity_guid, AtomicDDSEntity, DDS_ENTITY_NULL,
    },
    liveliness_mgt::{new_ke_liveliness_sub, qos_to_key_expr},
    qos::{History, HistoryKind, Qos},
    qos_helpers::{
        is_transient_local, route_qos_summary, same_route_qos, writer_qos_for_remote_writers,
    },
    ros2_utils::{is_message_for_action, ros2_message_type_to_dds_type},
    ros_discovery::RosDiscoveryInfoMgr,
    route_publisher::serialize_arc_atomic_u64,
    routes_mgr::Context,
    serialize_option_as_bool, vec_into_raw_parts, LOG_PAYLOAD,
};

enum ZSubscriber {
    Subscriber(Subscriber<()>),
    AdvancedSubscriber(AdvancedSubscriber<()>),
}

/// Synthetic local_nodes entry representing "at least one DDS reader is
/// matched with this route's writer", used when the reader's node never
/// declares it in ros_discovery_info (see attach_publication_matched_listener).
const MATCHED_READERS_KEY: &str = "<matched_dds_readers>";

/// Time for readers to match a rebuilt Writer before a zero match count retires MATCHED_READERS_KEY.
/// A fixed delay, a reader matching later than this re-activates the route through the listener.
const MATCH_RECHECK_DELAY: Duration = Duration::from_secs(3);

// a route from Zenoh to DDS
#[allow(clippy::upper_case_acronyms)]
#[derive(Serialize)]
pub struct RouteSubscriber {
    // the ROS2 Subscriber name
    ros2_name: String,
    // the ROS2 type
    ros2_type: String,
    // the Zenoh key expression used for routing
    zenoh_key_expr: OwnedKeyExpr,
    // the context
    #[serde(skip)]
    context: Context,
    // the zenoh subscriber receiving messages to be re-published by the DDS Writer
    // `None` when route is created on a remote announcement and no local ROS2 Subscriber discovered yet
    #[serde(rename = "is_active", serialize_with = "serialize_option_as_bool")]
    zenoh_subscriber: Option<ZSubscriber>,
    // QoS used when announcing the route on matched-reader activation
    #[serde(skip)]
    announce_qos: Qos,
    // the local DDS Writer re-publishing what comes from zenoh, NULL until needed, swapped on rebuild
    #[serde(serialize_with = "serialize_atomic_entity_guid")]
    dds_writer: Arc<AtomicDDSEntity>,
    // QoS of the current DDS Writer (None while there is none)
    #[serde(rename = "writer_qos", serialize_with = "serialize_opt_qos")]
    writer_qos: Option<Qos>,
    // action routes have a fixed Writer QoS, topic routes follow the remote publishers
    #[serde(skip)]
    fixed_qos: bool,
    // QoS announced by each remote publisher route, see remote_publisher_key
    #[serde(skip)]
    remote_writer_qos: BTreeMap<String, Qos>,
    // if the Writer is TRANSIENT_LOCAL
    transient_local: bool,
    // queries timeout for historical publication (if TRANSIENT_LOCAL)
    queries_timeout: Duration,
    // if the topic is keyless
    #[serde(skip)]
    keyless: bool,
    // a liveliness token associated to this route, for announcement to other plugins
    #[serde(skip)]
    liveliness_token: Option<LivelinessToken>,
    // the list of remote routes served by this route ("<zenoh_id>:<zenoh_key_expr>"")
    remote_routes: HashSet<String>,
    // the list of nodes served by this route
    local_nodes: HashSet<String>,
    // count of messages successfully forwarded Zenoh->DDS by this route
    #[serde(serialize_with = "serialize_arc_atomic_u64")]
    fwd_msg_count: Arc<AtomicU64>,
    // count of payload bytes successfully forwarded Zenoh->DDS by this route
    #[serde(serialize_with = "serialize_arc_atomic_u64")]
    fwd_byte_count: Arc<AtomicU64>,
}

impl Drop for RouteSubscriber {
    fn drop(&mut self) {
        let writer = self.dds_writer.swap(DDS_ENTITY_NULL, Ordering::Relaxed);
        if writer != DDS_ENTITY_NULL {
            tracing::debug!("{self}: delete Writer");
            delete_route_writer(writer, &self.context.ros_discovery_mgr);
        }
    }
}

/// Deletes a route Writer, detaching its listener first so that no spurious unmatch is reported.
fn delete_route_writer(writer: dds_entity_t, ros_discovery_mgr: &RosDiscoveryInfoMgr) {
    if let Err(e) = detach_listener(writer) {
        tracing::debug!("detach listener from DDS Writer {writer}: {e}");
    }
    match get_guid(&writer) {
        Ok(gid) => ros_discovery_mgr.remove_dds_writer(gid),
        Err(e) => tracing::warn!("DDS Writer {writer}: {e}"),
    }
    if let Err(e) = delete_dds_entity(writer) {
        tracing::warn!("error deleting DDS Writer {writer}: {e}");
    }
}

impl fmt::Display for RouteSubscriber {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Route Subscriber (Zenoh:{} -> ROS:{})",
            self.zenoh_key_expr, self.ros2_name
        )
    }
}

impl RouteSubscriber {
    /// Route with a fixed Writer QoS, created immediately (action routes).
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        keyless: bool,
        mut writer_qos: Qos,
        context: Context,
    ) -> Result<RouteSubscriber, String> {
        // force RELIABLE QoS for Writers (#23)
        if let Some(cyclors::qos::Reliability {
            kind: cyclors::qos::ReliabilityKind::BEST_EFFORT,
            ..
        }) = &mut writer_qos.reliability
        {
            // Per DDS specification, the default Reliability value for DataWriters is RELIABLE with max_blocking_time=100ms
            // Thus just use default value.
            writer_qos.reliability = None;
        }
        let mut route = Self::new(
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            keyless,
            writer_qos.clone(),
            true,
            context,
        );
        route.install_writer(writer_qos)?;
        Ok(route)
    }

    /// Topic route whose Writer QoS follows the announced remote publishers.
    /// `announce_qos` is announced when undeclared DDS readers activate the route.
    pub async fn create_for_topic(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        keyless: bool,
        announce_qos: Qos,
        context: Context,
    ) -> Result<RouteSubscriber, String> {
        tracing::debug!("Route Subscriber ({zenoh_key_expr} -> {ros2_name}): creation with type {ros2_type}");
        Ok(Self::new(
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            keyless,
            announce_qos,
            false,
            context,
        ))
    }

    fn new(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        keyless: bool,
        announce_qos: Qos,
        fixed_qos: bool,
        context: Context,
    ) -> RouteSubscriber {
        let queries_timeout = context.config.get_queries_timeout_tl_sub(&ros2_name);
        RouteSubscriber {
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            context,
            zenoh_subscriber: None,
            announce_qos,
            dds_writer: Arc::new(DDS_ENTITY_NULL.into()),
            writer_qos: None,
            fixed_qos,
            remote_writer_qos: BTreeMap::new(),
            transient_local: false,
            queries_timeout,
            keyless,
            liveliness_token: None,
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
            fwd_msg_count: Arc::new(AtomicU64::new(0)),
            fwd_byte_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Replaces the route's Writer by a new one with `qos`, the old one is deleted last.
    fn install_writer(&mut self, qos: Qos) -> Result<(), String> {
        tracing::debug!("{self}: create Writer with {qos:?}");
        let writer = create_dds_writer(
            self.context.participant,
            format!("rt{}", self.ros2_name),
            ros2_message_type_to_dds_type(&self.ros2_type),
            self.keyless,
            qos.clone(),
        )?;
        // Read back the GUID before keeping the writer; on failure (e.g. transient
        // BAD_PARAMETER under participant churn) delete the writer instead of
        // leaking it and leaving a half-created route.
        let writer_gid = match get_guid(&writer) {
            Ok(gid) => gid,
            Err(e) => {
                let _ = delete_dds_entity(writer);
                return Err(e);
            }
        };
        self.context.ros_discovery_mgr.add_dds_writer(writer_gid);

        // DDS-level local-interest detection: activate this route for ANY
        // matched reader, whether or not its node declares it in
        // ros_discovery_info (hector-UI subscriptions and ros2_control
        // endpoints show up as _NODE_NAME_UNKNOWN_ and were never served).
        // ignore_local=PARTICIPANT on the writer QoS excludes the bridge's
        // own readers, so no self-activation loop is possible.
        {
            let cb_name = self.ros2_name.clone();
            let tx = self.context.matched_reader_tx.clone();
            if let Err(e) = attach_publication_matched_listener(writer, move |count| {
                let _ = tx.try_send((cb_name.clone(), count > 0));
            }) {
                tracing::warn!(
                    "{self}: cannot attach publication-matched listener (undeclared \
                     local readers will not be served): {e}"
                );
            }
        }

        let old = self.dds_writer.swap(writer, Ordering::Relaxed);
        if old != DDS_ENTITY_NULL {
            delete_route_writer(old, &self.context.ros_discovery_mgr);
            // the deleted Writer's listener can no longer report its readers leaving
            if self.local_nodes.contains(MATCHED_READERS_KEY) {
                let current = self.dds_writer.clone();
                let tx = self.context.matched_reader_tx.clone();
                let ros2_name = self.ros2_name.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(MATCH_RECHECK_DELAY).await;
                    if current.load(Ordering::Relaxed) == writer
                        && publication_matched_count(writer) == Some(0)
                        && tx.send_async((ros2_name, false)).await.is_err()
                    {
                        tracing::debug!("routes manager gone, matched readers re-check dropped");
                    }
                });
            }
        }
        self.transient_local = is_transient_local(&qos);
        self.writer_qos = Some(qos);
        Ok(())
    }

    /// Creates or rebuilds the Writer for the announced remote publishers.
    /// Without any, a TRANSIENT_LOCAL Writer becomes the VOLATILE fallback, as in DDS the history leaves with its publisher.
    pub async fn update_writer(&mut self) -> Result<(), String> {
        if self.fixed_qos {
            return Ok(());
        }
        if self.remote_writer_qos.is_empty() {
            if self.transient_local {
                self.install_writer(self.fallback_writer_qos())?;
                if self.zenoh_subscriber.is_some() {
                    self.undeclare_zenoh_subscriber();
                    self.declare_zenoh_subscriber().await?;
                }
            }
            return Ok(());
        }
        // a failed re-declaration left an active route without zenoh subscriber
        if !self.local_nodes.is_empty()
            && self.liveliness_token.is_some()
            && self.zenoh_subscriber.is_none()
        {
            self.declare_zenoh_subscriber().await?;
        }
        let remote: Vec<Qos> = self.remote_writer_qos.values().cloned().collect();
        let desired = writer_qos_for_remote_writers(
            &remote,
            self.context.config.transient_local_cache_multiplier,
        );
        if let Some(current) = &self.writer_qos {
            if same_route_qos(current, &desired, false) {
                return Ok(());
            }
            tracing::info!(
                "{self}: remote publishers' QoS changed, rebuilding DDS Writer ({} to {})",
                route_qos_summary(current),
                route_qos_summary(&desired)
            );
        }
        let was_tl = self.transient_local;
        self.install_writer(desired)?;
        if self.zenoh_subscriber.is_some() && (was_tl || self.transient_local) {
            // re-fetch the remote TL history into the new Writer, matched readers may get it twice
            self.undeclare_zenoh_subscriber();
            self.declare_zenoh_subscriber().await?;
        }
        Ok(())
    }

    async fn declare_zenoh_subscriber(&mut self) -> Result<(), String> {
        // Callback routing message received by Zenoh subscriber to the current DDS Writer
        let ros2_name = self.ros2_name.clone();
        let dds_writer = self.dds_writer.clone();
        let fwd_msg_count = self.fwd_msg_count.clone();
        let fwd_byte_count = self.fwd_byte_count.clone();
        let subscriber_callback = move |s: Sample| {
            route_zenoh_message_to_dds(
                s,
                &ros2_name,
                dds_writer.load(Ordering::Relaxed),
                &fwd_msg_count,
                &fwd_byte_count,
            );
        };

        // a TRANSIENT_LOCAL Writer fetches as much remote history as it keeps
        self.zenoh_subscriber = if self.transient_local {
            let max_samples = match self.writer_qos.as_ref().and_then(|q| q.history.as_ref()) {
                Some(History {
                    kind: HistoryKind::KEEP_ALL,
                    ..
                }) => usize::MAX,
                Some(History { depth, .. }) => (*depth).max(1) as usize,
                None => 1,
            };
            let sub = self
                .context
                .zsession
                .declare_subscriber(&self.zenoh_key_expr)
                .advanced()
                .callback(subscriber_callback)
                .allowed_origin(Locality::Remote) // Allow only remote publications to avoid loops
                .history(
                    HistoryConfig::default()
                        .detect_late_publishers()
                        .max_samples(max_samples),
                )
                .query_timeout(self.queries_timeout)
                .await
                .map_err(|e| format!("{self}: failed to create FetchingSubscriber: {e}",))?;
            Some(ZSubscriber::AdvancedSubscriber(sub))
        } else {
            let sub = self
                .context
                .zsession
                .declare_subscriber(&self.zenoh_key_expr)
                .callback(subscriber_callback)
                .allowed_origin(Locality::Remote) // Allow only remote publications to avoid loops
                .await
                .map_err(|e| format!("{self}: failed to create Subscriber: {e}"))?;
            Some(ZSubscriber::Subscriber(sub))
        };
        Ok(())
    }

    fn undeclare_zenoh_subscriber(&mut self) {
        match self.zenoh_subscriber.take() {
            Some(ZSubscriber::Subscriber(s)) => {
                if let Err(e) = s.undeclare().wait() {
                    tracing::debug!("Unable to undeclare subscriber: {:?}", e);
                }
            }
            Some(ZSubscriber::AdvancedSubscriber(s)) => {
                if let Err(e) = s.undeclare().wait() {
                    tracing::debug!("Unable to undeclare subscriber: {:?}", e);
                }
            }
            None => {}
        };
    }

    /// VOLATILE Writer QoS for publishers that announce nothing (plain zenoh applications), type hash and depth from the local Reader.
    fn fallback_writer_qos(&self) -> Qos {
        let qos = Qos {
            user_data: self.announce_qos.user_data.clone(),
            history: self.announce_qos.history.clone(),
            ..Default::default()
        };
        writer_qos_for_remote_writers(&[qos], self.context.config.transient_local_cache_multiplier)
    }

    // Announce the route over Zenoh via a LivelinessToken
    async fn announce_route(&mut self, discovered_reader_qos: &Qos) -> Result<(), String> {
        tracing::debug!("{self} activate");
        if !self.fixed_qos && self.dds_writer.load(Ordering::Relaxed) == DDS_ENTITY_NULL {
            self.install_writer(self.fallback_writer_qos())?;
        }
        self.declare_zenoh_subscriber().await?;

        // if not for an Action (since actions declare their own liveliness)
        if !is_message_for_action(&self.ros2_name) {
            // create associated LivelinessToken
            let liveliness_ke = new_ke_liveliness_sub(
                &self.context.zsession.zid().into_keyexpr(),
                &self.zenoh_key_expr,
                &self.ros2_type,
                self.keyless,
                discovered_reader_qos,
            )?;
            let ros2_name = self.ros2_name.clone();
            self.liveliness_token = Some(
                self.context.zsession
                    .liveliness()
                    .declare_token(liveliness_ke)
                    .await
                    .map_err(|e| {
                        format!(
                            "Failed create LivelinessToken associated to route for Subscriber {ros2_name} : {e}"
                        )
                    })?,
            );
        }
        Ok(())
    }

    // Retire the route over Zenoh removing the LivelinessToken
    fn retire_route(&mut self) {
        tracing::debug!("{self} deactivate");
        // Drop Zenoh Subscriber and Liveliness token
        // The DDS Writer remains to be discovered by local ROS nodes
        self.undeclare_zenoh_subscriber();
        self.liveliness_token = None;
    }

    /// Action routes: a remote route is served (the Writer QoS is fixed).
    #[inline]
    pub fn add_remote_route(&mut self, zenoh_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .insert(format!("{zenoh_id}:{zenoh_key_expr}"));
        tracing::debug!("{self} now serving remote routes {:?}", self.remote_routes);
    }

    /// Topic routes: a remote publisher route was announced with `writer_qos`.
    pub async fn add_remote_publisher(
        &mut self,
        zenoh_id: &str,
        zenoh_key_expr: &keyexpr,
        keyless: bool,
        writer_qos: &Qos,
    ) -> Result<(), String> {
        let key = remote_publisher_key(zenoh_id, zenoh_key_expr, keyless, writer_qos);
        self.remote_routes.insert(key.clone());
        self.remote_writer_qos.insert(key, writer_qos.clone());
        tracing::debug!("{self} now serving remote routes {:?}", self.remote_routes);
        self.update_writer().await
    }

    /// Action routes: a remote route is no longer served.
    #[inline]
    pub fn remove_remote_route(&mut self, zenoh_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .remove(&format!("{zenoh_id}:{zenoh_key_expr}"));
        tracing::debug!("{self} now serving remote routes {:?}", self.remote_routes);
    }

    /// Topic routes: a remote publisher announcement was retired, callers then call `update_writer`.
    /// The QoS identifies it because a re-announcement precedes the retirement of the old one.
    pub fn remove_remote_publisher(
        &mut self,
        zenoh_id: &str,
        zenoh_key_expr: &keyexpr,
        keyless: bool,
        writer_qos: &Qos,
    ) {
        let key = remote_publisher_key(zenoh_id, zenoh_key_expr, keyless, writer_qos);
        self.remote_routes.remove(&key);
        self.remote_writer_qos.remove(&key);
        tracing::debug!("{self} now serving remote routes {:?}", self.remote_routes);
    }

    /// Remove all remote_routes entries for a departed bridge (prefix "<zenoh_id>:").
    /// Returns whether any entry was removed, callers then call `update_writer`.
    #[inline]
    pub fn prune_remote_routes_with_prefix(&mut self, prefix: &str) -> bool {
        let before = self.remote_routes.len();
        self.remote_routes.retain(|r| !r.starts_with(prefix));
        self.remote_writer_qos.retain(|r, _| !r.starts_with(prefix));
        if self.remote_routes.len() == before {
            return false;
        }
        tracing::debug!("{self} now serving remote routes {:?}", self.remote_routes);
        true
    }

    #[inline]
    pub fn is_serving_remote_route(&self) -> bool {
        !self.remote_routes.is_empty()
    }

    #[inline]
    pub async fn add_local_node(&mut self, entity_key: String, discovered_reader_qos: &Qos) {
        self.local_nodes.insert(entity_key);
        tracing::debug!("{self} now serving local nodes {:?}", self.local_nodes);
        // if 1st local node added, activate the route
        if self.local_nodes.len() == 1 {
            if let Err(e) = self.announce_route(discovered_reader_qos).await {
                tracing::error!("{self} activation failed: {e}");
            }
        }
    }

    #[inline]
    pub fn remove_local_node(&mut self, entity_key: &str) {
        self.local_nodes.remove(entity_key);
        tracing::debug!("{self} now serving local nodes {:?}", self.local_nodes);
        // if last local node removed, deactivate the route
        if self.local_nodes.is_empty() {
            self.retire_route();
        }
    }

    /// DDS-level matched-reader state changed (publication-matched listener).
    /// A matched reader is authoritative local interest even when its node
    /// never declares it in ros_discovery_info: represent it as a synthetic
    /// local_nodes entry so the usual announce/retire lifecycle applies.
    pub async fn update_matched_readers(&mut self, matched: bool) {
        if matched {
            if self.local_nodes.insert(MATCHED_READERS_KEY.to_string()) {
                tracing::debug!("{self} now serving matched (possibly undeclared) DDS readers");
                if self.local_nodes.len() == 1 {
                    tracing::info!(
                        "{self}: activating for matched DDS reader(s) not declared by any ROS node"
                    );
                    let qos = self.announce_qos.clone();
                    if let Err(e) = self.announce_route(&qos).await {
                        tracing::error!("{self} activation failed: {e}");
                    }
                }
            }
        } else if self.local_nodes.remove(MATCHED_READERS_KEY) && self.local_nodes.is_empty() {
            tracing::info!("{self}: last matched DDS reader gone - retiring");
            self.retire_route();
        }
    }

    #[inline]
    pub fn is_serving_local_node(&self) -> bool {
        !self.local_nodes.is_empty()
    }

    #[inline]
    pub fn is_unused(&self) -> bool {
        !self.is_serving_local_node() && !self.is_serving_remote_route()
    }
}

/// Identity of a remote publisher announcement, prefixed by "<zenoh_id>:" for pruning.
fn remote_publisher_key(
    zenoh_id: &str,
    zenoh_key_expr: &keyexpr,
    keyless: bool,
    writer_qos: &Qos,
) -> String {
    format!(
        "{zenoh_id}:{zenoh_key_expr}:{}",
        qos_to_key_expr(keyless, writer_qos)
    )
}

fn serialize_opt_qos<S>(qos: &Option<Qos>, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    s.serialize_str(&qos.as_ref().map(route_qos_summary).unwrap_or_default())
}

fn route_zenoh_message_to_dds(
    s: Sample,
    ros2_name: &str,
    data_writer: dds_entity_t,
    fwd_msg_count: &Arc<AtomicU64>,
    fwd_byte_count: &Arc<AtomicU64>,
) {
    if data_writer == DDS_ENTITY_NULL {
        tracing::trace!("Route Subscriber (Zenoh -> ROS:{ros2_name}): no DDS Writer yet, dropping sample");
        return;
    }
    // payload size for traffic accounting (captured before the buffer is moved)
    let payload_len = s.payload().len();
    if *LOG_PAYLOAD {
        tracing::debug!(
            "Route Subscriber (Zenoh:{} -> ROS:{}): routing message - payload: {:02x?}",
            s.key_expr(),
            &ros2_name,
            s.payload()
        );
    } else {
        tracing::trace!(
            "Route Subscriber (Zenoh:{} -> ROS:{}): routing message - {} bytes",
            s.key_expr(),
            &ros2_name,
            s.payload().len()
        );
    }

    unsafe {
        let bs = s.payload().to_bytes().to_vec();
        // As per the Vec documentation (see https://doc.rust-lang.org/std/vec/struct.Vec.html#method.into_raw_parts)
        // the only way to correctly releasing it is to create a vec using from_raw_parts
        // and then have its destructor do the cleanup.
        // Thus, while tempting to just pass the raw pointer to cyclone and then free it from C,
        // that is not necessarily safe or guaranteed to be leak free.
        // TODO replace when stable https://github.com/rust-lang/rust/issues/65816
        let (ptr, len, capacity) = vec_into_raw_parts(bs);
        let size: ddsrt_iov_len_t = match ddsrt_iov_len_from_usize(len) {
            Ok(s) => s,
            Err(_) => {
                tracing::warn!(
                    "Route Subscriber (Zenoh:{} -> ROS:{}): can't route message; excessive payload size ({})",
                    s.key_expr(),
                    ros2_name,
                    len
                );
                // reclaim the payload buffer (was leaked on this path)
                drop(Vec::from_raw_parts(ptr, len, capacity));
                return;
            }
        };

        let data_out = ddsrt_iovec_t {
            iov_base: ptr as *mut std::ffi::c_void,
            iov_len: size,
        };

        let mut sertype_ptr: *const ddsi_sertype = std::ptr::null_mut();
        let ret = dds_get_entity_sertype(data_writer, &mut sertype_ptr);
        if ret < 0 {
            tracing::warn!(
                "Route Subscriber (Zenoh:{} -> ROS:{}): can't route message; sertype lookup failed ({})",
                s.key_expr(),
                ros2_name,
                CStr::from_ptr(dds_strretcode(ret))
                    .to_str()
                    .unwrap_or("unrecoverable DDS retcode")
            );
            // reclaim the payload buffer (was leaked on this path)
            drop(Vec::from_raw_parts(ptr, len, capacity));
            return;
        }

        let fwdp = ddsi_serdata_from_ser_iov(
            sertype_ptr,
            ddsi_serdata_kind_SDK_DATA,
            1,
            &data_out,
            size as usize,
        );

        let ret = dds_writecdr(data_writer, fwdp);
        if ret < 0 {
            tracing::warn!(
                "Route Subscriber (Zenoh:{} -> ROS:{}): DDS write({data_writer}) failed: {}",
                s.key_expr(),
                ros2_name,
                CStr::from_ptr(dds_strretcode(ret))
                    .to_str()
                    .unwrap_or("unrecoverable DDS retcode")
            );
            // reclaim the payload buffer (was leaked on this path - and this
            // path fires repeatedly under congestion, when dds_writecdr times
            // out on a full reliable history)
            drop(Vec::from_raw_parts(ptr, len, capacity));
            return;
        }

        drop(Vec::from_raw_parts(ptr, len, capacity));
    }

    // count only successful forwards (all failure paths returned early above)
    fwd_msg_count.fetch_add(1, Ordering::Relaxed);
    fwd_byte_count.fetch_add(payload_len as u64, Ordering::Relaxed);
}
