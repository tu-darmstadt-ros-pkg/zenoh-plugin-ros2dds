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
    collections::HashSet,
    fmt,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Duration,
};

use cyclors::{
    qos::{HistoryKind, Qos},
    DDS_LENGTH_UNLIMITED,
};
use serde::{Serialize, Serializer};
use zenoh::{
    bytes::ZBytes,
    key_expr::{keyexpr, OwnedKeyExpr},
    liveliness::LivelinessToken,
    matching::MatchingListener,
    qos::{CongestionControl, Priority, Reliability},
    sample::Locality,
    Wait,
};
use zenoh_ext::{AdvancedPublisher, AdvancedPublisherBuilderExt, CacheConfig};

use crate::{
    dds_types::{DDSRawSample, TypeInfo},
    dds_utils::{
        create_dds_reader, delete_dds_entity, get_guid, serialize_atomic_entity_guid,
        AtomicDDSEntity, DDS_ENTITY_NULL,
    },
    liveliness_mgt::new_ke_liveliness_pub,
    qos_helpers::*,
    ros2_utils::{is_message_for_action, ros2_message_type_to_dds_type},
    ros_discovery::RosDiscoveryInfoMgr,
    routes_mgr::Context,
    Config, LOG_PAYLOAD,
};

pub struct ZPublisher {
    publisher: Arc<AdvancedPublisher<'static>>,
    cache_size: usize,
    // uncached publisher for the VOLATILE Writers of a topic that also has TRANSIENT_LOCAL ones
    volatile_publisher: Option<Arc<AdvancedPublisher<'static>>>,
}

impl Deref for ZPublisher {
    type Target = Arc<AdvancedPublisher<'static>>;

    fn deref(&self) -> &Self::Target {
        &self.publisher
    }
}

// Capacity of the per-route DDS->Zenoh forward queue. Sized to absorb multi-
// second bursts of high-rate topics (e.g. 2.5s of a 100Hz /tf) without ever
// blocking Cyclone's shared delivery thread.
const FWD_QUEUE_SIZE: usize = 256;
// Capacity of the forward channel, a flume channel cannot grow so `FwdQueue::limit` bounds each route.
const FWD_QUEUE_MAX: usize = 4096;
// Payload bytes a route queues while zenoh is congested, about 8 point clouds of 2 MB.
const FWD_QUEUE_BYTES: usize = 16 << 20;

/// Attachment of samples from VOLATILE Writers on a topic that also has TRANSIENT_LOCAL ones.
/// The remote bridge writes them through a VOLATILE DDS Writer, out of the TRANSIENT_LOCAL history.
pub const VOLATILE_ATTACHMENT: &[u8] = b"ros2dds:volatile";

// Item of the per-route forward queue.
enum FwdCmd {
    // `volatile` samples come from the VOLATILE Reader and stay out of the TRANSIENT_LOCAL cache
    Msg { payload: ZBytes, volatile: bool },
    Stop,
}

// Per-route DDS->Zenoh forward queue, the DDS Reader callbacks push and the forwarder thread pops.
#[derive(Clone)]
struct FwdQueue {
    tx: flume::Sender<FwdCmd>,
    rx: flume::Receiver<FwdCmd>,
    limit: Arc<AtomicUsize>,
    bytes: Arc<AtomicUsize>,
    dropped: Arc<AtomicU64>,
}

impl FwdQueue {
    fn new(limit: usize) -> Self {
        let (tx, rx) = flume::bounded(FWD_QUEUE_MAX);
        FwdQueue {
            tx,
            rx,
            limit: Arc::new(AtomicUsize::new(limit)),
            bytes: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Never blocks. Evicts the oldest samples beyond `limit` or FWD_QUEUE_BYTES, as KEEP_LAST does,
    /// so a congested link neither grows memory nor replays stale data first. Returns the evicted count.
    fn push(&self, payload: ZBytes, volatile: bool) -> u64 {
        let len = payload.len();
        let mut evicted = 0;
        while self.tx.len() >= self.limit.load(Ordering::Relaxed)
            || (!self.tx.is_empty() && self.bytes.load(Ordering::Relaxed) + len > FWD_QUEUE_BYTES)
        {
            match self.pop() {
                Some(_) => evicted += 1,
                None => break,
            }
        }
        self.bytes.fetch_add(len, Ordering::Relaxed);
        if self.tx.try_send(FwdCmd::Msg { payload, volatile }).is_err() {
            self.bytes.fetch_sub(len, Ordering::Relaxed);
            evicted += 1;
        }
        if evicted > 0 {
            self.dropped.fetch_add(evicted, Ordering::Relaxed);
        }
        evicted
    }

    fn pop(&self) -> Option<FwdCmd> {
        let cmd = self.rx.try_recv().ok()?;
        if let FwdCmd::Msg { payload, .. } = &cmd {
            self.bytes.fetch_sub(payload.len(), Ordering::Relaxed);
        }
        Some(cmd)
    }
}

// a route from DDS to Zenoh
#[allow(clippy::upper_case_acronyms)]
#[derive(Serialize)]
pub struct RoutePublisher {
    // the ROS2 Publisher name
    ros2_name: String,
    // the ROS2 type
    ros2_type: String,
    // the Zenoh key expression used for routing
    zenoh_key_expr: OwnedKeyExpr,
    // the context
    #[serde(skip)]
    context: Context,
    // the zenoh publisher re-publishing what the DDS Reader receives, replaced when the route is rebuilt
    #[serde(
        rename = "publication_cache_size",
        serialize_with = "serialize_pub_cache"
    )]
    zenoh_publisher: Arc<RwLock<ZPublisher>>,
    // the local DDS Reader created to serve the route (i.e. re-publish to zenoh message coming from DDS)
    #[serde(serialize_with = "serialize_atomic_entity_guid")]
    dds_reader: Arc<AtomicDDSEntity>,
    // the VOLATILE Reader of a topic with both TRANSIENT_LOCAL and VOLATILE local Writers
    #[serde(serialize_with = "serialize_atomic_entity_guid")]
    dds_volatile_reader: Arc<AtomicDDSEntity>,
    // activates the DDS Reader while remote subscribers match. Kept here so Drop undeclares it,
    // its callback holds the zenoh Publisher and would otherwise keep forwarding.
    #[serde(skip)]
    _matching_listener: Option<MatchingListener<()>>,
    // the Zenoh Priority for publications
    #[serde(serialize_with = "serialize_priority")]
    priority: Priority,
    #[serde(skip)]
    is_express: bool,
    // TypeInfo for Reader creation (if available)
    #[serde(skip)]
    _type_info: Option<Arc<TypeInfo>>,
    // if the topic is keyless
    #[serde(skip)]
    keyless: bool,
    // the DDS Reader QoS, shared with the matching listener that creates the Reader
    #[serde(serialize_with = "serialize_reader_qos")]
    reader_qos: Arc<Mutex<Qos>>,
    #[serde(serialize_with = "serialize_volatile_reader_qos")]
    volatile_reader_qos: Arc<Mutex<Option<Qos>>>,
    // true for action routes (fixed protocol QoS)
    #[serde(skip)]
    fixed_qos: bool,
    // type hash source of the placeholder Reader used while no local Writer is known
    #[serde(skip)]
    fallback_qos: Qos,
    // QoS announced in the current liveliness token (if any)
    #[serde(skip)]
    announced_qos: Option<Qos>,
    // second announcement for the VOLATILE Writers of a topic with mixed durability
    #[serde(skip)]
    volatile_announcement: Option<(Qos, LivelinessToken)>,
    // whether the "local Writers disagree" warning was already logged
    #[serde(skip)]
    warned_mixed_writers: bool,
    // a liveliness token associated to this route, for announcement to other plugins
    #[serde(skip)]
    liveliness_token: Option<LivelinessToken>,
    // the list of remote routes served by this route ("<zenoh_id>:<zenoh_key_expr>"")
    remote_routes: HashSet<String>,
    // the list of nodes served by this route
    local_nodes: HashSet<String>,
    // count of messages successfully forwarded DDS->Zenoh by this route
    #[serde(serialize_with = "serialize_arc_atomic_u64")]
    fwd_msg_count: Arc<AtomicU64>,
    // count of payload bytes successfully forwarded DDS->Zenoh by this route
    #[serde(serialize_with = "serialize_arc_atomic_u64")]
    fwd_byte_count: Arc<AtomicU64>,
    // messages evicted from the full forward queue because zenoh could not keep up
    #[serde(serialize_with = "serialize_arc_atomic_u64")]
    dropped_msg_count: Arc<AtomicU64>,
    // the DDS Reader callbacks enqueue here, the route's forwarder thread does the possibly blocking put
    #[serde(skip)]
    fwd_queue: FwdQueue,
    // set on Drop, ends the forwarder thread even when FwdCmd::Stop cannot be queued
    #[serde(skip)]
    fwd_stop: Arc<AtomicBool>,
    // Serializes DDS Reader activation/deactivation between the matching-
    // listener callback (zenoh thread) and the routes-mgr paths
    // (add_remote_route reactivation, remove/prune deactivation, rebuild).
    #[serde(skip)]
    activation_lock: Arc<Mutex<()>>,
    // Last matching status seen by the listener. add_remote_route uses it to
    // re-activate a reader deactivated by a late-processed retire/prune when
    // no further matching transition will ever fire ("listed but dead").
    #[serde(skip)]
    is_matching: Arc<AtomicBool>,
}

impl Drop for RoutePublisher {
    fn drop(&mut self) {
        // Undeclare the matching listener FIRST, before deactivating the Reader:
        // this releases its clone of the zenoh Publisher (breaking the reference
        // cycle), and `wait_callbacks()` blocks until any in-flight callback
        // returns so it can't re-create a DDS Reader after we've torn it down.
        self.undeclare_matching_listener();
        self.deactivate_dds_reader();
        // the flag ends the forwarder within its 10 s wakeup, Stop ends it at once if the queue has room
        self.fwd_stop.store(true, Ordering::Relaxed);
        let _ = self.fwd_queue.tx.try_send(FwdCmd::Stop);
    }
}

impl fmt::Display for RoutePublisher {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Route Publisher (ROS:{} -> Zenoh:{})",
            self.ros2_name, self.zenoh_key_expr
        )
    }
}

/// Size of the zenoh publication cache for a route whose DDS Reader has `reader_qos`, 0 if not TRANSIENT_LOCAL.
fn tl_cache_size(ros2_name: &str, reader_qos: &Qos, keyless: bool, multiplier: usize) -> usize {
    if !is_transient_local(reader_qos) {
        return 0;
    }
    let history_qos = get_history_or_default(reader_qos);
    let durability_service_qos = get_durability_service_or_default(reader_qos);
    let n = history_qos.depth.max(1);
    let history = match history_qos.kind {
        // only 1 instance
        HistoryKind::KEEP_LAST if keyless => n as usize,
        HistoryKind::KEEP_LAST if durability_service_qos.max_instances == DDS_LENGTH_UNLIMITED => {
            usize::MAX
        }
        // assumes the instances are published at the same rate, there is one cache per route
        HistoryKind::KEEP_LAST if durability_service_qos.max_instances > 0 => {
            n.saturating_mul(durability_service_qos.max_instances) as usize
        }
        HistoryKind::KEEP_LAST => n as usize,
        HistoryKind::KEEP_ALL => usize::MAX,
    };
    // a fixed QoS route may serve several Writers
    let history = history.saturating_mul(multiplier);
    tracing::debug!(
        "Route Publisher ({ros2_name}): caching TRANSIENT_LOCAL publications with history={history} (computed from Reader's QoS: history=({:?},{}), durability_service.max_instances={})",
        history_qos.kind, history_qos.depth, durability_service_qos.max_instances
    );
    history
}

impl RoutePublisher {
    /// Route with a fixed DDS Reader QoS (action routes).
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        type_info: &Option<Arc<TypeInfo>>,
        keyless: bool,
        reader_qos: Qos,
        context: Context,
    ) -> Result<RoutePublisher, String> {
        Self::new(
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            type_info,
            keyless,
            reader_qos.clone(),
            reader_qos,
            true,
            context,
        )
        .await
    }

    /// Topic route whose Reader QoS follows `local_writers`, see readers_qos_for_local_writers.
    pub async fn create_for_topic(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        keyless: bool,
        local_writers: &[Qos],
        fallback_qos: Qos,
        context: Context,
    ) -> Result<RoutePublisher, String> {
        let (reader_qos, volatile_reader_qos) = readers_qos_for_local_writers(
            local_writers,
            &fallback_qos,
            context.config.transient_local_cache_multiplier,
        );
        let mut route = Self::new(
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            &None,
            keyless,
            reader_qos,
            fallback_qos,
            false,
            context,
        )
        .await?;
        if volatile_reader_qos.is_some() {
            route.set_volatile_reader(volatile_reader_qos).await?;
        }
        Ok(route)
    }

    #[allow(clippy::too_many_arguments)]
    async fn new(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        type_info: &Option<Arc<TypeInfo>>,
        keyless: bool,
        reader_qos: Qos,
        fallback_qos: Qos,
        fixed_qos: bool,
        context: Context,
    ) -> Result<RoutePublisher, String> {
        tracing::debug!(
            "Route Publisher ({ros2_name} -> {zenoh_key_expr}): creation with type {ros2_type}"
        );

        // Priority if configured for this topic
        let (priority, is_express) = context
            .config
            .get_pub_priority_and_express(&ros2_name)
            .unwrap_or_default();

        // a topic route's Reader depth already includes the multiplier
        let multiplier = if fixed_qos {
            context.config.transient_local_cache_multiplier
        } else {
            1
        };
        let cache_size = tl_cache_size(&ros2_name, &reader_qos, keyless, multiplier);
        let publisher = declare_zenoh_publisher(
            &ros2_name,
            &zenoh_key_expr,
            &context,
            &reader_qos,
            cache_size,
            priority,
            is_express,
        )
        .await?;
        let zenoh_publisher = Arc::new(RwLock::new(ZPublisher {
            publisher,
            cache_size,
            volatile_publisher: None,
        }));

        // admin space traffic counters, counted per successful forward
        let fwd_msg_count = Arc::new(AtomicU64::new(0));
        let fwd_byte_count = Arc::new(AtomicU64::new(0));

        // Reader callbacks run on Cyclone's delivery thread shared by all routes, so they only queue
        // and the forwarder below does the possibly blocking put.
        // TRANSIENT_LOCAL routes queue as much as they cache, so the history burst at activation fits
        let fwd_queue = FwdQueue::new(queue_limit_for(cache_size));
        let dropped_msg_count = fwd_queue.dropped.clone();
        let fwd_stop = Arc::new(AtomicBool::new(false));
        {
            let zenoh_publisher = zenoh_publisher.clone();
            let fwd_msg_count = fwd_msg_count.clone();
            let fwd_byte_count = fwd_byte_count.clone();
            let route_id = format!("Route Publisher (ROS:{ros2_name} -> Zenoh:{zenoh_key_expr})");
            let fwd_stop = fwd_stop.clone();
            let fwd_queue = fwd_queue.clone();
            // A blocking put stalls a whole tokio worker, so each route forwards on its own thread.
            // One OS thread per publisher route, a shared pool would scale better to thousands of routes.
            let forwarder = move || {
                loop {
                    // the stop flag ends the thread when Stop cannot be queued, the Reader callbacks keep the channel open
                    match fwd_queue.rx.recv_timeout(Duration::from_secs(10)) {
                        Ok(FwdCmd::Msg { payload, volatile }) => {
                            let len = payload.len();
                            fwd_queue.bytes.fetch_sub(len, Ordering::Relaxed);
                            let (publisher, attachment) = {
                                let zp = zenoh_publisher.read().unwrap_or_else(|p| p.into_inner());
                                match (&zp.volatile_publisher, volatile) {
                                    (Some(p), true) => (p.clone(), Some(VOLATILE_ATTACHMENT)),
                                    (_, false) => (zp.publisher.clone(), None),
                                    // queued before its Reader was removed, the cache must not keep it
                                    (None, true) => continue,
                                }
                            };
                            if let Err(e) = publisher.put(payload).attachment(attachment).wait() {
                                tracing::error!("{route_id}: failed to route message: {e}");
                            } else {
                                fwd_msg_count.fetch_add(1, Ordering::Relaxed);
                                fwd_byte_count.fetch_add(len as u64, Ordering::Relaxed);
                            }
                        }
                        Ok(FwdCmd::Stop) | Err(flume::RecvTimeoutError::Disconnected) => break,
                        Err(flume::RecvTimeoutError::Timeout) => {
                            if fwd_stop.load(Ordering::Relaxed) {
                                break;
                            }
                        }
                    }
                }
                // the Reader callbacks keep the channel alive, so free the samples it still holds
                while fwd_queue.pop().is_some() {}
                tracing::debug!("{route_id}: forwarder thread terminated");
            };
            std::thread::Builder::new()
                .name("ros2dds-fwd".into())
                .spawn(forwarder)
                .map_err(|e| format!("Route Publisher ({ros2_name}): failed to spawn forwarder thread: {e}"))?;
        }

        let mut route = RoutePublisher {
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            context,
            zenoh_publisher,
            dds_reader: Arc::new(DDS_ENTITY_NULL.into()),
            dds_volatile_reader: Arc::new(DDS_ENTITY_NULL.into()),
            _matching_listener: None,
            priority,
            is_express,
            _type_info: type_info.clone(),
            keyless,
            reader_qos: Arc::new(Mutex::new(reader_qos)),
            volatile_reader_qos: Arc::new(Mutex::new(None)),
            fixed_qos,
            fallback_qos,
            announced_qos: None,
            volatile_announcement: None,
            warned_mixed_writers: false,
            liveliness_token: None,
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
            fwd_msg_count,
            fwd_byte_count,
            dropped_msg_count,
            fwd_queue,
            fwd_stop,
            activation_lock: Arc::new(Mutex::new(())),
            is_matching: Arc::new(AtomicBool::new(false)),
        };
        route.declare_matching_listener().await?;
        Ok(route)
    }

    fn current_publisher(&self) -> Arc<AdvancedPublisher<'static>> {
        self.zenoh_publisher
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .publisher
            .clone()
    }

    // activate/deactivate DDS Reader on detection/undetection of matching Subscribers
    // (copy/move all required args for the callback)
    async fn declare_matching_listener(&mut self) -> Result<(), String> {
        // NOT backgrounded: the handle is stored in the RoutePublisher so it is
        // undeclared on Drop (see field doc).
        let publisher = self.current_publisher();
        let matching_listener = publisher
            .matching_listener()
            .callback({
                let route_id = self.to_string();
                let dds_reader = self.dds_reader.clone();
                let dds_volatile_reader = self.dds_volatile_reader.clone();
                let ros2_name = self.ros2_name.clone();
                let ros2_type = self.ros2_type.clone();
                let context = self.context.clone();
                let reader_qos = self.reader_qos.clone();
                let volatile_reader_qos = self.volatile_reader_qos.clone();
                let type_info = self._type_info.clone();
                let keyless = self.keyless;
                let fwd_queue = self.fwd_queue.clone();
                let activation_lock = self.activation_lock.clone();
                let is_matching = self.is_matching.clone();

                move |status| {
                    tracing::debug!("{route_id} MatchingStatus changed: {status:?}");
                    let _guard = activation_lock.lock().unwrap_or_else(|p| p.into_inner());
                    // under the lock: keeps the flag consistent with the
                    // activation state committed by this callback
                    is_matching.store(status.matching(), Ordering::Relaxed);
                    if status.matching() {
                        activate_dds_readers(
                            &dds_reader,
                            &dds_volatile_reader,
                            &reader_qos,
                            &volatile_reader_qos,
                            &ros2_name,
                            &ros2_type,
                            &route_id,
                            &context,
                            keyless,
                            &type_info,
                            &fwd_queue,
                        );
                    } else {
                        deactivate_dds_reader(&dds_reader, &route_id, &context.ros_discovery_mgr);
                        deactivate_dds_reader(
                            &dds_volatile_reader,
                            &route_id,
                            &context.ros_discovery_mgr,
                        );
                    }
                }
            })
            .await
            .map_err(|e| format!("Failed to listen of matching status changes: {e}",))?;
        self._matching_listener = Some(matching_listener);
        Ok(())
    }

    fn undeclare_matching_listener(&mut self) {
        if let Some(listener) = self._matching_listener.take() {
            if let Err(e) = listener.undeclare().wait_callbacks().wait() {
                tracing::debug!("{self}: error undeclaring matching listener: {e}");
            }
        }
    }

    fn deactivate_dds_reader(&mut self) {
        let route_id = self.to_string();
        let _guard = self
            .activation_lock
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        for reader in [&self.dds_reader, &self.dds_volatile_reader] {
            deactivate_dds_reader(reader, &route_id, &self.context.ros_discovery_mgr);
        }
    }

    /// Creates the missing DDS Readers if a remote subscriber matches.
    fn activate_if_matching(&self) {
        if !self.is_matching.load(Ordering::Relaxed) {
            return;
        }
        let _guard = self
            .activation_lock
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        activate_dds_readers(
            &self.dds_reader,
            &self.dds_volatile_reader,
            &self.reader_qos,
            &self.volatile_reader_qos,
            &self.ros2_name,
            &self.ros2_type,
            &self.to_string(),
            &self.context,
            self.keyless,
            &self._type_info,
            &self.fwd_queue,
        );
    }

    /// Topic routes: rebuilds the route when the Reader QoS derived from `local_writers` changes,
    /// and re-announces it when the announced QoS changes.
    pub async fn update_local_writers(&mut self, local_writers: &[Qos]) -> Result<(), String> {
        if self.fixed_qos {
            return Ok(());
        }
        if local_writers_disagree(local_writers) && !self.warned_mixed_writers {
            self.warned_mixed_writers = true;
            tracing::warn!(
                "{self}: local publishers of the same durability use different reliability QoS; \
                 the bridge reads them BEST_EFFORT"
            );
        }
        let multiplier = self.context.config.transient_local_cache_multiplier;
        // announce first, remote bridges drop marked samples until they have a VOLATILE Writer
        if !local_writers.is_empty() && self.liveliness_token.is_some() {
            let (announced, announced_volatile) =
                announced_qoses_for_local_writers(local_writers, multiplier);
            let changed = self
                .announced_qos
                .as_ref()
                .is_none_or(|a| !same_route_qos(a, &announced, false));
            if changed {
                // the new token replaces the old one only once declared, so remote bridges never see the route vanish
                self.announce_route(&announced).await?;
            }
            let current = self.volatile_announcement.take();
            self.volatile_announcement = match (current, announced_volatile) {
                (Some((qos, token)), Some(desired)) if same_route_qos(&qos, &desired, false) => {
                    Some((qos, token))
                }
                (_, Some(desired)) => {
                    let token = self.declare_liveliness_token(&desired).await?;
                    Some((desired, token))
                }
                (_, None) => None,
            };
        }
        let (desired, desired_volatile) =
            readers_qos_for_local_writers(local_writers, &self.fallback_qos, multiplier);
        let current = self
            .reader_qos
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if !same_route_qos(&current, &desired, true) {
            self.rebuild(desired, desired_volatile).await?;
        } else {
            let current_volatile = self
                .volatile_reader_qos
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            let same_volatile = match (&current_volatile, &desired_volatile) {
                (Some(a), Some(b)) => same_route_qos(a, b, true),
                (a, b) => a.is_none() && b.is_none(),
            };
            if !same_volatile {
                self.set_volatile_reader(desired_volatile).await?;
            }
        }
        Ok(())
    }

    /// Recreates zenoh publisher, matching listener and DDS Reader for `reader_qos`.
    /// A TRANSIENT_LOCAL Reader re-reads the local history, so matched remote subscribers may get it twice.
    async fn rebuild(
        &mut self,
        reader_qos: Qos,
        volatile_reader_qos: Option<Qos>,
    ) -> Result<(), String> {
        tracing::info!("{self}: local publishers' QoS changed, rebuilding route");
        let cache_size = tl_cache_size(&self.ros2_name, &reader_qos, self.keyless, 1);
        let publisher = declare_zenoh_publisher(
            &self.ros2_name,
            &self.zenoh_key_expr,
            &self.context,
            &reader_qos,
            cache_size,
            self.priority,
            self.is_express,
        )
        .await?;
        let volatile_publisher = self.declare_volatile_publisher(&volatile_reader_qos).await?;
        // stop reacting to the old publisher's matching, drop the old Readers
        self.undeclare_matching_listener();
        self.deactivate_dds_reader();
        let old_qos = std::mem::replace(
            &mut *self.reader_qos.lock().unwrap_or_else(|p| p.into_inner()),
            reader_qos,
        );
        let old_volatile_qos = std::mem::replace(
            &mut *self
                .volatile_reader_qos
                .lock()
                .unwrap_or_else(|p| p.into_inner()),
            volatile_reader_qos,
        );
        self.fwd_queue
            .limit
            .store(queue_limit_for(cache_size), Ordering::Relaxed);
        let old = std::mem::replace(
            &mut *self
                .zenoh_publisher
                .write()
                .unwrap_or_else(|p| p.into_inner()),
            ZPublisher {
                publisher,
                cache_size,
                volatile_publisher,
            },
        );
        drop(old);
        self.is_matching.store(false, Ordering::Relaxed);
        if let Err(e) = self.declare_matching_listener().await {
            // keep the old QoS so that the next update retries the rebuild
            *self.reader_qos.lock().unwrap_or_else(|p| p.into_inner()) = old_qos;
            *self
                .volatile_reader_qos
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = old_volatile_qos;
            return Err(e);
        }
        // zenoh reports the current matching status during the declaration
        self.activate_if_matching();
        Ok(())
    }

    async fn declare_volatile_publisher(
        &self,
        volatile_reader_qos: &Option<Qos>,
    ) -> Result<Option<Arc<AdvancedPublisher<'static>>>, String> {
        match volatile_reader_qos {
            Some(qos) => Ok(Some(
                declare_zenoh_publisher(
                    &self.ros2_name,
                    &self.zenoh_key_expr,
                    &self.context,
                    qos,
                    0,
                    self.priority,
                    self.is_express,
                )
                .await?,
            )),
            None => Ok(None),
        }
    }

    /// Adds, replaces or removes the VOLATILE Reader of a topic with mixed durability.
    /// The TRANSIENT_LOCAL Reader and the zenoh cache stay, so late subscribers still get the history.
    async fn set_volatile_reader(
        &mut self,
        volatile_reader_qos: Option<Qos>,
    ) -> Result<(), String> {
        tracing::info!("{self}: VOLATILE local publishers changed, replacing their Reader");
        let volatile_publisher = self.declare_volatile_publisher(&volatile_reader_qos).await?;
        {
            let _guard = self
                .activation_lock
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            deactivate_dds_reader(
                &self.dds_volatile_reader,
                &self.to_string(),
                &self.context.ros_discovery_mgr,
            );
            *self
                .volatile_reader_qos
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = volatile_reader_qos;
            self.zenoh_publisher
                .write()
                .unwrap_or_else(|p| p.into_inner())
                .volatile_publisher = volatile_publisher;
        }
        self.activate_if_matching();
        Ok(())
    }

    async fn announce_route(&mut self, discovered_writer_qos: &Qos) -> Result<(), String> {
        // only if not for an Action (since actions declare their own liveliness)
        if !is_message_for_action(&self.ros2_name) {
            // an existing token is undeclared only once the new one is declared
            self.liveliness_token =
                Some(self.declare_liveliness_token(discovered_writer_qos).await?);
            self.announced_qos = Some(discovered_writer_qos.clone());
        }
        Ok(())
    }

    async fn declare_liveliness_token(&self, writer_qos: &Qos) -> Result<LivelinessToken, String> {
        let liveliness_ke = new_ke_liveliness_pub(
            &self.context.zsession.zid().into_keyexpr(),
            &self.zenoh_key_expr,
            &self.ros2_type,
            self.keyless,
            writer_qos,
        )?;
        self.context
            .zsession
            .liveliness()
            .declare_token(liveliness_ke)
            .await
            .map_err(|e| {
                format!(
                    "Failed create LivelinessToken associated to route for Publisher {}: {e}",
                    self.ros2_name
                )
            })
    }

    fn retire_route(&mut self) {
        self.liveliness_token = None;
        self.announced_qos = None;
        self.volatile_announcement = None;
    }

    #[inline]
    pub fn add_remote_route(&mut self, zenoh_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .insert(format!("{zenoh_id}:{zenoh_key_expr}"));
        tracing::debug!("{self} now serving remote routes {:?}", self.remote_routes);

        // Reconnect fallback (same pattern as RouteServiceCli): if a late-
        // processed retire/prune deactivated the DDS Reader AFTER the matching
        // listener already fired true, no further transition will ever fire
        // and the route stayed "listed but dead". Re-activate here.
        self.activate_if_matching();
    }

    #[inline]
    pub fn remove_remote_route(&mut self, zenoh_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .remove(&format!("{zenoh_id}:{zenoh_key_expr}"));
        tracing::debug!("{self} now serving remote routes {:?}", self.remote_routes);
        // if last remote route removed, deactivate the DDS Reader
        if self.remote_routes.is_empty() {
            self.deactivate_dds_reader();
        }
    }

    /// Remove all remote_routes entries for a departed bridge (prefix
    /// "<zenoh_id>:"), deactivating the DDS Reader if this leaves the route
    /// serving no remote route. Returns whether any entry was removed.
    #[inline]
    pub fn prune_remote_routes_with_prefix(&mut self, prefix: &str) -> bool {
        let before = self.remote_routes.len();
        self.remote_routes.retain(|r| !r.starts_with(prefix));
        if self.remote_routes.len() == before {
            return false;
        }
        tracing::debug!("{self} now serving remote routes {:?}", self.remote_routes);
        if self.remote_routes.is_empty() {
            self.deactivate_dds_reader();
        }
        true
    }

    #[inline]
    pub fn is_serving_remote_route(&self) -> bool {
        !self.remote_routes.is_empty()
    }

    /// Announces the route with `announce_qos` when the first local node is added.
    #[inline]
    pub async fn add_local_node(&mut self, node: String, announce_qos: &Qos) {
        if self.local_nodes.insert(node) {
            tracing::debug!("{self} now serving local nodes {:?}", self.local_nodes);
            // if 1st local node added, announce the route
            if self.local_nodes.len() == 1 {
                if let Err(e) = self.announce_route(announce_qos).await {
                    tracing::error!("{self} announcement failed: {e}");
                }
            }
        }
    }

    #[inline]
    pub fn remove_local_node(&mut self, node: &str) {
        if self.local_nodes.remove(node) {
            tracing::debug!("{self} now serving local nodes {:?}", self.local_nodes);
            // if last local node removed, retire the route
            if self.local_nodes.is_empty() {
                self.retire_route();
            }
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

fn queue_limit_for(cache_size: usize) -> usize {
    FWD_QUEUE_SIZE.max(cache_size.min(FWD_QUEUE_MAX))
}

// create the zenoh Publisher
// if Reader shall be TRANSIENT_LOCAL, use a PublicationCache to store historical messages
async fn declare_zenoh_publisher(
    ros2_name: &str,
    zenoh_key_expr: &OwnedKeyExpr,
    context: &Context,
    reader_qos: &Qos,
    cache_size: usize,
    priority: Priority,
    is_express: bool,
) -> Result<Arc<AdvancedPublisher<'static>>, String> {
    // CongestionControl to be used when re-publishing over zenoh: Blocking if Writer is RELIABLE (since we don't know what is remote Reader's QoS)
    let congestion_ctrl = match (
        context.config.reliable_routes_blocking,
        is_reliable(reader_qos),
    ) {
        (true, true) => CongestionControl::Block,
        _ => CongestionControl::Drop,
    };
    tracing::debug!(
        "Route Publisher ({ros2_name} -> {zenoh_key_expr}): congestion_ctrl {:?}, priority {:?}, express:{}",
        congestion_ctrl,
        priority,
        is_express
    );

    let mut publisher_builder = context
        .zsession
        .declare_publisher(zenoh_key_expr.clone())
        .advanced();
    if is_transient_local(reader_qos) {
        publisher_builder = publisher_builder
            .cache(CacheConfig::default().max_samples(cache_size))
            .publisher_detection();
    }

    Ok(Arc::new(
        publisher_builder
            .reliability(Reliability::Reliable)
            .allowed_destination(Locality::Remote)
            .congestion_control(congestion_ctrl)
            .express(is_express)
            .priority(priority)
            .await
            .map_err(|e| format!("Failed create Publisher for key {zenoh_key_expr}: {e}",))?,
    ))
}

fn serialize_reader_qos<S>(qos: &Arc<Mutex<Qos>>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&route_qos_summary(&qos.lock().unwrap_or_else(|p| p.into_inner())))
}

fn serialize_volatile_reader_qos<S>(qos: &Arc<Mutex<Option<Qos>>>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match &*qos.lock().unwrap_or_else(|p| p.into_inner()) {
        Some(qos) => s.serialize_str(&route_qos_summary(qos)),
        None => s.serialize_none(),
    }
}

pub fn serialize_pub_cache<S>(zpub: &Arc<RwLock<ZPublisher>>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let size = zpub.read().unwrap_or_else(|p| p.into_inner()).cache_size;
    s.serialize_u64(size as u64)
}

fn serialize_priority<S>(p: &Priority, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_u8(*p as u8)
}

// Return the read period if name matches one of the "pub_max_frequencies" option
fn get_read_period(config: &Config, ros2_name: &str) -> Option<Duration> {
    config
        .get_pub_max_frequencies(ros2_name)
        .map(|f| Duration::from_secs_f32(1f32 / f))
}

#[allow(clippy::too_many_arguments)]
fn activate_dds_reader(
    dds_reader: &Arc<AtomicDDSEntity>,
    ros2_name: &str,
    ros2_type: &str,
    route_id: &str,
    context: &Context,
    keyless: bool,
    reader_qos: &Qos,
    type_info: &Option<Arc<TypeInfo>>,
    fwd_queue: &FwdQueue,
    volatile: bool,
) -> Result<(), String> {
    tracing::debug!("{route_id}: create Reader with {reader_qos:?}");
    let topic_name: String = format!("rt{}", ros2_name);
    let type_name = ros2_message_type_to_dds_type(ros2_type);
    let read_period = get_read_period(&context.config, ros2_name);

    // create matching DDS Reader that forwards message coming from DDS to Zenoh
    let reader = create_dds_reader(
        context.participant,
        topic_name.clone(),
        type_name,
        type_info,
        keyless,
        reader_qos.clone(),
        read_period,
        volatile,
        {
            let route_id = route_id.to_string();
            let fwd_queue = fwd_queue.clone();
            move |sample: &DDSRawSample| {
                // NEVER block Cyclone's shared delivery thread: enqueue only.
                if *LOG_PAYLOAD {
                    tracing::debug!("{route_id}: routing message - payload: {:02x?}", sample);
                } else {
                    tracing::trace!("{route_id}: routing message - {} bytes", sample.len());
                }
                if fwd_queue.push(sample.into(), volatile) > 0 {
                    let n = fwd_queue.dropped.load(Ordering::Relaxed);
                    if n == 1 || n % 100 == 0 {
                        tracing::warn!(
                            "{route_id}: forward queue full - {n} message(s) dropped \
                             since activation (zenoh side congested)"
                        );
                    }
                }
            }
        },
    )?;
    // get_guid can fail under participant churn, then the reader is deleted before it is published
    let reader_gid = match get_guid(&reader) {
        Ok(gid) => gid,
        Err(e) => {
            let _ = delete_dds_entity(reader);
            return Err(e);
        }
    };
    context.ros_discovery_mgr.add_dds_reader(reader_gid);
    let old = dds_reader.deref().swap(reader, Ordering::Relaxed);

    if old != DDS_ENTITY_NULL {
        tracing::warn!("{route_id}: on activation their was already a DDS Reader - overwrite it");
        if let Err(e) = delete_dds_entity(old) {
            tracing::warn!("{route_id}: failed to delete overwritten DDS Reader: {e}");
        }
    }

    Ok(())
}

/// Creates whichever Reader of a route is missing, the caller holds `activation_lock`.
/// The TRANSIENT_LOCAL Reader comes first, so the VOLATILE one can leave its Writers to it.
#[allow(clippy::too_many_arguments)]
fn activate_dds_readers(
    dds_reader: &Arc<AtomicDDSEntity>,
    dds_volatile_reader: &Arc<AtomicDDSEntity>,
    reader_qos: &Mutex<Qos>,
    volatile_reader_qos: &Mutex<Option<Qos>>,
    ros2_name: &str,
    ros2_type: &str,
    route_id: &str,
    context: &Context,
    keyless: bool,
    type_info: &Option<Arc<TypeInfo>>,
    fwd_queue: &FwdQueue,
) {
    let volatile_qos = volatile_reader_qos
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    let readers = [
        (dds_reader, Some(reader_qos.lock().unwrap_or_else(|p| p.into_inner()).clone()), false),
        (dds_volatile_reader, volatile_qos, true),
    ];
    for (reader, qos, volatile) in readers {
        let Some(qos) = qos else { continue };
        if reader.load(Ordering::Relaxed) != DDS_ENTITY_NULL {
            continue;
        }
        if let Err(e) = activate_dds_reader(
            reader,
            ros2_name,
            ros2_type,
            route_id,
            context,
            keyless,
            &qos,
            type_info,
            fwd_queue,
            volatile,
        ) {
            tracing::error!("{route_id}: failed to activate DDS Reader: {e}");
        }
    }
}

fn deactivate_dds_reader(
    dds_reader: &Arc<AtomicDDSEntity>,
    route_id: &str,
    ros_discovery_mgr: &Arc<RosDiscoveryInfoMgr>,
) {
    tracing::debug!("{route_id}: delete Reader");
    let reader = dds_reader.swap(DDS_ENTITY_NULL, Ordering::Relaxed);
    if reader != DDS_ENTITY_NULL {
        // remove reader's GID from ros_discovery_info message
        match get_guid(&reader) {
            Ok(gid) => ros_discovery_mgr.remove_dds_reader(gid),
            Err(e) => tracing::warn!("{route_id}: {e}"),
        }
        if let Err(e) = delete_dds_entity(reader) {
            tracing::warn!("{route_id}: error deleting DDS Reader:  {e}");
        }
    }
}

pub fn serialize_arc_atomic_u64<S>(v: &Arc<AtomicU64>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_u64(v.load(Ordering::Relaxed))
}

#[cfg(test)]
mod tl_cache_tests {
    use cyclors::qos::{Durability, DurabilityKind, History};

    use super::*;

    fn tl(depth: i32) -> Qos {
        Qos {
            durability: Some(Durability {
                kind: DurabilityKind::TRANSIENT_LOCAL,
            }),
            history: Some(History {
                kind: HistoryKind::KEEP_LAST,
                depth,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn keyless_topic_caches_depth_times_multiplier() {
        assert_eq!(tl_cache_size("/tf_static", &tl(20), true, 1), 20);
        assert_eq!(tl_cache_size("/map", &tl(3), true, 10), 30);
    }

    #[test]
    fn volatile_route_has_no_cache() {
        assert_eq!(tl_cache_size("/x", &Qos::default(), true, 25), 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn first_byte(cmd: Option<FwdCmd>) -> u8 {
        match cmd {
            Some(FwdCmd::Msg { payload, .. }) => payload.to_bytes()[0],
            _ => panic!("expected a queued sample"),
        }
    }

    #[test]
    fn fwd_queue_evicts_oldest_beyond_byte_budget() {
        let q = FwdQueue::new(FWD_QUEUE_SIZE);
        let len = FWD_QUEUE_BYTES / 3;
        for i in 0..4u8 {
            q.push(ZBytes::from(vec![i; len]), false);
        }
        assert_eq!(q.dropped.load(Ordering::Relaxed), 1);
        assert_eq!(q.bytes.load(Ordering::Relaxed), 3 * len);
        assert_eq!(first_byte(q.pop()), 1);
        assert_eq!(q.bytes.load(Ordering::Relaxed), 2 * len);
    }

    #[test]
    fn fwd_queue_evicts_oldest_beyond_limit() {
        let q = FwdQueue::new(FWD_QUEUE_SIZE);
        for i in 0..FWD_QUEUE_SIZE + 10 {
            q.push(ZBytes::from(vec![i as u8]), false);
        }
        assert_eq!(q.dropped.load(Ordering::Relaxed), 10);
        assert_eq!(q.tx.len(), FWD_QUEUE_SIZE);
        assert_eq!(first_byte(q.pop()), 10);
    }

    #[test]
    fn fwd_queue_keeps_a_sample_larger_than_the_budget() {
        let q = FwdQueue::new(FWD_QUEUE_SIZE);
        q.push(ZBytes::from(vec![7; FWD_QUEUE_BYTES + 1]), false);
        assert_eq!(q.dropped.load(Ordering::Relaxed), 0);
        assert_eq!(first_byte(q.pop()), 7);
        assert_eq!(q.bytes.load(Ordering::Relaxed), 0);
    }
}
