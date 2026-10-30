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
// Capacity of the forward channel, a flume channel cannot grow so `queue_limit` bounds each route.
const FWD_QUEUE_MAX: usize = 4096;

// Item of the per-route forward queue.
enum FwdCmd {
    Msg(ZBytes),
    Stop,
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
    // the MatchingListener activating/deactivating the DDS Reader on remote
    // subscriber (un)matching. Kept here (NOT backgrounded) so it is undeclared
    // on Drop — otherwise its callback's clone of the zenoh Publisher forms a
    // reference cycle, leaking a Reader per route re-creation and duplicating
    // forwarded messages.
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
    // true for action routes (fixed protocol QoS)
    #[serde(skip)]
    fixed_qos: bool,
    // type hash source of the placeholder Reader used while no local Writer is known
    #[serde(skip)]
    fallback_qos: Qos,
    // QoS announced in the current liveliness token (if any)
    #[serde(skip)]
    announced_qos: Option<Qos>,
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
    // count of messages dropped because the forward queue was full (i.e. the
    // zenoh side could not keep up - congested link)
    #[serde(serialize_with = "serialize_arc_atomic_u64")]
    dropped_msg_count: Arc<AtomicU64>,
    // sender side of the per-route forward queue; the DDS Reader callback
    // enqueues here, a dedicated forwarder task performs the (possibly
    // blocking) zenoh put. See create() for the rationale.
    #[serde(skip)]
    fwd_tx: flume::Sender<FwdCmd>,
    // effective forward queue limit (see FWD_QUEUE_MAX)
    #[serde(skip)]
    queue_limit: Arc<AtomicUsize>,
    // Set on Drop; the forwarder task checks it on its periodic wakeup, so
    // termination is guaranteed even when FwdCmd::Stop cannot be enqueued.
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
        // Stop the forwarder task: flag first (checked on its periodic wakeup,
        // guaranteed path), then a best-effort sentinel for prompt shutdown.
        // Both are needed because cyclors leaks the reader-callback closure,
        // which owns a Sender clone - the channel never disconnects.
        self.fwd_stop.store(true, Ordering::Relaxed);
        let _ = self.fwd_tx.try_send(FwdCmd::Stop);
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
    // the route may serve several Writers
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

    /// Topic route whose Reader QoS follows `local_writers`, see reader_qos_for_local_writers.
    pub async fn create_for_topic(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        keyless: bool,
        local_writers: &[Qos],
        fallback_qos: Qos,
        context: Context,
    ) -> Result<RoutePublisher, String> {
        let reader_qos = reader_qos_for_local_writers(local_writers, &fallback_qos);
        Self::new(
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
        .await
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

        let cache_size = tl_cache_size(
            &ros2_name,
            &reader_qos,
            keyless,
            context.config.transient_local_cache_multiplier,
        );
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
        }));

        // traffic counters (incremented by the forwarder task on each
        // successful forward; surfaced in the admin space)
        let fwd_msg_count = Arc::new(AtomicU64::new(0));
        let fwd_byte_count = Arc::new(AtomicU64::new(0));
        let dropped_msg_count = Arc::new(AtomicU64::new(0));

        // Decouple the DDS Reader callback from zenoh backpressure. The DDS
        // listener callback runs on Cyclone's SINGLE per-domain delivery thread
        // ("dq.user"), shared by EVERY route of this bridge: a publisher.put()
        // blocking there on a congested link stalled all DDS->Zenoh forwarding
        // bridge-wide (and route teardown, via dds_delete waiting for the
        // callback). The callback now only copies the sample into this bounded
        // queue; the forwarder task below performs the (possibly blocking) put.
        // When the queue is full the sample is DROPPED and counted - a bounded,
        // observable degradation instead of a bridge-wide wedge.
        //
        // TRANSIENT_LOCAL routes queue as much as they cache, so the history burst at activation fits
        let queue_limit = Arc::new(AtomicUsize::new(queue_limit_for(cache_size)));
        let (fwd_tx, fwd_rx) = flume::bounded::<FwdCmd>(FWD_QUEUE_MAX);
        let fwd_stop = Arc::new(AtomicBool::new(false));
        {
            let zenoh_publisher = zenoh_publisher.clone();
            let fwd_msg_count = fwd_msg_count.clone();
            let fwd_byte_count = fwd_byte_count.clone();
            let route_id = format!("Route Publisher (ROS:{ros2_name} -> Zenoh:{zenoh_key_expr})");
            let fwd_stop = fwd_stop.clone();
            tokio::task::spawn(async move {
                loop {
                    // Bounded wait + stop flag: the FwdCmd::Stop sentinel alone
                    // is not guaranteed to be deliverable (the queue may be full
                    // at Drop, and the leaked cyclors callback closure keeps a
                    // Sender alive so the channel never disconnects). Without
                    // this, a torn-down route leaked its task + zenoh publisher.
                    match tokio::time::timeout(Duration::from_secs(10), fwd_rx.recv_async()).await {
                        Ok(Ok(FwdCmd::Msg(payload))) => {
                            let len = payload.len();
                            let publisher = zenoh_publisher
                                .read()
                                .unwrap_or_else(|p| p.into_inner())
                                .publisher
                                .clone();
                            if let Err(e) = publisher.put(payload).await {
                                tracing::error!("{route_id}: failed to route message: {e}");
                            } else {
                                fwd_msg_count.fetch_add(1, Ordering::Relaxed);
                                fwd_byte_count.fetch_add(len as u64, Ordering::Relaxed);
                            }
                        }
                        Ok(Ok(FwdCmd::Stop)) | Ok(Err(_)) => break,
                        Err(_timeout) => {
                            if fwd_stop.load(Ordering::Relaxed) {
                                break;
                            }
                        }
                    }
                }
                tracing::debug!("{route_id}: forwarder task terminated");
            });
        }

        let mut route = RoutePublisher {
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            context,
            zenoh_publisher,
            dds_reader: Arc::new(DDS_ENTITY_NULL.into()),
            _matching_listener: None,
            priority,
            is_express,
            _type_info: type_info.clone(),
            keyless,
            reader_qos: Arc::new(Mutex::new(reader_qos)),
            fixed_qos,
            fallback_qos,
            announced_qos: None,
            warned_mixed_writers: false,
            liveliness_token: None,
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
            fwd_msg_count,
            fwd_byte_count,
            dropped_msg_count,
            fwd_tx,
            queue_limit,
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
                let ros2_name = self.ros2_name.clone();
                let ros2_type = self.ros2_type.clone();
                let context = self.context.clone();
                let reader_qos = self.reader_qos.clone();
                let type_info = self._type_info.clone();
                let keyless = self.keyless;
                let fwd_tx = self.fwd_tx.clone();
                let queue_limit = self.queue_limit.clone();
                let dropped_msg_count = self.dropped_msg_count.clone();
                let activation_lock = self.activation_lock.clone();
                let is_matching = self.is_matching.clone();

                move |status| {
                    tracing::debug!("{route_id} MatchingStatus changed: {status:?}");
                    let _guard = activation_lock.lock().unwrap_or_else(|p| p.into_inner());
                    // under the lock: keeps the flag consistent with the
                    // activation state committed by this callback
                    is_matching.store(status.matching(), Ordering::Relaxed);
                    // a Reader may already be active, re-activated by add_remote_route
                    if status.matching() && dds_reader.load(Ordering::Relaxed) == DDS_ENTITY_NULL {
                        let qos = reader_qos.lock().unwrap_or_else(|p| p.into_inner()).clone();
                        if let Err(e) = activate_dds_reader(
                            &dds_reader,
                            &ros2_name,
                            &ros2_type,
                            &route_id,
                            &context,
                            keyless,
                            &qos,
                            &type_info,
                            &fwd_tx,
                            &queue_limit,
                            &dropped_msg_count,
                        ) {
                            tracing::error!("{route_id}: failed to activate DDS Reader: {e}");
                        }
                    } else {
                        deactivate_dds_reader(&dds_reader, &route_id, &context.ros_discovery_mgr)
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
        let _guard = self
            .activation_lock
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let dds_reader = self.dds_reader.swap(DDS_ENTITY_NULL, Ordering::Relaxed);
        if dds_reader != DDS_ENTITY_NULL {
            // remove reader's GID from ros_discovery_info message
            match get_guid(&dds_reader) {
                Ok(gid) => self.context.ros_discovery_mgr.remove_dds_reader(gid),
                Err(e) => tracing::warn!("{self}: {e}"),
            }
            if let Err(e) = delete_dds_entity(dds_reader) {
                tracing::warn!("{}: error deleting DDS Reader:  {}", self, e);
            }
        }
    }

    /// Creates the DDS Reader if a remote subscriber matches but there is none.
    fn activate_if_matching(&self) {
        if !self.is_matching.load(Ordering::Relaxed)
            || self.dds_reader.load(Ordering::Relaxed) != DDS_ENTITY_NULL
        {
            return;
        }
        let route_id = self.to_string();
        let _guard = self
            .activation_lock
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if self.dds_reader.load(Ordering::Relaxed) != DDS_ENTITY_NULL {
            return; // the listener won the race meanwhile
        }
        let qos = self
            .reader_qos
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if let Err(e) = activate_dds_reader(
            &self.dds_reader,
            &self.ros2_name,
            &self.ros2_type,
            &route_id,
            &self.context,
            self.keyless,
            &qos,
            &self._type_info,
            &self.fwd_tx,
            &self.queue_limit,
            &self.dropped_msg_count,
        ) {
            tracing::error!("{self}: failed to (re-)activate DDS Reader: {e}");
        }
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
                "{self}: local publishers use different reliability/durability QoS; the bridge \
                 reads them with the weakest common QoS (no TRANSIENT_LOCAL history / no \
                 reliability across the bridge for this topic)"
            );
        }
        let desired = reader_qos_for_local_writers(local_writers, &self.fallback_qos);
        let current = self
            .reader_qos
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if !same_route_qos(&current, &desired, true) {
            self.rebuild(desired).await?;
        }
        if !local_writers.is_empty() && self.liveliness_token.is_some() {
            let announced = announced_qos_for_local_writers(local_writers);
            let changed = self
                .announced_qos
                .as_ref()
                .is_none_or(|a| !same_route_qos(a, &announced, false));
            if changed {
                // the new token replaces the old one only once declared, so remote bridges never see the route vanish
                self.announce_route(&announced).await?;
            }
        }
        Ok(())
    }

    /// Recreates zenoh publisher, matching listener and DDS Reader for `reader_qos`.
    /// A TRANSIENT_LOCAL Reader re-reads the local history, so matched remote subscribers may get it twice.
    async fn rebuild(&mut self, reader_qos: Qos) -> Result<(), String> {
        tracing::info!("{self}: local publishers' QoS changed, rebuilding route");
        let cache_size = tl_cache_size(
            &self.ros2_name,
            &reader_qos,
            self.keyless,
            self.context.config.transient_local_cache_multiplier,
        );
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
        // stop reacting to the old publisher's matching, drop the old Reader
        self.undeclare_matching_listener();
        self.deactivate_dds_reader();
        let old_qos = std::mem::replace(
            &mut *self.reader_qos.lock().unwrap_or_else(|p| p.into_inner()),
            reader_qos,
        );
        self.queue_limit
            .store(queue_limit_for(cache_size), Ordering::Relaxed);
        let old = std::mem::replace(
            &mut *self
                .zenoh_publisher
                .write()
                .unwrap_or_else(|p| p.into_inner()),
            ZPublisher {
                publisher,
                cache_size,
            },
        );
        drop(old);
        self.is_matching.store(false, Ordering::Relaxed);
        if let Err(e) = self.declare_matching_listener().await {
            // keep the old QoS so that the next update retries the rebuild
            *self.reader_qos.lock().unwrap_or_else(|p| p.into_inner()) = old_qos;
            return Err(e);
        }
        // zenoh reports the current matching status during the declaration
        self.activate_if_matching();
        Ok(())
    }

    async fn announce_route(&mut self, discovered_writer_qos: &Qos) -> Result<(), String> {
        // only if not for an Action (since actions declare their own liveliness)
        if !is_message_for_action(&self.ros2_name) {
            // create associated LivelinessToken
            let liveliness_ke = new_ke_liveliness_pub(
                &self.context.zsession.zid().into_keyexpr(),
                &self.zenoh_key_expr,
                &self.ros2_type,
                self.keyless,
                discovered_writer_qos,
            )?;
            let ros2_name = self.ros2_name.clone();
            // an existing token is undeclared only once the new one is declared
            self.liveliness_token = Some(self.context.zsession
                .liveliness()
                .declare_token(liveliness_ke)
                .await
                .map_err(|e| {
                    format!(
                        "Failed create LivelinessToken associated to route for Publisher {ros2_name}: {e}"
                    )
                })?
            );
            self.announced_qos = Some(discovered_writer_qos.clone());
        }
        Ok(())
    }

    fn retire_route(&mut self) {
        self.liveliness_token = None;
        self.announced_qos = None;
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
    fwd_tx: &flume::Sender<FwdCmd>,
    queue_limit: &Arc<AtomicUsize>,
    dropped_msg_count: &Arc<AtomicU64>,
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
        {
            let route_id = route_id.to_string();
            let fwd_tx = fwd_tx.clone();
            let queue_limit = queue_limit.clone();
            let dropped_msg_count = dropped_msg_count.clone();
            move |sample: &DDSRawSample| {
                // NEVER block Cyclone's shared delivery thread: enqueue only.
                if *LOG_PAYLOAD {
                    tracing::debug!("{route_id}: routing message - payload: {:02x?}", sample);
                } else {
                    tracing::trace!("{route_id}: routing message - {} bytes", sample.len());
                }
                let res = if fwd_tx.len() >= queue_limit.load(Ordering::Relaxed) {
                    Err(flume::TrySendError::Full(()))
                } else {
                    fwd_tx.try_send(FwdCmd::Msg(sample.into())).map_err(|e| match e {
                        flume::TrySendError::Full(_) => flume::TrySendError::Full(()),
                        flume::TrySendError::Disconnected(_) => flume::TrySendError::Disconnected(()),
                    })
                };
                match res {
                    Ok(()) => {}
                    Err(flume::TrySendError::Full(_)) => {
                        let n = dropped_msg_count.fetch_add(1, Ordering::Relaxed) + 1;
                        if n == 1 || n % 100 == 0 {
                            tracing::warn!(
                                "{route_id}: forward queue full - {n} message(s) dropped \
                                 since activation (zenoh side congested)"
                            );
                        }
                    }
                    Err(flume::TrySendError::Disconnected(_)) => {}
                }
            }
        },
    )?;
    // Read back the GUID before committing the reader into the atomic; on failure
    // (e.g. transient BAD_PARAMETER under participant churn) delete the reader and
    // return Err with the atomic left untouched — avoiding a committed-but-dead
    // reader that would leak and leave the route "listed but dead".
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
        assert_eq!(tl_cache_size("/tf_static", &tl(1), true, 25), 25);
        assert_eq!(tl_cache_size("/map", &tl(3), true, 25), 75);
    }

    #[test]
    fn volatile_route_has_no_cache() {
        assert_eq!(tl_cache_size("/x", &Qos::default(), true, 25), 0);
    }
}
