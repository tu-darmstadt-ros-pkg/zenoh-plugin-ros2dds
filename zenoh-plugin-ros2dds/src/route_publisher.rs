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
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
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
    // the zenoh publisher used to re-publish to zenoh the message received by the DDS Reader
    // `None` when route is created on a remote announcement and no local ROS2 Subscriber discovered yet
    #[serde(
        rename = "publication_cache_size",
        serialize_with = "serialize_pub_cache"
    )]
    zenoh_publisher: ZPublisher,
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
    // TypeInfo for Reader creation (if available)
    #[serde(skip)]
    _type_info: Option<Arc<TypeInfo>>,
    // if the topic is keyless
    #[serde(skip)]
    keyless: bool,
    // the QoS for the DDS Reader to be created.
    // those are either the QoS announced by a remote bridge on a Reader discovery,
    // either the QoS adapted from a local discovered Writer
    #[serde(skip)]
    _reader_qos: Qos,
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
    // Set on Drop; the forwarder task checks it on its periodic wakeup, so
    // termination is guaranteed even when FwdCmd::Stop cannot be enqueued.
    #[serde(skip)]
    fwd_stop: Arc<AtomicBool>,
    // Serializes DDS Reader activation/deactivation between the matching-
    // listener callback (zenoh thread) and the routes-mgr paths
    // (add_remote_route reactivation, remove/prune deactivation).
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
        if let Some(listener) = self._matching_listener.take() {
            if let Err(e) = listener.undeclare().wait_callbacks().wait() {
                tracing::debug!("{self}: error undeclaring matching listener: {e}");
            }
        }
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

impl RoutePublisher {
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
        tracing::debug!(
            "Route Publisher ({ros2_name} -> {zenoh_key_expr}): creation with type {ros2_type}"
        );

        // create the zenoh Publisher
        // if Reader shall be TRANSIENT_LOCAL, use a PublicationCache to store historical messages
        let transient_local: bool = is_transient_local(&reader_qos);
        let cache_size: usize = if transient_local {
            #[allow(non_upper_case_globals)]
            let history_qos = get_history_or_default(&reader_qos);
            let durability_service_qos = get_durability_service_or_default(&reader_qos);
            let mut history = match (history_qos.kind, history_qos.depth) {
                (HistoryKind::KEEP_LAST, n) => {
                    if keyless {
                        // only 1 instance => history=n
                        n as usize
                    } else if durability_service_qos.max_instances == DDS_LENGTH_UNLIMITED {
                        // No limit! => history=MAX
                        usize::MAX
                    } else if durability_service_qos.max_instances > 0 {
                        // Compute cache size as history.depth * durability_service.max_instances
                        // This makes the assumption that the frequency of publication is the same for all instances...
                        // But as we have no way to have 1 cache per-instance, there is no other choice.
                        n.saturating_mul(durability_service_qos.max_instances) as usize
                    } else {
                        n as usize
                    }
                }
                (HistoryKind::KEEP_ALL, _) => usize::MAX,
            };
            // In case there are several Writers served by this route, increase the cache size
            history = history.saturating_mul(context.config.transient_local_cache_multiplier);
            tracing::debug!(
                "Route Publisher ({ros2_name} -> {zenoh_key_expr}): caching TRANSIENT_LOCAL publications with history={history} (computed from Reader's QoS: history=({:?},{}), durability_service.max_instances={})",
                history_qos.kind, history_qos.depth, durability_service_qos.max_instances
            );
            history
        } else {
            0
        };

        // CongestionControl to be used when re-publishing over zenoh: Blocking if Writer is RELIABLE (since we don't know what is remote Reader's QoS)
        let congestion_ctrl = match (
            context.config.reliable_routes_blocking,
            is_reliable(&reader_qos),
        ) {
            (true, true) => CongestionControl::Block,
            _ => CongestionControl::Drop,
        };

        // Priority if configured for this topic
        let (priority, is_express) = context
            .config
            .get_pub_priority_and_express(&ros2_name)
            .unwrap_or_default();
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
        if transient_local {
            publisher_builder = publisher_builder
                .cache(CacheConfig::default().max_samples(cache_size))
                .publisher_detection();
        }

        let publisher: Arc<AdvancedPublisher<'static>> = Arc::new(
            publisher_builder
                .reliability(Reliability::Reliable)
                .allowed_destination(Locality::Remote)
                .congestion_control(congestion_ctrl)
                .express(is_express)
                .priority(priority)
                .await
                .map_err(|e| format!("Failed create Publisher for key {zenoh_key_expr}: {e}",))?,
        );

        // activate/deactivate DDS Reader on detection/undetection of matching Subscribers
        // (copy/move all required args for the callback)
        let dds_reader: Arc<AtomicDDSEntity> = Arc::new(DDS_ENTITY_NULL.into());
        let activation_lock: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
        let is_matching: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

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
        // TRANSIENT_LOCAL routes get a queue at least as deep as their
        // publication cache so the historical burst delivered at reader
        // activation is not truncated at FWD_QUEUE_SIZE (capped at 4096).
        let queue_size = FWD_QUEUE_SIZE.max(cache_size.min(4096));
        let (fwd_tx, fwd_rx) = flume::bounded::<FwdCmd>(queue_size);
        let fwd_stop = Arc::new(AtomicBool::new(false));
        {
            let publisher = publisher.clone();
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

        // NOT backgrounded: the handle is stored in the RoutePublisher below so
        // it is undeclared on Drop (see field doc).
        let matching_listener = publisher
            .matching_listener()
            .callback({
                let dds_reader = dds_reader.clone();
                let ros2_name = ros2_name.clone();
                let ros2_type = ros2_type.clone();
                let zenoh_key_expr = zenoh_key_expr.clone();
                let route_id =
                    format!("Route Publisher (ROS:{ros2_name} -> Zenoh:{zenoh_key_expr})");
                let context = context.clone();
                let reader_qos = reader_qos.clone();
                let type_info = type_info.clone();
                let fwd_tx = fwd_tx.clone();
                let dropped_msg_count = dropped_msg_count.clone();
                let activation_lock = activation_lock.clone();
                let is_matching = is_matching.clone();

                move |status| {
                    tracing::debug!("{route_id} MatchingStatus changed: {status:?}");
                    let _guard = activation_lock.lock().unwrap_or_else(|p| p.into_inner());
                    // under the lock: keeps the flag consistent with the
                    // activation state committed by this callback
                    is_matching.store(status.matching(), Ordering::Relaxed);
                    if status.matching() {
                        if let Err(e) = activate_dds_reader(
                            &dds_reader,
                            &ros2_name,
                            &ros2_type,
                            &route_id,
                            &context,
                            keyless,
                            &reader_qos,
                            &type_info,
                            &fwd_tx,
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

        Ok(RoutePublisher {
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            context,
            zenoh_publisher: ZPublisher {
                publisher,
                cache_size,
            },
            dds_reader,
            _matching_listener: Some(matching_listener),
            priority,
            _type_info: type_info.clone(),
            _reader_qos: reader_qos,
            keyless,
            liveliness_token: None,
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
            fwd_msg_count,
            fwd_byte_count,
            dropped_msg_count,
            fwd_tx,
            fwd_stop,
            activation_lock,
            is_matching,
        })
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
        }
        Ok(())
    }

    fn retire_route(&mut self) {
        self.liveliness_token = None;
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
        if self.is_matching.load(Ordering::Relaxed)
            && self.dds_reader.load(Ordering::Relaxed) == DDS_ENTITY_NULL
        {
            let route_id = self.to_string();
            let _guard = self
                .activation_lock
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if self.dds_reader.load(Ordering::Relaxed) != DDS_ENTITY_NULL {
                return; // the listener won the race meanwhile
            }
            if let Err(e) = activate_dds_reader(
                &self.dds_reader,
                &self.ros2_name,
                &self.ros2_type,
                &route_id,
                &self.context,
                self.keyless,
                &self._reader_qos,
                &self._type_info,
                &self.fwd_tx,
                &self.dropped_msg_count,
            ) {
                tracing::error!("{self}: failed to re-activate DDS Reader: {e}");
            }
        }
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

    #[inline]
    pub async fn add_local_node(&mut self, node: String, discovered_writer_qos: &Qos) {
        if self.local_nodes.insert(node) {
            tracing::debug!("{self} now serving local nodes {:?}", self.local_nodes);
            // if 1st local node added, announce the route
            if self.local_nodes.len() == 1 {
                if let Err(e) = self.announce_route(discovered_writer_qos).await {
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

pub fn serialize_pub_cache<S>(zpub: &ZPublisher, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_u64(zpub.cache_size as u64)
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
            let dropped_msg_count = dropped_msg_count.clone();
            move |sample: &DDSRawSample| {
                // NEVER block Cyclone's shared delivery thread: enqueue only.
                if *LOG_PAYLOAD {
                    tracing::debug!("{route_id}: routing message - payload: {:02x?}", sample);
                } else {
                    tracing::trace!("{route_id}: routing message - {} bytes", sample.len());
                }
                match fwd_tx.try_send(FwdCmd::Msg(sample.into())) {
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
