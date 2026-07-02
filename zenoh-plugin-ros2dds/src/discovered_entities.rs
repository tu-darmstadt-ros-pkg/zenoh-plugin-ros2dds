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
    collections::HashMap,
    fmt::{self, Debug},
};

use zenoh::{
    bytes::{Encoding, ZBytes},
    key_expr::{
        format::{kedefine, keformat},
        keyexpr, OwnedKeyExpr,
    },
    query::Query,
};

use crate::{
    dds_discovery::{DdsEntity, DdsParticipant},
    events::ROS2DiscoveryEvent,
    gid::Gid,
    node_info::*,
    ros_discovery::{NodeEntitiesInfo, ParticipantEntitiesInfo},
};

/// Name PREFIX of the synthetic ROS nodes under which DDS writers that no real
/// ROS node declares in `ros_discovery_info` are attached, so they still get
/// routed (e.g. ros2_control's `controller_manager/activity` publisher, which
/// appears in DDS discovery as `_NODE_NAME_UNKNOWN_`).
///
/// One synthetic node PER PARTICIPANT (suffix = participant gid): a shared
/// fullname would collapse all participants' claims into one `local_nodes`
/// entry at the routes level, so the first departing participant tore down a
/// route other participants still needed (seen in production on /tf).
pub(crate) const ORPHAN_NODE_PREFIX: &str = "_zenoh_orphan_endpoints_";

/// A writer is only orphan-claimed after staying undeclared for this long.
/// Without a grace period the 100ms sweep claimed every writer whose owning
/// node's `ros_discovery_info` was merely still in flight (i.e. virtually all
/// of them at startup - seen in production on the joy topic, claimed one poll
/// tick before its node's declaration arrived).
pub(crate) const ORPHAN_GRACE_PERIOD: std::time::Duration = std::time::Duration::from_secs(5);

/// Node-internal standard topics: every node has these writers, they are
/// always declared by their owner eventually, and bridging them from a
/// synthetic node is never useful.
const ORPHAN_EXCLUDED_TOPICS: &[&str] = &["rt/rosout", "rt/parameter_events"];

/// True when `name` (fullname, with or without leading '/') designates one of
/// the synthetic orphan nodes.
pub(crate) fn is_orphan_node_name(name: &str) -> bool {
    name.trim_start_matches('/').starts_with(ORPHAN_NODE_PREFIX)
}

kedefine!(
    pub(crate) ke_admin_participant: "dds/${pgid:*}",
    pub(crate) ke_admin_writer: "dds/${pgid:*}/writer/${wgid:*}/${topic:**}",
    pub(crate) ke_admin_reader: "dds/${pgid:*}/reader/${wgid:*}/${topic:**}",
    pub(crate) ke_admin_node: "node/${node_id:**}",
);

#[derive(Default)]
pub struct DiscoveredEntities {
    participants: HashMap<Gid, DdsParticipant>,
    writers: HashMap<Gid, DdsEntity>,
    readers: HashMap<Gid, DdsEntity>,
    ros_participant_info: HashMap<Gid, ParticipantEntitiesInfo>,
    nodes_info: HashMap<Gid, HashMap<String, NodeInfo>>,
    admin_space: HashMap<OwnedKeyExpr, EntityRef>,
    // First time each still-unclaimed writer was seen by the orphan sweep
    // (grace-period bookkeeping; entries are pruned when claimed or removed).
    orphan_first_seen: HashMap<Gid, std::time::Instant>,
}

impl Debug for DiscoveredEntities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "participants: {:?}",
            self.participants.keys().collect::<Vec<&Gid>>()
        )?;
        writeln!(
            f,
            "writers: {:?}",
            self.writers.keys().collect::<Vec<&Gid>>()
        )?;
        writeln!(
            f,
            "readers: {:?}",
            self.readers.keys().collect::<Vec<&Gid>>()
        )?;
        writeln!(f, "ros_participant_info: {:?}", self.ros_participant_info)?;
        writeln!(f, "nodes_info: {:?}", self.nodes_info)?;
        writeln!(
            f,
            "admin_space: {:?}",
            self.admin_space.keys().collect::<Vec<&OwnedKeyExpr>>()
        )
    }
}

#[derive(Debug)]
enum EntityRef {
    Participant(Gid),
    Writer(Gid),
    Reader(Gid),
    Node(Gid, String),
}

impl DiscoveredEntities {
    #[inline]
    pub fn add_participant(&mut self, participant: DdsParticipant) {
        self.admin_space.insert(
            keformat!(ke_admin_participant::formatter(), pgid = participant.key).unwrap(),
            EntityRef::Participant(participant.key),
        );
        self.participants.insert(participant.key, participant);
    }

    #[inline]
    pub fn remove_participant(&mut self, gid: &Gid) -> Vec<ROS2DiscoveryEvent> {
        let mut events: Vec<ROS2DiscoveryEvent> = Vec::new();
        // Remove Participant from participants list and from admin_space
        self.participants.remove(gid);
        self.admin_space
            .remove(&keformat!(ke_admin_participant::formatter(), pgid = gid).unwrap());
        // Remove associated NodeInfos
        if let Some(nodes) = self.nodes_info.remove(gid) {
            for (name, mut node) in nodes {
                tracing::info!("Undiscovered ROS Node {}", name);
                self.admin_space.remove(
                    &keformat!(ke_admin_node::formatter(), node_id = node.id_as_keyexpr(),)
                        .unwrap(),
                );
                // return undiscovery events for this node
                events.append(&mut node.remove_all_entities());
            }
        }
        // Purge the participant's endpoints from the DDS-level maps and the
        // admin space. Previously they were left behind: the orphan sweep then
        // resurrected routes for a DEAD participant's writers (ghost routes
        // that nothing could ever tear down), and the maps leaked.
        let stale_writers: Vec<Gid> = self
            .writers
            .values()
            .filter(|w| w.participant_key == *gid)
            .map(|w| w.key)
            .collect();
        for wgid in stale_writers {
            if let Some(w) = self.writers.remove(&wgid) {
                self.admin_space.remove(
                    &keformat!(
                        ke_admin_writer::formatter(),
                        pgid = w.participant_key,
                        wgid = w.key,
                        topic = &w.topic_name,
                    )
                    .unwrap(),
                );
            }
            self.orphan_first_seen.remove(&wgid);
        }
        let stale_readers: Vec<Gid> = self
            .readers
            .values()
            .filter(|r| r.participant_key == *gid)
            .map(|r| r.key)
            .collect();
        for rgid in stale_readers {
            if let Some(r) = self.readers.remove(&rgid) {
                self.admin_space.remove(
                    &keformat!(
                        ke_admin_reader::formatter(),
                        pgid = r.participant_key,
                        wgid = r.key,
                        topic = &r.topic_name,
                    )
                    .unwrap(),
                );
            }
        }
        events
    }

    #[inline]
    pub fn add_writer(&mut self, writer: DdsEntity) -> Option<ROS2DiscoveryEvent> {
        // insert in admin space
        self.admin_space.insert(
            keformat!(
                ke_admin_writer::formatter(),
                pgid = writer.participant_key,
                wgid = writer.key,
                topic = &writer.topic_name,
            )
            .unwrap(),
            EntityRef::Writer(writer.key),
        );

        // Check if this Writer is present in some NodeInfo.undiscovered_writer list
        let mut event: Option<ROS2DiscoveryEvent> = None;
        for nodes_map in self.nodes_info.values_mut() {
            for node in nodes_map.values_mut() {
                if let Some(i) = node
                    .undiscovered_writer
                    .iter()
                    .position(|gid| gid == &writer.key)
                {
                    // update the NodeInfo with this Writer's info
                    node.undiscovered_writer.remove(i);
                    event = node.update_with_writer(&writer);
                    break;
                }
            }
            if event.is_some() {
                break;
            }
        }

        // insert in Writers list
        self.writers.insert(writer.key, writer);
        event
    }

    #[inline]
    pub fn get_writer(&self, gid: &Gid) -> Option<&DdsEntity> {
        self.writers.get(gid)
    }

    #[inline]
    pub fn remove_writer(&mut self, gid: &Gid) -> Option<ROS2DiscoveryEvent> {
        if let Some(writer) = self.writers.remove(gid) {
            self.admin_space.remove(
                &keformat!(
                    ke_admin_writer::formatter(),
                    pgid = writer.participant_key,
                    wgid = writer.key,
                    topic = &writer.topic_name,
                )
                .unwrap(),
            );

            // Remove the Writer from any NodeInfo that might use it, possibly leading to a UndiscoveredX event
            for nodes_map in self.nodes_info.values_mut() {
                for node in nodes_map.values_mut() {
                    if let Some(e) = node.remove_writer(gid) {
                        // A Reader can be used by only 1 Node, no need to go on with loops
                        return Some(e);
                    }
                }
            }
        }
        None
    }

    #[inline]
    pub fn add_reader(&mut self, reader: DdsEntity) -> Option<ROS2DiscoveryEvent> {
        // insert in admin space
        self.admin_space.insert(
            keformat!(
                ke_admin_reader::formatter(),
                pgid = reader.participant_key,
                wgid = reader.key,
                topic = &reader.topic_name,
            )
            .unwrap(),
            EntityRef::Reader(reader.key),
        );

        // Check if this Reader is present in some NodeInfo.undiscovered_reader list
        let mut event = None;
        for nodes_map in self.nodes_info.values_mut() {
            for node in nodes_map.values_mut() {
                if let Some(i) = node
                    .undiscovered_reader
                    .iter()
                    .position(|gid| gid == &reader.key)
                {
                    // update the NodeInfo with this Reader's info
                    node.undiscovered_reader.remove(i);
                    event = node.update_with_reader(&reader);
                    break;
                }
            }
            if event.is_some() {
                break;
            }
        }

        // insert in Readers list
        self.readers.insert(reader.key, reader);
        event
    }

    #[inline]
    pub fn get_reader(&self, gid: &Gid) -> Option<&DdsEntity> {
        self.readers.get(gid)
    }

    #[inline]
    pub fn remove_reader(&mut self, gid: &Gid) -> Option<ROS2DiscoveryEvent> {
        if let Some(reader) = self.readers.remove(gid) {
            self.admin_space.remove(
                &keformat!(
                    ke_admin_reader::formatter(),
                    pgid = reader.participant_key,
                    wgid = reader.key,
                    topic = &reader.topic_name,
                )
                .unwrap(),
            );

            // Remove the Reader from any NodeInfo that might use it, possibly leading to a UndiscoveredX event
            for nodes_map in self.nodes_info.values_mut() {
                for node in nodes_map.values_mut() {
                    if let Some(e) = node.remove_reader(gid) {
                        // A Reader can be used by only 1 Node, no need to go on with loops
                        return Some(e);
                    }
                }
            }
        }
        None
    }

    pub fn update_participant_info(
        &mut self,
        ros_info: ParticipantEntitiesInfo,
    ) -> Vec<ROS2DiscoveryEvent> {
        let mut events: Vec<ROS2DiscoveryEvent> = Vec::new();
        let Self {
            writers,
            readers,
            nodes_info,
            admin_space,
            ..
        } = self;
        let nodes_map = nodes_info.entry(ros_info.gid).or_insert_with(HashMap::new);

        // Remove nodes that are no longer present in ParticipantEntitiesInfo
        nodes_map.retain(|name, node| {
            // Never remove the synthetic nodes holding orphan (unclaimed)
            // endpoints: they are not advertised in ros_discovery_info, so they
            // would always be retained-out. Their endpoints are cleaned up via
            // remove_writer/remove_reader and via the ownership transfer below.
            if is_orphan_node_name(name) {
                return true;
            }
            if !ros_info.node_entities_info_seq.contains_key(name) {
                tracing::info!("Undiscovered ROS Node {}", name);
                admin_space.remove(
                    &keformat!(ke_admin_node::formatter(), node_id = node.id_as_keyexpr(),)
                        .unwrap(),
                );
                // return undiscovery events for this node
                events.append(&mut node.remove_all_entities());
                false
            } else {
                true
            }
        });

        // Writer GIDs currently owned by this participant's synthetic orphan
        // node(s), mapped to the owning orphan node name. When a REAL node
        // declares such a writer below, ownership is TRANSFERRED to it: the
        // real node claims it normally (DiscoveredMsgPub with the real node),
        // then the orphan node releases it (UndiscoveredMsgPub after, so the
        // route sees add-then-remove and never flaps empty). The old "sticky"
        // rule instead ignored the real claim forever, permanently
        // misattributing every writer whose ros_discovery_info was merely late.
        let orphan_owned: HashMap<Gid, String> = nodes_map
            .iter()
            .filter(|(name, _)| is_orphan_node_name(name))
            .flat_map(|(name, node)| {
                node.msg_pub
                    .values()
                    .flat_map(|p| p.writers.iter().copied())
                    .map(move |w| (w, name.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        let mut transferred: Vec<Gid> = Vec::new();

        // For each declared node in this ros_node_info
        for (name, ros_node_info) in &ros_info.node_entities_info_seq {
            // If node was not yet discovered, add a new NodeInfo
            if !nodes_map.contains_key(name) {
                tracing::info!("Discovered ROS Node {}", name);
                match NodeInfo::create(
                    ros_node_info.node_namespace.clone(),
                    ros_node_info.node_name.clone(),
                    ros_info.gid,
                ) {
                    Ok(node) => {
                        admin_space.insert(
                            keformat!(ke_admin_node::formatter(), node_id = node.id_as_keyexpr(),)
                                .unwrap(),
                            EntityRef::Node(ros_info.gid, node.fullname().to_string()),
                        );
                        nodes_map.insert(node.fullname().to_string(), node);
                    }
                    Err(e) => {
                        tracing::warn!("ROS Node has incompatible name: {e}");
                        break;
                    }
                }
            };

            // Update NodeInfo, adding resulting events to the list
            let node = nodes_map.get_mut(name).unwrap();
            events.append(&mut Self::update_node_info(
                node,
                ros_node_info,
                readers,
                writers,
                &orphan_owned,
                &mut transferred,
            ));
        }

        // Complete the ownership transfers: release each transferred writer
        // from its orphan node (AFTER the real node's Discovered event above),
        // and drop orphan nodes left empty.
        for wgid in transferred {
            let Some(orphan_name) = orphan_owned.get(&wgid) else {
                continue;
            };
            if let Some(orphan_node) = nodes_map.get_mut(orphan_name) {
                tracing::info!(
                    "DDS Writer {wgid} is now declared by its real ROS node - releasing it from {orphan_name}"
                );
                if let Some(e) = orphan_node.remove_writer(&wgid) {
                    events.push(e);
                }
                if orphan_node.msg_pub.is_empty() {
                    let node_ke = orphan_node.id_as_keyexpr().to_owned();
                    admin_space
                        .remove(&keformat!(ke_admin_node::formatter(), node_id = node_ke).unwrap());
                    nodes_map.remove(orphan_name);
                }
            }
        }

        // Save ParticipantEntitiesInfo
        self.ros_participant_info.insert(ros_info.gid, ros_info);
        events
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_node_info(
        node: &mut NodeInfo,
        ros_node_info: &NodeEntitiesInfo,
        readers: &mut HashMap<Gid, DdsEntity>,
        writers: &mut HashMap<Gid, DdsEntity>,
        orphan_owned: &HashMap<Gid, String>,
        transferred: &mut Vec<Gid>,
    ) -> Vec<ROS2DiscoveryEvent> {
        let mut events = Vec::new();
        // For each declared Reader
        for rgid in &ros_node_info.reader_gid_seq {
            if let Some(entity) = readers.get(rgid) {
                tracing::trace!(
                    "ROS Node {ros_node_info} declares a Reader on {}",
                    entity.topic_name
                );
                if let Some(e) = node.update_with_reader(entity) {
                    tracing::debug!(
                        "ROS Node {ros_node_info} declares a new Reader on {}",
                        entity.topic_name
                    );
                    events.push(e)
                };
            } else {
                tracing::debug!(
                    "ROS Node {ros_node_info} declares a not yet discovered DDS Reader: {rgid}"
                );
                node.undiscovered_reader.push(*rgid);
            }
        }
        // For each declared Writer
        for wgid in &ros_node_info.writer_gid_seq {
            // Writers currently owned by a synthetic orphan node are being
            // claimed by their REAL node now: record the transfer (the orphan
            // side is released by the caller, after this node's events).
            if orphan_owned.contains_key(wgid) {
                transferred.push(*wgid);
            }
            if let Some(entity) = writers.get(wgid) {
                tracing::trace!(
                    "ROS Node {ros_node_info} declares Writer on {}",
                    entity.topic_name
                );
                if let Some(e) = node.update_with_writer(entity) {
                    tracing::debug!(
                        "ROS Node {ros_node_info} declares a new Writer on {}",
                        entity.topic_name
                    );
                    events.push(e)
                };
            } else {
                tracing::debug!(
                    "ROS Node {ros_node_info} declares a not yet discovered DDS Writer: {wgid}"
                );
                node.undiscovered_writer.push(*wgid);
            }
        }
        events
    }

    /// Forward DDS message Writers (`rt/...`) that no ROS node declares in
    /// `ros_discovery_info` (RTI labels them `_NODE_NAME_UNKNOWN_`); otherwise they
    /// are stored but never routed. Each is attached to a synthetic
    /// per-participant node (see [`ORPHAN_NODE_PREFIX`]), reusing the regular
    /// Writer routing/QoS/undiscovery machinery. If the real owner declares the
    /// writer later, ownership is transferred to it (see update_participant_info).
    ///
    /// A writer is only claimed after staying undeclared for `grace` (see
    /// [`ORPHAN_GRACE_PERIOD`]): a late `ros_discovery_info` must NOT lose the
    /// race against this sweep. Node-internal standard topics (rosout,
    /// parameter_events) are never claimed.
    pub fn forward_orphan_writers(
        &mut self,
        grace: std::time::Duration,
    ) -> Vec<ROS2DiscoveryEvent> {
        let mut events: Vec<ROS2DiscoveryEvent> = Vec::new();
        let now = std::time::Instant::now();

        // Set of already-claimed Writer GIDs, built ONCE per sweep (O(1) per
        // candidate instead of O(nodes) - the sweep runs on every 100ms poll).
        let claimed_writers: std::collections::HashSet<Gid> = self
            .nodes_info
            .values()
            .flat_map(|nodes_map| nodes_map.values())
            .flat_map(|node| {
                node.undiscovered_writer.iter().copied().chain(
                    node.msg_pub
                        .values()
                        .flat_map(|p| p.writers.iter().copied()),
                )
            })
            .collect();

        // Candidate orphans: plain message Writers ("rt/<topic>") not yet claimed.
        // Action sub-topics ("rt/<action>/_action/...") are excluded - tracked
        // separately, not via msg_pub. Standard node-internal topics excluded.
        let orphan_candidates: Vec<DdsEntity> = self
            .writers
            .values()
            .filter(|w| w.topic_name.starts_with("rt/") && !w.topic_name.contains("/_action/"))
            .filter(|w| !ORPHAN_EXCLUDED_TOPICS.contains(&w.topic_name.as_str()))
            .filter(|w| !claimed_writers.contains(&w.key))
            .cloned()
            .collect();

        // Grace-period bookkeeping: remember when each candidate was first seen
        // unclaimed; forget writers that are gone or got claimed meanwhile.
        let current: std::collections::HashSet<Gid> =
            orphan_candidates.iter().map(|w| w.key).collect();
        self.orphan_first_seen
            .retain(|gid, _| current.contains(gid));

        for writer in orphan_candidates {
            let first_seen = *self.orphan_first_seen.entry(writer.key).or_insert(now);
            if now.duration_since(first_seen) < grace {
                continue; // its ros_discovery_info may still be in flight
            }

            let participant = writer.participant_key;
            // Per-participant synthetic node name (see ORPHAN_NODE_PREFIX doc).
            let orphan_node_name = format!("{ORPHAN_NODE_PREFIX}{participant}");
            let nodes_map = self.nodes_info.entry(participant).or_default();
            let orphan_fullname = format!("/{orphan_node_name}");
            // Get-or-create the synthetic orphan node for this participant.
            if !nodes_map.contains_key(&orphan_fullname) {
                match NodeInfo::create("/".to_string(), orphan_node_name.clone(), participant) {
                    Ok(node) => {
                        self.admin_space.insert(
                            keformat!(ke_admin_node::formatter(), node_id = node.id_as_keyexpr(),)
                                .unwrap(),
                            EntityRef::Node(participant, node.fullname().to_string()),
                        );
                        self.nodes_info
                            .get_mut(&participant)
                            .unwrap()
                            .insert(node.fullname().to_string(), node);
                    }
                    Err(e) => {
                        tracing::warn!("Cannot create synthetic orphan-endpoints node: {e}");
                        continue;
                    }
                }
            }
            let node = self
                .nodes_info
                .get_mut(&participant)
                .unwrap()
                .get_mut(&orphan_fullname)
                .unwrap();
            if let Some(e) = node.update_with_writer(&writer) {
                tracing::info!(
                    "Forwarding unclaimed DDS Writer on '{}' (not declared by any ROS node for {:?})",
                    writer.topic_name,
                    grace,
                );
                events.push(e);
            }
            self.orphan_first_seen.remove(&writer.key);
        }
        events
    }

    fn get_entity_json_value(
        &self,
        entity_ref: &EntityRef,
    ) -> Result<Option<serde_json::Value>, serde_json::Error> {
        match entity_ref {
            EntityRef::Participant(gid) => self
                .participants
                .get(gid)
                .map(serde_json::to_value)
                .map(remove_null_qos_values)
                .transpose(),
            EntityRef::Writer(gid) => self
                .writers
                .get(gid)
                .map(serde_json::to_value)
                .map(remove_null_qos_values)
                .transpose(),
            EntityRef::Reader(gid) => self
                .readers
                .get(gid)
                .map(serde_json::to_value)
                .map(remove_null_qos_values)
                .transpose(),
            EntityRef::Node(gid, name) => self
                .nodes_info
                .get(gid)
                .and_then(|map| map.get(name))
                .map(serde_json::to_value)
                .transpose(),
        }
    }

    pub async fn treat_admin_query(&self, query: &Query, admin_keyexpr_prefix: &keyexpr) {
        let selector = query.selector();

        // get the list of sub-key expressions that will match the same stored keys than
        // the selector, if those keys had the admin_keyexpr_prefix.
        let sub_kes = selector.key_expr().strip_prefix(admin_keyexpr_prefix);
        if sub_kes.is_empty() {
            tracing::error!("Received query for admin space: '{}' - but it's not prefixed by admin_keyexpr_prefix='{}'", selector, admin_keyexpr_prefix);
            return;
        }

        // For all sub-key expression
        for sub_ke in sub_kes {
            if sub_ke.is_wild() {
                // iterate over all admin space to find matching keys and reply for each
                for (ke, entity_ref) in self.admin_space.iter() {
                    if sub_ke.intersects(ke) {
                        self.send_admin_reply(query, admin_keyexpr_prefix, ke, entity_ref)
                            .await;
                    }
                }
            } else {
                // sub_ke correspond to 1 key - just get it and reply
                if let Some(entity_ref) = self.admin_space.get(sub_ke) {
                    self.send_admin_reply(query, admin_keyexpr_prefix, sub_ke, entity_ref)
                        .await;
                }
            }
        }
    }

    async fn send_admin_reply(
        &self,
        query: &Query,
        admin_keyexpr_prefix: &keyexpr,
        key_expr: &keyexpr,
        entity_ref: &EntityRef,
    ) {
        match self.get_entity_json_value(entity_ref) {
            Ok(Some(v)) => {
                let admin_keyexpr = admin_keyexpr_prefix / key_expr;
                match serde_json::to_vec(&v) {
                    Ok(bytes) => {
                        if let Err(e) = query
                            .reply(admin_keyexpr, ZBytes::from(bytes))
                            .encoding(Encoding::APPLICATION_JSON)
                            .await
                        {
                            tracing::warn!("Error replying to admin query {:?}: {}", query, e);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Error transforming JSON to admin query {:?}: {}", query, e);
                    }
                }
            }
            Ok(None) => {
                tracing::error!("INTERNAL ERROR: Dangling {:?} for {}", entity_ref, key_expr)
            }
            Err(e) => {
                tracing::error!("INTERNAL ERROR serializing admin value as JSON: {}", e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use cyclors::qos::Qos;

    use super::*;
    use crate::ros_discovery::{NodeEntitiesInfo, ParticipantEntitiesInfo};

    fn participant() -> Gid {
        Gid::from([1; 16])
    }

    fn writer(gid: u8, topic: &str) -> DdsEntity {
        writer_of(participant(), gid, topic)
    }

    fn writer_of(participant: Gid, gid: u8, topic: &str) -> DdsEntity {
        DdsEntity {
            key: Gid::from([gid; 16]),
            participant_key: participant,
            topic_name: topic.to_string(),
            type_name: "std_msgs::msg::dds_::String_".to_string(),
            _type_info: None,
            keyless: true,
            qos: Qos::default(),
        }
    }

    fn orphan_fullname(participant: Gid) -> String {
        format!("/{ORPHAN_NODE_PREFIX}{participant}")
    }

    // Returns the node fullname that owns the publisher in a DiscoveredMsgPub event.
    fn pub_node(event: &ROS2DiscoveryEvent) -> &str {
        match event {
            ROS2DiscoveryEvent::DiscoveredMsgPub(node, _) => node,
            other => panic!("expected DiscoveredMsgPub, got {other:?}"),
        }
    }

    // Returns the node fullname in an UndiscoveredMsgPub event.
    fn unpub_node(event: &ROS2DiscoveryEvent) -> &str {
        match event {
            ROS2DiscoveryEvent::UndiscoveredMsgPub(node, _) => node,
            other => panic!("expected UndiscoveredMsgPub, got {other:?}"),
        }
    }

    fn rdi_declaring_writer(node_name: &str, wgid: Gid, part: Gid) -> ParticipantEntitiesInfo {
        let mut node_info = NodeEntitiesInfo::new("/".to_string(), node_name.to_string());
        node_info.writer_gid_seq.insert(wgid);
        let mut part_info = ParticipantEntitiesInfo::new(part);
        part_info
            .node_entities_info_seq
            .insert(node_info.full_name(), node_info);
        part_info
    }

    #[test]
    fn orphan_writer_is_forwarded_once() {
        let mut entities = DiscoveredEntities::default();
        // A plain rt/ Writer that no node declares.
        assert!(entities.add_writer(writer(10, "rt/some/topic")).is_none());

        // First sweep (grace elapsed) forwards it under the synthetic node.
        let events = entities.forward_orphan_writers(Duration::ZERO);
        assert_eq!(events.len(), 1);
        assert_eq!(pub_node(&events[0]), orphan_fullname(participant()));

        // Second sweep is idempotent: already owned, so no new event.
        assert!(entities.forward_orphan_writers(Duration::ZERO).is_empty());
    }

    #[test]
    fn non_message_writers_are_not_forwarded() {
        let mut entities = DiscoveredEntities::default();
        entities.add_writer(writer(20, "rq/some/serviceRequest"));
        entities.add_writer(writer(21, "rr/some/serviceReply"));
        entities.add_writer(writer(22, "rt/some/action/_action/status"));
        entities.add_writer(writer(23, "rt/some/action/_action/feedback"));

        // None of these are plain message publishers: nothing is orphan-forwarded.
        assert!(entities.forward_orphan_writers(Duration::ZERO).is_empty());
    }

    /// FIXED (was: sticky misattribution): a writer claimed by the orphan sweep
    /// is TRANSFERRED to its real node when that node's (late)
    /// ros_discovery_info finally declares it — with the real node's Discovered
    /// event emitted BEFORE the orphan's Undiscovered event, so the route never
    /// flaps through an empty local_nodes set.
    #[test]
    fn orphaned_writer_is_transferred_to_late_declaring_node() {
        let mut entities = DiscoveredEntities::default();
        let w = writer(30, "rt/some/topic");
        let wgid = w.key;
        entities.add_writer(w);

        // Forwarded as orphan first (grace elapsed).
        let events = entities.forward_orphan_writers(Duration::ZERO);
        assert_eq!(events.len(), 1);
        assert_eq!(pub_node(&events[0]), orphan_fullname(participant()));

        // Later, the real node starts declaring the very same Writer GID.
        let events = entities.update_participant_info(rdi_declaring_writer(
            "real_node",
            wgid,
            participant(),
        ));
        assert_eq!(events.len(), 2, "expected transfer events, got {events:?}");
        assert_eq!(pub_node(&events[0]), "/real_node");
        assert_eq!(unpub_node(&events[1]), orphan_fullname(participant()));

        // Ownership fully moved; the empty orphan node is gone.
        let nodes = entities.nodes_info.get(&participant()).unwrap();
        assert!(
            !nodes.contains_key(&orphan_fullname(participant())),
            "empty orphan node must be removed"
        );
        let real_owns = nodes
            .get("/real_node")
            .map(|n| n.msg_pub.values().any(|p| p.writers.contains(&wgid)))
            .unwrap_or(false);
        assert!(real_owns, "real node must own the Writer after transfer");

        // And the sweep must NOT re-claim it.
        assert!(entities.forward_orphan_writers(Duration::ZERO).is_empty());
    }

    /// FIXED (was: shared fullname collision tearing down live routes): each
    /// participant gets its own synthetic node, so one participant's departure
    /// only removes ITS local_nodes entry — the route keeps serving the other.
    #[test]
    fn orphan_nodes_are_participant_unique_and_independent() {
        let p1 = Gid::from([1; 16]);
        let p2 = Gid::from([2; 16]);
        let mut entities = DiscoveredEntities::default();

        let w1 = writer_of(p1, 40, "rt/tf");
        let w1gid = w1.key;
        entities.add_writer(w1);
        entities.add_writer(writer_of(p2, 41, "rt/tf"));

        let events = entities.forward_orphan_writers(Duration::ZERO);
        assert_eq!(events.len(), 2);
        let names: std::collections::HashSet<&str> = events.iter().map(|e| pub_node(e)).collect();
        assert_eq!(
            names.len(),
            2,
            "orphan node fullnames must be participant-unique, got {names:?}"
        );

        // Participant 1's writer disappears: only ITS orphan node undiscovers.
        let event = entities.remove_writer(&w1gid).expect("undiscovery event");
        assert_eq!(unpub_node(&event), orphan_fullname(p1));

        // Participant 2's claim is untouched (route keeps a local node).
        let p2_owns = entities
            .nodes_info
            .get(&p2)
            .and_then(|nodes| nodes.get(&orphan_fullname(p2)))
            .map(|n| n.msg_pub.values().any(|p| !p.writers.is_empty()))
            .unwrap_or(false);
        assert!(p2_owns, "participant 2's orphan claim must survive");
    }

    /// FIXED (was: claimed at startup): node-internal standard topics are never
    /// orphan-forwarded.
    #[test]
    fn rosout_and_parameter_events_are_never_orphan_forwarded() {
        let mut entities = DiscoveredEntities::default();
        entities.add_writer(writer(50, "rt/rosout"));
        entities.add_writer(writer(51, "rt/parameter_events"));

        assert!(entities.forward_orphan_writers(Duration::ZERO).is_empty());
    }

    /// FIXED (was: no grace period): a writer whose ros_discovery_info is
    /// merely late is NOT claimed while the grace period runs — the real node
    /// wins the race and claims it normally.
    #[test]
    fn grace_period_lets_late_rdi_win() {
        let mut entities = DiscoveredEntities::default();
        let w = writer(60, "rt/joy");
        let wgid = w.key;
        entities.add_writer(w);

        // Sweep ticks during the grace window: nothing claimed.
        assert!(entities
            .forward_orphan_writers(ORPHAN_GRACE_PERIOD)
            .is_empty());
        assert!(entities
            .forward_orphan_writers(ORPHAN_GRACE_PERIOD)
            .is_empty());

        // The (late) declaration arrives: the real node claims it normally.
        let events =
            entities.update_participant_info(rdi_declaring_writer("joy_node", wgid, participant()));
        assert_eq!(events.len(), 1);
        assert_eq!(pub_node(&events[0]), "/joy_node");

        // Subsequent sweeps have nothing to claim (writer is declared now).
        assert!(entities
            .forward_orphan_writers(ORPHAN_GRACE_PERIOD)
            .is_empty());
        assert!(entities.forward_orphan_writers(Duration::ZERO).is_empty());
    }

    /// remove_participant must purge the participant's endpoints so the sweep
    /// cannot resurrect routes for a dead participant's writers.
    #[test]
    fn dead_participants_writers_are_not_resurrected() {
        let mut entities = DiscoveredEntities::default();
        entities.add_writer(writer(70, "rt/ghost_topic"));
        let events = entities.forward_orphan_writers(Duration::ZERO);
        assert_eq!(events.len(), 1);

        // The participant dies (lease expiry): its orphan claim undiscovers...
        let events = entities.remove_participant(&participant());
        assert_eq!(events.len(), 1);
        assert_eq!(unpub_node(&events[0]), orphan_fullname(participant()));

        // ...and its writers are purged: no ghost re-claim on the next sweep.
        assert!(entities.forward_orphan_writers(Duration::ZERO).is_empty());
        assert!(entities.writers.is_empty(), "writers map must be purged");
    }
}

// Remove any null QoS values from a serde_json::Value
fn remove_null_qos_values(
    value: Result<serde_json::Value, serde_json::Error>,
) -> Result<serde_json::Value, serde_json::Error> {
    match value {
        Ok(value) => match value {
            serde_json::Value::Object(mut obj) => {
                let qos = obj.get_mut("qos");
                if let Some(qos) = qos {
                    if qos.is_object() {
                        qos.as_object_mut().unwrap().retain(|_, v| !v.is_null());
                    }
                }
                Ok(serde_json::Value::Object(obj))
            }
            _ => Ok(value),
        },
        Err(error) => Err(error),
    }
}
