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

/// Name of the synthetic ROS node under which DDS writers that no real ROS node
/// declares in `ros_discovery_info` are attached, so they still get routed.
/// (e.g. ros2_control's `controller_manager/activity` publisher, which appears in
/// DDS discovery as `_NODE_NAME_UNKNOWN_`.)
pub(crate) const ORPHAN_NODE_NAME: &str = "_zenoh_orphan_endpoints_";

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
            // Never remove the synthetic node holding orphan (unclaimed) endpoints:
            // it is not advertised in ros_discovery_info, so it would always be retained-out.
            // Its endpoints are cleaned up individually via remove_writer/remove_reader.
            if name == ORPHAN_NODE_NAME {
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

        // Writer GIDs already owned by the synthetic orphan node (see
        // forward_orphan_writers). Once a Writer has been forwarded as an orphan it
        // stays attached to the orphan node for its lifetime, so we must NOT also
        // attach it to the real node that later starts declaring it - that would
        // create a duplicate route and leave stale state on teardown (remove_writer
        // stops at the first owning node).
        let orphan_writers: std::collections::HashSet<Gid> = nodes_map
            .get(ORPHAN_NODE_NAME)
            .map(|node| {
                node.msg_pub
                    .values()
                    .flat_map(|p| p.writers.iter().copied())
                    .collect()
            })
            .unwrap_or_default();

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
                        self.admin_space.insert(
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
                &orphan_writers,
            ));
        }

        // Save ParticipantEntitiesInfo
        self.ros_participant_info.insert(ros_info.gid, ros_info);
        events
    }

    pub fn update_node_info(
        node: &mut NodeInfo,
        ros_node_info: &NodeEntitiesInfo,
        readers: &mut HashMap<Gid, DdsEntity>,
        writers: &mut HashMap<Gid, DdsEntity>,
        orphan_writers: &std::collections::HashSet<Gid>,
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
            // Skip Writers already owned by the synthetic orphan node: they were
            // forwarded as unclaimed earlier and stay attached there (sticky), so
            // they must not be attached to this real node as well.
            if orphan_writers.contains(wgid) {
                continue;
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
    /// [`ORPHAN_NODE_NAME`] node under its participant, reusing the regular Writer
    /// routing/QoS/undiscovery machinery, and stays owned by it for life (later
    /// claims by a real node are ignored). Call after processing `ros_discovery_info`,
    /// so genuinely-claimed writers are never mistaken for orphans.
    pub fn forward_orphan_writers(&mut self) -> Vec<ROS2DiscoveryEvent> {
        let mut events: Vec<ROS2DiscoveryEvent> = Vec::new();

        // Set of already-claimed Writer GIDs, built ONCE per sweep. Re-deriving it
        // per candidate walked every node's publisher map (+ a linear scan of each
        // node's `undiscovered_writer` Vec), i.e. O(candidates x nodes), under the
        // discovered_entities write lock on every ros_discovery_info message. Costly
        // on a large ROS graph; this makes the per-candidate check O(1).
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
        // separately, not via msg_pub.
        let orphan_writers: Vec<DdsEntity> = self
            .writers
            .values()
            .filter(|w| w.topic_name.starts_with("rt/") && !w.topic_name.contains("/_action/"))
            .filter(|w| !claimed_writers.contains(&w.key))
            .cloned()
            .collect();

        for writer in orphan_writers {
            let participant = writer.participant_key;
            let nodes_map = self.nodes_info.entry(participant).or_default();
            // Get-or-create the synthetic orphan node for this participant.
            if !nodes_map.contains_key(ORPHAN_NODE_NAME) {
                match NodeInfo::create("/".to_string(), ORPHAN_NODE_NAME.to_string(), participant) {
                    Ok(node) => {
                        self.admin_space.insert(
                            keformat!(ke_admin_node::formatter(), node_id = node.id_as_keyexpr(),)
                                .unwrap(),
                            EntityRef::Node(participant, node.fullname().to_string()),
                        );
                        self.nodes_info
                            .get_mut(&participant)
                            .unwrap()
                            .insert(ORPHAN_NODE_NAME.to_string(), node);
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
                .get_mut(ORPHAN_NODE_NAME)
                .unwrap();
            if let Some(e) = node.update_with_writer(&writer) {
                tracing::info!(
                    "Forwarding unclaimed DDS Writer on '{}' (not declared by any ROS node)",
                    writer.topic_name
                );
                events.push(e);
            }
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
    use cyclors::qos::Qos;

    use super::*;
    use crate::ros_discovery::{NodeEntitiesInfo, ParticipantEntitiesInfo};

    fn participant() -> Gid {
        Gid::from([1; 16])
    }

    fn writer(gid: u8, topic: &str) -> DdsEntity {
        DdsEntity {
            key: Gid::from([gid; 16]),
            participant_key: participant(),
            topic_name: topic.to_string(),
            type_name: "std_msgs::msg::dds_::String_".to_string(),
            _type_info: None,
            keyless: true,
            qos: Qos::default(),
        }
    }

    // Returns the node fullname that owns the publisher in a DiscoveredMsgPub event.
    fn pub_node(event: &ROS2DiscoveryEvent) -> &str {
        match event {
            ROS2DiscoveryEvent::DiscoveredMsgPub(node, _) => node,
            other => panic!("expected DiscoveredMsgPub, got {other:?}"),
        }
    }

    #[test]
    fn orphan_writer_is_forwarded_once() {
        let mut entities = DiscoveredEntities::default();
        // A plain rt/ Writer that no node declares.
        assert!(entities.add_writer(writer(10, "rt/some/topic")).is_none());

        // First sweep forwards it under the synthetic orphan node.
        let events = entities.forward_orphan_writers();
        assert_eq!(events.len(), 1);
        assert_eq!(pub_node(&events[0]), format!("/{ORPHAN_NODE_NAME}"));

        // Second sweep is idempotent: already owned, so no new event.
        assert!(entities.forward_orphan_writers().is_empty());
    }

    #[test]
    fn non_message_writers_are_not_forwarded() {
        let mut entities = DiscoveredEntities::default();
        entities.add_writer(writer(20, "rq/some/serviceRequest"));
        entities.add_writer(writer(21, "rr/some/serviceReply"));
        entities.add_writer(writer(22, "rt/some/action/_action/status"));
        entities.add_writer(writer(23, "rt/some/action/_action/feedback"));

        // None of these are plain message publishers: nothing is orphan-forwarded.
        assert!(entities.forward_orphan_writers().is_empty());
    }

    #[test]
    fn orphaned_writer_is_not_reclaimed_by_a_late_node() {
        let mut entities = DiscoveredEntities::default();
        let w = writer(30, "rt/some/topic");
        let wgid = w.key;
        entities.add_writer(w);

        // Forwarded as orphan first.
        let events = entities.forward_orphan_writers();
        assert_eq!(events.len(), 1);
        assert_eq!(pub_node(&events[0]), format!("/{ORPHAN_NODE_NAME}"));

        // Later, a real node starts declaring the very same Writer GID.
        let mut node_info = NodeEntitiesInfo::new("/".to_string(), "real_node".to_string());
        node_info.writer_gid_seq.insert(wgid);
        let mut part_info = ParticipantEntitiesInfo::new(participant());
        part_info
            .node_entities_info_seq
            .insert(node_info.full_name(), node_info);

        // The sticky-orphan rule must hold: no duplicate DiscoveredMsgPub event...
        let events = entities.update_participant_info(part_info);
        assert!(
            events.is_empty(),
            "late claim must not re-route an already-orphaned Writer, got {events:?}"
        );

        // ...and the Writer stays owned by exactly the orphan node, not the real one.
        let nodes = entities.nodes_info.get(&participant()).unwrap();
        let orphan_owns = nodes
            .get(ORPHAN_NODE_NAME)
            .unwrap()
            .msg_pub
            .values()
            .any(|p| p.writers.contains(&wgid));
        assert!(orphan_owns, "orphan node should still own the Writer");
        let real_owns = nodes
            .get("/real_node")
            .map(|n| n.msg_pub.values().any(|p| p.writers.contains(&wgid)))
            .unwrap_or(false);
        assert!(!real_owns, "real node must not also own the Writer");
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

    // Returns the node fullname in an UndiscoveredMsgPub event.
    fn unpub_node(event: &ROS2DiscoveryEvent) -> &str {
        match event {
            ROS2DiscoveryEvent::UndiscoveredMsgPub(node, _) => node,
            other => panic!("expected UndiscoveredMsgPub, got {other:?}"),
        }
    }

    /// CLAIM (audit finding, seen live on /tf in production): all participants'
    /// orphan nodes share the single fullname "/_zenoh_orphan_endpoints_". A
    /// route's `local_nodes` is a HashSet of fullnames, so N orphan-claimed
    /// co-publishers collapse into ONE entry — and the first participant whose
    /// writer disappears emits an UndiscoveredMsgPub for that shared name,
    /// which empties `local_nodes` at the routes level and tears the route
    /// down while other participants still publish. No later sweep ever
    /// re-emits a DiscoveredMsgPub, so the route never comes back.
    ///
    /// This test PASSES while the bug exists (it verifies the claim); after a
    /// fix (participant-unique orphan identity, or re-forward after teardown)
    /// its final two assertions are expected to fail and it should be updated.
    #[test]
    fn claim_shared_topic_orphan_collision_tears_down_and_never_recovers() {
        let p1 = Gid::from([1; 16]);
        let p2 = Gid::from([2; 16]);
        let mut entities = DiscoveredEntities::default();

        // Two participants each publish rt/tf; neither is declared by any node.
        let w1 = writer_of(p1, 40, "rt/tf");
        let w1gid = w1.key;
        entities.add_writer(w1);
        entities.add_writer(writer_of(p2, 41, "rt/tf"));

        // Both are orphan-forwarded — under the SAME node fullname.
        let events = entities.forward_orphan_writers();
        assert_eq!(events.len(), 2);
        let orphan_fullname = format!("/{ORPHAN_NODE_NAME}");
        assert_eq!(pub_node(&events[0]), orphan_fullname);
        assert_eq!(pub_node(&events[1]), orphan_fullname);
        // Identical fullnames => at the routes level local_nodes (a HashSet of
        // fullnames) holds ONE entry for two live publishers.

        // Participant 1's writer disappears (node restart / crash).
        let event = entities
            .remove_writer(&w1gid)
            .expect("removal of an orphan-owned writer must emit an event");
        // CLAIM: an UndiscoveredMsgPub for the SHARED fullname is emitted even
        // though participant 2 still publishes rt/tf. routes_mgr will remove
        // the only local_nodes entry and retire/remove the still-needed route.
        assert_eq!(unpub_node(&event), orphan_fullname);

        // CLAIM: no recovery — p2's orphan node still holds rt/tf in msg_pub,
        // so the sweep's update_with_writer returns None forever: no event
        // will ever re-create the torn-down route.
        assert!(
            entities.forward_orphan_writers().is_empty(),
            "sweep must not re-forward (this is the no-recovery half of the bug)"
        );
    }

    /// CLAIM (audit finding, seen live in production logs 2026-07-02): writers
    /// on node-internal standard topics (rosout, parameter_events) are NOT
    /// excluded from orphan forwarding, so any node whose ros_discovery_info
    /// is merely late gets its rosout/parameter_events writers permanently
    /// claimed by the synthetic orphan node.
    #[test]
    fn claim_rosout_and_parameter_events_are_orphan_forwarded() {
        let mut entities = DiscoveredEntities::default();
        entities.add_writer(writer(50, "rt/rosout"));
        entities.add_writer(writer(51, "rt/parameter_events"));

        let events = entities.forward_orphan_writers();
        assert_eq!(
            events.len(),
            2,
            "rosout/parameter_events are claimed by the orphan sweep (the claim); \
             an exclusion-list fix should make this 0"
        );
    }

    /// CLAIM (audit finding, seen live: joy writer claimed 100ms before its
    /// node's ros_discovery_info in production logs): the sweep has no grace
    /// period — a writer whose owning node's ros_discovery_info has simply not
    /// arrived YET (participant completely unknown to ros_discovery) is
    /// claimed immediately and stays misattributed after the node declares it.
    #[test]
    fn claim_no_grace_period_for_participants_without_any_rdi() {
        let mut entities = DiscoveredEntities::default();
        // DDS discovery of the writer arrives; the participant has never sent
        // any ros_discovery_info (its rdi is in flight).
        entities.add_writer(writer(60, "rt/joy"));

        // One sweep tick later (100ms in production) the writer is claimed,
        // even though nothing indicates its node won't declare it momentarily.
        let events = entities.forward_orphan_writers();
        assert_eq!(events.len(), 1);
        assert_eq!(pub_node(&events[0]), format!("/{ORPHAN_NODE_NAME}"));

        // The real node's declaration arrives one poll tick later — too late:
        // it is skipped (sticky rule), the misattribution is permanent.
        let mut node_info = NodeEntitiesInfo::new("/".to_string(), "joy_node".to_string());
        node_info.writer_gid_seq.insert(Gid::from([60; 16]));
        let mut part_info = ParticipantEntitiesInfo::new(participant());
        part_info
            .node_entities_info_seq
            .insert(node_info.full_name(), node_info);
        let events = entities.update_participant_info(part_info);
        assert!(
            events.is_empty(),
            "late-by-one-tick declaration is ignored; writer stays on the orphan node"
        );
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
