/*----------------------------------------------------------------------------------------------------------
 *  Copyright (c) Peter Bjorklund. All rights reserved. https://github.com/piot/conclave-room-rs
 *  Licensed under the MIT License. See LICENSE in the project root for license information.
 *--------------------------------------------------------------------------------------------------------*/
//! The Conclave Logic for a Room
//!
//! Evaluating connection quality for all connections attached to the room. Using "votes" from the connections, together with
//! [Knowledge] and [ConnectionQuality] it determines which connection should be appointed leader.

 use std::collections::HashMap;
use std::time::Instant;

/// ID or index for a room connection
pub type ConnectionIndex = u8;

/// The reserved index for connection index
const ILLEGAL_CONNECTION_INDEX: ConnectionIndex = 0xff;

/// The knowledge of the game state, typically the step or tick ID.
pub type Knowledge = u64;

/// The term that Leader is currently running.
pub type Term = u16;

/// Evaluating how many times something occurs every second.
pub struct RateMetrics {
    count: u32,
    last_calculated_at: Instant,
}

impl RateMetrics {
    fn new(time: Instant) -> Self {
        Self {
            count: 0,
            last_calculated_at: time,
        }
    }

    fn increment(&mut self) {
        self.count += 1;
    }

    fn has_enough_time_passed(&self, time: Instant) -> bool {
        (time - self.last_calculated_at).as_millis() > 200
    }

    fn calculate_rate(&mut self, time: Instant) -> f32 {
        let elapsed_time = time - self.last_calculated_at;
        let milliseconds = elapsed_time.as_secs() as f32 * 1000.0
            + elapsed_time.subsec_nanos() as f32 / 1_000_000.0;

        let rate = if milliseconds > 0.0 {
            self.count as f32 / milliseconds
        } else {
            0.0
        };

        // Reset the counter and start time for the next period
        self.count = 0;
        self.last_calculated_at = time;

        rate
    }
}

/// Resulting Assessment made by [ConnectionQuality]
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum QualityAssessment {
    NeedMoreTimeOrInformation,
    RecommendDisconnect,
    Acceptable,
    Good,
}

/// Evaluate room connection quality
pub struct ConnectionQuality {
    pub last_ping_at: Instant,
    pub pings_per_second: RateMetrics,
    pub assessment: QualityAssessment,
    threshold: f32,
}

impl ConnectionQuality {
    pub fn new(threshold: f32, time: Instant) -> Self {
        Self {
            assessment: QualityAssessment::NeedMoreTimeOrInformation,
            last_ping_at: Instant::now(),
            pings_per_second: RateMetrics::new(time),
            threshold,
        }
    }

    pub fn on_ping(&mut self, time: Instant) {
        self.last_ping_at = time;
        self.pings_per_second.increment();
    }

    pub fn update(&mut self, time: Instant) {
        if !self.pings_per_second.has_enough_time_passed(time) {
            self.assessment = QualityAssessment::NeedMoreTimeOrInformation;
        } else {
            let pings_per_second = self.pings_per_second.calculate_rate(time);
            self.assessment = if pings_per_second < self.threshold {
                QualityAssessment::RecommendDisconnect
            } else if pings_per_second > self.threshold * 2.0 {
                QualityAssessment::Good
            } else {
                QualityAssessment::Acceptable
            };
        }
    }
}

pub enum ConnectionState {
    Online,
    Disconnected,
}


/// A Room Connection
pub struct Connection {
    pub id: ConnectionIndex,
    pub quality: ConnectionQuality,
    pub knowledge: Knowledge,
    pub state: ConnectionState,
    pub last_reported_term: Term,
    pub has_connection_host: bool,
}

const PINGS_PER_SECOND_THRESHOLD: f32 = 10.0;

impl Connection {
    fn new(connection_id: ConnectionIndex, term: Term, time: Instant) -> Self {
        Connection {
            has_connection_host: false,
            last_reported_term: term,
            id: connection_id,
            quality: ConnectionQuality::new(PINGS_PER_SECOND_THRESHOLD, time),
            knowledge: 0,
            state: ConnectionState::Online,
        }
    }

    pub fn on_ping(
        &mut self,
        term: Term,
        has_connection_to_host: bool,
        knowledge: Knowledge,
        time: Instant,
    ) {
        self.last_reported_term = term;
        self.has_connection_host = has_connection_to_host;
        self.quality.on_ping(time);
        self.knowledge = knowledge;
    }
}

/// Contains the Room [Connection]s as well the appointed Leader.
pub struct Room {
    pub id: ConnectionIndex,
    pub connections: HashMap<ConnectionIndex, Connection>,
    pub leader_index: ConnectionIndex,
    pub term: Term,
}

impl Room {
    pub fn new() -> Self {
        Self {
            term: 0,
            id: 0,
            connections: Default::default(),
            leader_index: ILLEGAL_CONNECTION_INDEX,
        }
    }

    /// checks if most connections, that are on the same term, has lost connection to leader
    pub fn has_most_lost_connection_to_leader(&self) -> bool {
        let mut disappointed_count = 0;
        for (_, connection) in self.connections.iter() {
            if !connection.has_connection_host && connection.last_reported_term == self.term {
                disappointed_count += 1;
            }
        }
        disappointed_count >= self.connections.len()
    }

    pub fn connection_with_most_knowledge_and_acceptable_quality(
        &self,
        exclude_index: ConnectionIndex,
    ) -> ConnectionIndex {
        let mut knowledge: Knowledge = 0;
        let mut connection_index: ConnectionIndex = ILLEGAL_CONNECTION_INDEX;

        for (_, connection) in self.connections.iter() {
            if (connection.knowledge >= knowledge || connection_index == ILLEGAL_CONNECTION_INDEX)
                && (connection.id != exclude_index)
            {
                knowledge = connection.knowledge;
                connection_index = connection.id;
            }
        }

        connection_index
    }

    pub fn change_leader_if_down_voted(&mut self) {
        if self.leader_index == ILLEGAL_CONNECTION_INDEX {
            return;
        }

        if self.has_most_lost_connection_to_leader() {
            self.leader_index =
                self.connection_with_most_knowledge_and_acceptable_quality(self.leader_index);
            // We start a new term, since we have a new leader
            self.term += 1;
        }
    }

    fn find_unique_connection_index(&self) -> ConnectionIndex {
        let mut candidate = self.id;

        while self.connections.contains_key(&candidate) {
            candidate += 1;
            if candidate == self.id {
                panic!("No unique connection index available");
            }
        }

        candidate
    }

    pub fn create_connection(&mut self, time: Instant) -> &mut Connection {
        eprintln!("creating a connection");
        self.id += 1;
        let connection_id = self.find_unique_connection_index();
        let connection = Connection::new(connection_id, self.term, time);
        self.connections.insert(self.id, connection);
        self.connections
            .get_mut(&self.id)
            .expect("key is missing from hashmap somehow")
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::{Knowledge, QualityAssessment, Room, Term};

    #[test]
    fn check_ping() {
        let mut room = Room::new();
        let now = Instant::now();
        let connection = room.create_connection(now);
        assert_eq!(connection.id, 1);
        let knowledge: Knowledge = 42;
        let term: Term = 1;
        connection.on_ping(term, true, knowledge, now);

        connection.quality.update(now);
        assert_eq!(
            connection.quality.assessment,
            QualityAssessment::NeedMoreTimeOrInformation
        );

        let time_in_future = now + Duration::new(10, 0);
        connection.quality.update(time_in_future);
        assert_eq!(
            connection.quality.assessment,
            QualityAssessment::RecommendDisconnect
        );
    }
}
