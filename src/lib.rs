use rand::Rng;
use std::cmp::max;
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

/// Lamport Clock structure that encapsulates the logical clock value.
/// Invariant: The logical clock value (time) strictly increases over time on events.
pub struct LamportClock {
    time: u64,
}

impl LamportClock {
    /// Initialize a new Lamport Clock with time initialized to zero.
    fn new() -> Self {
        LamportClock { time: 0 }
    }

    /// Increment the clock on an event and return the updated time to be stamped on the message.
    /// "R1. Before executing an event (send, received, or internal), $p_{i}$ executes the following:
    /// $C_{i} := C_{i} + d (d > 0)$," where, "$d$ is typically kept at $1$,
    /// since this allows a process to identify the time of each event uniquely at a process while minimizing $d$'s rate of increase."
    fn tick(&mut self) -> u64 {
        self.time += 1;
        self.time
    }

    /// Update the clock on receiving a message with a remote timestamp and return the updated time.
    /// "R2. Each message piggybacks the clock value of its sender at sending time. When $p_{i}$ receives a message with the timestamp $C_{msg}$,
    /// it executes the following actions:
    /// 1. $C_{i} := max(C_{i}, C_{msg})$.
    /// 2. Execute R1.
    /// 3. Deliver the message."
    fn update(&mut self, remote_time: u64) -> u64 {
        self.time = max(self.time, remote_time);
        self.time += 1;
        self.time
    }

    /// Read the current time of the clock without modifying it.
    fn read(&self) -> u64 {
        self.time
    }
}

/// Message structure that encapsulates the sender ID, timestamp, and content of the message.
pub struct Message {
    sender_id: usize,
    timestamp: u64,
    content: String,
}

/// Node structure that encapsulates a Lamport clock, a receiver for incoming messages, and a map of peer nodes to send messages to.
/// It represents a process in the distributed system.
pub struct Node {
    id: usize,
    clock: LamportClock,
    receiver: Receiver<Message>,
    peers: HashMap<usize, Sender<Message>>,
}

impl Node {
    /// Constructor for `Node` that initializes (and owns) a Lamport clock, a receiver, and a map of peer nodes, and runs the event loop for a specified duration.
    pub fn spawn(
        id: usize,
        receiver: Receiver<Message>,
        peers: HashMap<usize, Sender<Message>>,
        duration: Duration,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut node = Node {
                id,
                clock: LamportClock::new(),
                receiver,
                peers,
            };
            node.run_event_loop(duration);
        })
    }

    /// Simulate a process's life cycle through choosing random actions, processing incoming messages, and updating the Lamport clock accordingly.
    pub fn run_event_loop(&mut self, duration: Duration) {
        let start_time = Instant::now();
        let mut rng = rand::rng();

        println!(
            "Process {} started at (logical) time {}.",
            self.id,
            self.clock.read()
        );

        while start_time.elapsed() < duration {
            let sleep_ms = rng.random_range(10..50);
            thread::sleep(Duration::from_millis(sleep_ms));
            let action_choice = rng.random_range(0..100);
            if action_choice < 33 {
                self.handle_internal_event();
            } else if action_choice < 66 {
                self.handle_send_event(&mut rng);
            } else {
                self.process_incoming_messages();
            }
        }

        println!(
            "Process {} finished at (logical) time {}.",
            self.id,
            self.clock.read()
        );
    }

    /// Handle an internal event by ticking the Lamport clock and logging the event.
    pub fn handle_internal_event(&mut self) {
        let t = self.clock.tick();
        println!(
            "Process {} performed internal event at (logical) time {}.",
            self.id, t
        );
    }

    /// Handle an event of updating the Lamport clock, sending a message to a random peer, and logging the event.
    pub fn handle_send_event<R: Rng>(&mut self, rng: &mut R) {
        if self.peers.is_empty() {
            return;
        }

        let t = self.clock.tick();

        let msg = Message {
            sender_id: self.id,
            timestamp: t,
            content: format!("Message from process {} at (logical) time {}", self.id, t),
        };

        let peers_ids: Vec<usize> = self.peers.keys().cloned().collect();
        let target_id = peers_ids[rng.random_range(0..peers_ids.len())];

        if let Some(tx) = self.peers.get(&target_id) {
            match tx.send(msg) {
                Ok(_) => {
                    println!(
                        "Process {} sent message to process {} at (logical) time {}.",
                        self.id, target_id, t
                    );
                }
                Err(e) => {
                    println!(
                        "Process {} failed to send message to process {}: {}",
                        self.id, target_id, e
                    );
                }
            }
        }
    }

    /// Handle an event of updating the Lamport clock upon receiving messages, processing incoming messages, and logging the events.
    pub fn process_incoming_messages(&mut self) {
        loop {
            match self.receiver.try_recv() {
                Ok(msg) => {
                    let prev_time = self.clock.read();
                    let updated_time = self.clock.update(msg.timestamp);
                    println!(
                        "Process {} received message from process {} at (logical) time {}: {}. Clock updated from {} to {}.",
                        self.id, msg.sender_id, msg.timestamp, msg.content, prev_time, updated_time
                    );
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    println!("Process {}'s receiver disconnected.", self.id);
                    break;
                }
            }
        }
    }
}
