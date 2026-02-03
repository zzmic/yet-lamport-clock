use rand::Rng;
use std::cmp::max;
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

/// Lamport Clock structure that encapsulates the logical clock value (`time`).
/// Invariant: The logical clock value strictly increases on events.
pub struct LamportClock {
    time: u64,
}

impl LamportClock {
    /// Initialize a new Lamport's logical clock with time initialized to zero.
    const fn new() -> Self {
        Self { time: 0 }
    }

    /// Increment the clock on an event and return the updated time to be stamped on the message.
    /// "R1. Before executing an event (send, received, or internal), $p_{i}$ executes the following:
    /// $C_{i} := C_{i} + d (d > 0)$," where, "$d$ is typically kept at $1$,
    /// since this allows a process to identify the time of each event uniquely at a process while minimizing $d$'s rate of increase" [Raynal and Singhal, 1996].
    /// "R1. This governs how a process updates the local logical clock (to capture its progress) when it executes an event, whether send, receive, or internal" [Raynal and Singhal, 1996].
    const fn tick(&mut self) -> u64 {
        self.time += 1;
        self.time
    }

    /// Update the clock on receiving a message with a remote timestamp and return the updated time.
    ///
    /// "R2. Each message piggybacks the clock value of its sender at sending time. When $p_{i}$ receives a message with the timestamp $C_{msg}$,
    /// it executes the following actions:
    /// 1. $C_{i} := max(C_{i}, C_{msg})$.
    /// 2. Execute R1.
    /// 3. Deliver the message" [Raynal and Singhal, 1996].
    ///
    /// "R2. This governs how a process updates its global logical clock to update its view of the global time and global progress.
    /// It dictates what information about the logical time a process piggybacks in a message and how the receiving process uses this information to update its view of the global time" [Raynal and Singhal, 1996].
    fn update(&mut self, remote_time: u64) -> u64 {
        self.time = max(self.time, remote_time) + 1;
        self.time
    }

    /// Read the current time of the clock without modifying it.
    const fn read(&self) -> u64 {
        self.time
    }
}

/// Message structure that encapsulates the sender ID, timestamp, and content of the message.
pub struct Message {
    sender_id: usize,
    timestamp: u64,
    content: String,
}

/// Event structure for centralized total-order logging in the Lamport clock simulation.
pub struct Event {
    process_id: usize,
    time: u64,
    description: String,
}

impl Event {
    /// Constructor for `Event`.
    const fn new(process_id: usize, time: u64, description: String) -> Self {
        Self {
            process_id,
            time,
            description,
        }
    }

    /// Return the total-order key for the event: (logical time, process ID).
    /// This implements the tie-breaker using a fixed linear ordering of processes:
    /// "To break ties, we use any arbitrary total ordering $\prec$ of the processes.
    /// More precisely, we define a relation $\Rightarrow$ as follows:
    /// if $a$ is an event in process $P_{i}$ and $b$ is an event in process $P_{j}$,
    /// then $a \Rightarrow b$ if and only if either (i) $C_{i}(a) < C_{j}(b)$ or (ii) $C_{i}(a) = C_{j}(b)$ and $P_{i} \prec P_{j}$.
    /// It is easy to see that this defines a total ordering, and that the Clock Condition implies that if $a \rightarrow b$ then $a \Rightarrow b$.
    /// In other words, the relation $\Rightarrow$ is a way of completing the "happened before" partial ordering to a total ordering" [Lamport, 1978].
    const fn total_order_key(&self) -> (u64, usize) {
        (self.time, self.process_id)
    }
}

/// Spawn a centralized logger that prints events in total order.
#[must_use]
pub fn spawn_event_logger(receiver: Receiver<Event>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut events = Vec::new();
        while let Ok(event) = receiver.recv() {
            events.push(event);
        }

        // Sort events by total order: (logical time, process ID).
        events.sort_by_key(Event::total_order_key);

        println!("\n--- Lamport Clock Total Order Log (Logical Time, Process ID) ---");
        for event in events {
            println!(
                "[Time: {}, Process: {}] {}",
                event.time, event.process_id, event.description
            );
        }
    })
}

/// Node structure that encapsulates a Lamport clock, a receiver for incoming messages, a map of peer nodes to send messages to, and an event sender for logging.
///
/// It represents a process in the distributed system.
pub struct Node {
    id: usize,
    clock: LamportClock,
    receiver: Receiver<Message>,
    peers: HashMap<usize, Sender<Message>>,
    event_sender: Sender<Event>,
}

impl Node {
    /// Constructor for `Node` that initializes (and owns) a Lamport clock, a receiver, a map of peer nodes, and an event sender, and runs the event loop for a specified duration.
    #[must_use]
    pub fn spawn(
        id: usize,
        receiver: Receiver<Message>,
        peers: HashMap<usize, Sender<Message>>,
        event_sender: Sender<Event>,
        duration: Duration,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut node = Self {
                id,
                clock: LamportClock::new(),
                receiver,
                peers,
                event_sender,
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
            // Sleep for a random short duration to simulate time between events.
            let sleep_ms = rng.random_range(10..50);
            thread::sleep(Duration::from_millis(sleep_ms));
            // Choose a "random" action: internal event, send event, or receive event.
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
        self.log_event(t, "internal event".to_string());
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

        let peers_ids: Vec<usize> = self.peers.keys().copied().collect();
        let target_id = peers_ids[rng.random_range(0..peers_ids.len())];

        if let Some(tx) = self.peers.get(&target_id) {
            match tx.send(msg) {
                Ok(()) => {
                    println!(
                        "Process {} sent message to process {} at (logical) time {}.",
                        self.id, target_id, t
                    );
                    self.log_event(t, format!("send -> process {target_id}"));
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
                        self.id, msg.sender_id, msg.timestamp, msg.content, prev_time, updated_time,
                    );
                    self.log_event(
                        updated_time,
                        format!("receive <- process {}", msg.sender_id),
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

    /// Log an event by sending it to the centralized event logger.
    fn log_event(&self, time: u64, description: String) {
        let _ = self
            .event_sender
            .send(Event::new(self.id, time, description));
    }
}
