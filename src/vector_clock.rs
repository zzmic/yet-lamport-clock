use rand::Rng;
use std::cmp::Ordering;
use std::cmp::max;
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

/// Vector Clock structure that encapsulates the vector of logical clock values (`times`) for each process.
///
/// Invariant: Each logical time is monotonically non-decreasing and strictly increases on local events.
pub struct VectorClock {
    times: Vec<u64>,
}

impl VectorClock {
    /// Initialize a new vector clock with all times initialized to zero.
    fn new(num_nodes: usize) -> Self {
        Self {
            times: vec![0; num_nodes],
        }
    }

    /// Increment the clock for a specific process (node) on an event.
    /// "R1. Before executing an event, $p_{i}$ updates its local logical time as follows:
    /// $vt_{i}[i] := vt_{i}[i] + d (d > 0)$," where, "$d$ is typically kept at $1$,
    /// since this allows a process to identify the time of each event uniquely at a process while minimizing $d$'s rate of increase" [Raynal and Singhal, 1996].
    fn tick(&mut self, process_id: usize) {
        assert!(
            process_id < self.times.len(),
            "Process ID {} is out of bounds for VectorClock of size {}",
            process_id,
            self.len()
        );
        self.times[process_id] += 1;
    }

    /// Update the vector clock on receiving a message with a remote vector timestamp.
    ///
    /// "R2. Each sender process piggybacks a message $m$ with its vector clock value at sending time.
    /// Upon receiving such a message $(m, vt)$, $p_{i}$ executes the following sequence of actions:
    /// 1. Update its logical global time as follows: $1 \leq k \leq n: vt_{i}[k] := \max(vt_{i}[k], vt[k])$.
    /// 2. Execute R1.
    /// 3. Deliver the message $m$" [Raynal and Singhal, 1996].
    fn update(&mut self, other: &Self, my_process_id: usize) {
        assert!(
            self.len() == other.len(),
            "Vector clocks must be of the same length"
        );
        for (local_time, remote_time) in self.times.iter_mut().zip(other.times.iter()) {
            *local_time = max(*local_time, *remote_time);
        }
        self.tick(my_process_id);
    }

    /// Return the length of the vector clock.
    const fn len(&self) -> usize {
        self.times.len()
    }

    /// Read the full vector clock without modifying it.
    fn read(&self) -> &[u64] {
        &self.times
    }

    /// Clone the vector clock.
    fn clone(&self) -> Self {
        Self {
            times: self.times.clone(),
        }
    }
}

/// Implement partial ordering for vector clocks.
/// The default-derived implementation does (may) not capture the semantics of vector clocks and instead
/// relies on lexicographical ordering.
impl PartialOrd for VectorClock {
    /// Compare two vector clocks according to the vector clocks' partial-ordering semantics.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mut less = false;
        let mut greater = false;

        for (t0, t1) in self.times.iter().zip(other.times.iter()) {
            if t0 < t1 {
                less = true;
            } else if t0 > t1 {
                greater = true;
            }
        }

        if greater && less {
            None
        } else if greater {
            Some(Ordering::Greater)
        } else if less {
            Some(Ordering::Less)
        } else {
            Some(Ordering::Equal)
        }
    }
}

/// Implement equality for vector clocks (required for `PartialOrd`).
impl PartialEq for VectorClock {
    /// Check equality of two vector clocks by (structurally) comparing their time vectors.
    fn eq(&self, other: &Self) -> bool {
        self.times == other.times
    }
}

/// Message structure that encapsulates the sender ID, vector timestamp, and content of the message.
pub struct Message {
    sender_id: usize,
    timestamps: VectorClock,
    content: String,
}

/// Event structure for centralized total-order logging in the vector clock simulation.
pub struct Event {
    process_id: usize,
    local_time: u64,
    clock_sum: u64,
    clock: Vec<u64>,
    description: String,
}

impl Event {
    /// Constructor for `Event`.
    fn new(process_id: usize, clock: &VectorClock, description: String) -> Self {
        let times = clock.read();
        Self {
            process_id,
            local_time: times[process_id],
            clock_sum: times.iter().sum(),
            clock: times.to_vec(),
            description,
        }
    }

    /// Return the total-order key for the event: (clock sum, process ID).
    ///
    /// The scalar component is $\sum_{k} vt[k]$, the sum of the vector clock's components,
    /// rather than the local component $vt[i]$. The local component alone is *not* a valid
    /// scalar ordering for vector time: $vt_{i}[i]$ counts only the events of $p_{i}$, so two
    /// events on distinct processes are ordered by unrelated counters and a receive event can
    /// sort ahead of the send that caused it. The sum, on the contrary, satisfies the Clock
    /// Condition. It strictly increases along every causal chain, since R1 adds $d = 1$ to one
    /// component while leaving the rest fixed, and R2 takes a component-wise maximum
    /// (non-decreasing in every component) before executing R1. Hence $a \rightarrow b$ implies
    /// $\sum_{k} vt(a)[k] < \sum_{k} vt(b)[k]$, and sorting on the sum yields a linear
    /// extension of the "happened before" partial order.
    ///
    /// Ties (which are exactly the concurrent events the partial order leaves unrelated) are
    /// broken by the fixed linear ordering of processes, following Lamport's construction:
    /// "To break ties, we use any arbitrary total ordering $\prec$ of the processes.
    /// More precisely, we define a relation $\Rightarrow$ as follows:
    /// if $a$ is an event in process $P_{i}$ and $b$ is an event in process $P_{j}$,
    /// then $a \Rightarrow b$ if and only if either (i) $C_{i}(a) < C_{j}(b)$ or (ii) $C_{i}(a) = C_{j}(b)$ and $P_{i} \prec P_{j}$.
    /// It is easy to see that this defines a total ordering, and that the Clock Condition implies that if $a \rightarrow b$ then $a \Rightarrow b$.
    /// In other words, the relation $\Rightarrow$ is a way of completing the "happened before" partial ordering to a total ordering" [Lamport, 1978].
    const fn total_order_key(&self) -> (u64, usize) {
        (self.clock_sum, self.process_id)
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

        // Sort events by total order: (clock sum, process ID).
        events.sort_by_key(Event::total_order_key);

        println!("\n--- Vector Clock Total Order Log (Clock Sum, Process ID) ---");
        for event in events {
            println!(
                "[Sum: {}, Local: {}, Process: {}] {} | VC = {:?}",
                event.clock_sum, event.local_time, event.process_id, event.description, event.clock
            );
        }
    })
}

/// Node structure that encapsulates a vector clock, a receiver for incoming messages,
/// a map of peer nodes to send messages to, and a logger to (centrally) log events.
pub struct Node {
    id: usize,
    clock: VectorClock,
    receiver: Receiver<Message>,
    peers: HashMap<usize, Sender<Message>>,
    event_sender: Sender<Event>,
}

impl Node {
    /// Spawn a new thread for the node (process) that runs the event loop for a specified duration.
    #[must_use]
    pub fn spawn(
        id: usize,
        num_nodes: usize,
        receiver: Receiver<Message>,
        peers: HashMap<usize, Sender<Message>>,
        event_sender: Sender<Event>,
        duration: Duration,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut node = Self {
                id,
                clock: VectorClock::new(num_nodes),
                receiver,
                peers,
                event_sender,
            };
            node.run_event_loop(duration);
        })
    }

    /// Run the event loop for the process, simulating internal events, send events, and receive events.
    pub fn run_event_loop(&mut self, duration: Duration) {
        let start_time = Instant::now();
        let mut rng = rand::rng();
        let mut step = 0u64;

        println!(
            "Process {} started at (vector) time {:?}.",
            self.id,
            self.clock.read()
        );

        while start_time.elapsed() < duration {
            // Sleep for a random short duration to simulate time between events.
            let sleep_ms = rng.random_range(10..50);
            thread::sleep(Duration::from_millis(sleep_ms));

            // Perform periodic bursts to intentionally create different causal relations.
            // - Internal burst: pushes the local clock ahead so later messages often become happened-before.
            // - Receive burst: processes messages quickly so some become happened-after.
            step += 1;
            if step.is_multiple_of(9) {
                let burst = rng.random_range(3..7);
                for _ in 0..burst {
                    self.handle_internal_event();
                }
                continue;
            } else if step.is_multiple_of(5) {
                for _ in 0..3 {
                    self.process_incoming_messages();
                }
                continue;
            }

            // Choose a "random" action: internal event, send event, or receive event.
            let action_choice = rng.random_range(0..100);
            if action_choice < 45 {
                self.process_incoming_messages();
            } else if action_choice < 70 {
                self.handle_internal_event();
            } else {
                self.handle_send_event(&mut rng);
            }
        }

        println!(
            "Process {} finished at (vector) time {:?}.",
            self.id,
            self.clock.read()
        );
    }

    /// Handle an internal event by ticking the vector clock and logging the event.
    pub fn handle_internal_event(&mut self) {
        self.clock.tick(self.id);
        println!(
            "Process {} performed internal event at (vector) time {:?}.",
            self.id,
            self.clock.read()
        );
        self.log_event("Internal Event".to_string());
    }

    /// Handle a send event by ticking the vector clock, sending a message to a specific peer, and logging the event.
    pub fn handle_send_event<R: Rng>(&mut self, rng: &mut R) {
        if self.peers.is_empty() {
            return;
        }

        self.clock.tick(self.id);

        let msg = Message {
            sender_id: self.id,
            timestamps: self.clock.clone(),
            content: format!(
                "Message from process {} at (vector) time {:?}",
                self.id,
                self.clock.read()
            ),
        };

        let peers_ids: Vec<usize> = self.peers.keys().copied().collect();
        let target_id = peers_ids[rng.random_range(0..peers_ids.len())];

        if let Some(sender) = self.peers.get(&target_id) {
            match sender.send(msg) {
                Ok(()) => {
                    println!(
                        "Process {} sent message to process {} at (vector) time {:?}.",
                        self.id,
                        target_id,
                        self.clock.read()
                    );
                    self.log_event(format!("Send -> Process {target_id}"));
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

    /// Handle an event of processing incoming messages, updating the vector clock, and logging the event along with causal relationship(s).
    pub fn process_incoming_messages(&mut self) {
        loop {
            match self.receiver.try_recv() {
                Ok(msg) => {
                    let prev_clock = self.clock.clone();

                    let relation = match msg.timestamps.partial_cmp(&prev_clock) {
                        Some(Ordering::Less) => "happened-before",
                        Some(Ordering::Greater) => "happened-after",
                        Some(Ordering::Equal) => "identical-to",
                        None => "concurrent-with",
                    };

                    println!(
                        "Process {} received message from process {} at (vector) time {:?}: {}",
                        self.id,
                        msg.sender_id,
                        self.clock.read(),
                        msg.content
                    );
                    println!("  -> Causal Relationship: {relation}");

                    self.clock.update(&msg.timestamps, self.id);
                    println!(
                        "  -> Clock updated from {:?} to {:?}.",
                        prev_clock.read(),
                        self.clock.read()
                    );
                    self.log_event(format!("Receive <- Process {}", msg.sender_id));
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
    fn log_event(&self, description: String) {
        let _ = self
            .event_sender
            .send(Event::new(self.id, &self.clock, description));
    }
}
