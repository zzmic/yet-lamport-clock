use rand::Rng;
use std::cmp::Ordering;
use std::cmp::max;
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

/// Vector Clock structure that encapsulates the vector of logical clock values (times) for each process.
/// Invariant: Each logical time is monotonically non-decreasing and strictly increases on local events.
pub struct VectorClock {
    times: Vec<u64>,
}

impl VectorClock {
    /// Initialize a new vector clock with all times initialized to zero.
    fn new(num_nodes: usize) -> Self {
        VectorClock {
            times: vec![0; num_nodes],
        }
    }

    /// Increment the clock for a specific process (node) on an event.
    /// "R1. Before executing an event, $p_{i}$ updates its local logical time as follows:
    /// $vt_{i}[i] := vt_{i}[i] + d (d > 0)$," where, "$d$ is typically kept at $1$,
    /// since this allows a process to identify the time of each event uniquely at a process while minimizing $d$'s rate of increase" [Raynal and Singhal, 1996].
    fn tick(&mut self, node_id: usize) {
        if node_id >= self.times.len() {
            panic!(
                "Node ID {} is out of bounds for VectorClock of size {}",
                node_id,
                self.len()
            );
        }
        self.times[node_id] += 1;
    }

    /// Update the vector clock on receiving a message with a remote vector timestamp.
    /// "R2. Each sender process piggybacks a message $m$ with its vector clock value at sending time.
    /// Upon receiving such a message $(m, vt)$, $p_i$ executes the following sequence of actions:
    /// 1. Update its logical global time as follows:
    ///   $1 \leq k \leq n: vt_i[k] := \max(vt_i[k], vt[k])$
    /// 2. Execute R1.
    /// 3. Deliver the message $m$" [Raynal and Singhal, 1996].
    fn update(&mut self, other: &VectorClock, my_node_id: usize) {
        assert!(
            self.len() == other.len(),
            "Vector clocks must be of the same length"
        );
        for (local_time, remote_time) in self.times.iter_mut().zip(other.times.iter()) {
            *local_time = max(*local_time, *remote_time);
        }
        self.tick(my_node_id);
    }

    /// Return the length of the vector clock.
    fn len(&self) -> usize {
        self.times.len()
    }

    /// Read the full vector clock without modifying it.
    fn read(&self) -> &[u64] {
        &self.times
    }

    /// Clone the vector clock.
    fn clone(&self) -> Self {
        VectorClock {
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

/// Node structure that encapsulates a vector clock, a receiver for incoming messages,
/// a map of peer nodes to send messages to, and a logger to (centrally) log events.
pub struct Node {
    id: usize,
    clock: VectorClock,
    receiver: Receiver<Message>,
    peers: HashMap<usize, Sender<Message>>,
}

impl Node {
    /// Spawn a new thread for the node (process) that runs the event loop for a specified duration.
    pub fn spawn(
        id: usize,
        num_nodes: usize,
        receiver: Receiver<Message>,
        peers: HashMap<usize, Sender<Message>>,
        duration: Duration,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut node = Node {
                id,
                clock: VectorClock::new(num_nodes),
                receiver,
                peers,
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
            if step % 9 == 0 {
                let burst = rng.random_range(3..7);
                for _ in 0..burst {
                    self.handle_internal_event();
                }
                continue;
            } else if step % 5 == 0 {
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

        let peers_ids: Vec<usize> = self.peers.keys().cloned().collect();
        let target_id = peers_ids[rng.random_range(0..peers_ids.len())];

        if let Some(tx) = self.peers.get(&target_id) {
            match tx.send(msg) {
                Ok(_) => {
                    println!(
                        "Process {} sent message to process {} at (vector) time {:?}.",
                        self.id,
                        target_id,
                        self.clock.read()
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
                    println!("  -> Causal Relationship: {}", relation);

                    self.clock.update(&msg.timestamps, self.id);
                    println!(
                        "  -> Clock updated from {:?} to {:?}.",
                        prev_clock.read(),
                        self.clock.read()
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
