use lamport_clock::Node;
use std::collections::HashMap;
use std::sync::mpsc::{self};
use std::time::Duration;

/// Number of nodes in the distributed system.
static NUM_NODES: usize = 5;
/// Duration to run the simulation.
static RUN_TIME: Duration = Duration::from_secs(5);

fn main() {
    println!(
        "Starting Lamport Clock Simulation with {} nodes for {:?}...",
        NUM_NODES, RUN_TIME
    );

    let mut senders = Vec::new();
    let mut receivers = Vec::new();
    for _ in 0..NUM_NODES {
        let (sender, receiver) = mpsc::channel();
        senders.push(sender);
        receivers.push(Some(receiver));
    }

    let mut handles = Vec::new();
    for id in 0..NUM_NODES {
        let mut peers = HashMap::new();
        for peed_id in 0..NUM_NODES {
            if id != peed_id {
                peers.insert(peed_id, senders[peed_id].clone());
            }
        }
        let receiver = receivers[id].take().expect("Receiver should be present");
        handles.push(Node::spawn(id, receiver, peers, RUN_TIME));
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    println!("Simulation complete.");
}
