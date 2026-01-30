use std::collections::HashMap;
use std::env;
use std::sync::mpsc::{self};
use std::time::Duration;

/// Number of nodes in the distributed system.
static NUM_NODES: usize = 5;
/// Duration to run the simulation.
static DURATION: Duration = Duration::from_secs(5);

/// Run the Lamport clock simulation.
fn run_lamport_clock_simulation() {
    println!(
        "Starting Lamport Clock Simulation with {} nodes for {:?}...",
        NUM_NODES, DURATION
    );

    let (event_sender, event_receiver) = mpsc::channel();
    let logger_handle = clock::lamport_clock::spawn_event_logger(event_receiver);

    let mut senders = Vec::with_capacity(NUM_NODES);
    let mut receivers = Vec::with_capacity(NUM_NODES);
    for _ in 0..NUM_NODES {
        let (sender, receiver) = mpsc::channel();
        senders.push(sender);
        receivers.push(Some(receiver));
    }

    let mut handles = Vec::new();
    for id in 0..NUM_NODES {
        let mut peers = HashMap::new();
        for peer_id in 0..NUM_NODES {
            if id != peer_id {
                peers.insert(peer_id, senders[peer_id].clone());
            }
        }
        let receiver = receivers[id].take().expect("Receiver should be present");
        handles.push(clock::lamport_clock::Node::spawn(
            id,
            receiver,
            peers,
            event_sender.clone(),
            DURATION,
        ));
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    drop(event_sender);
    logger_handle.join().expect("Logger thread panicked");

    println!("Simulation complete.");
}

/// Run the Vector clock simulation.
fn vector_clock_simulation() {
    println!(
        "Starting Vector Clock Simulation with {} nodes for {:?}...",
        NUM_NODES, DURATION
    );

    let (event_sender, event_receiver) = mpsc::channel();
    let logger_handle = clock::vector_clock::spawn_event_logger(event_receiver);

    let mut senders = Vec::with_capacity(NUM_NODES);
    let mut receivers = Vec::with_capacity(NUM_NODES);

    for _ in 0..NUM_NODES {
        let (sender, receiver) = mpsc::channel();
        senders.push(sender);
        receivers.push(Some(receiver));
    }

    let mut handles = Vec::new();

    for id in 0..NUM_NODES {
        let mut peers = HashMap::new();
        for peer_id in 0..NUM_NODES {
            if id != peer_id {
                peers.insert(peer_id, senders[peer_id].clone());
            }
        }
        let receiver = receivers[id].take().expect("Receiver should be present");
        handles.push(clock::vector_clock::Node::spawn(
            id,
            NUM_NODES,
            receiver,
            peers,
            event_sender.clone(),
            DURATION,
        ));
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    drop(event_sender);
    logger_handle.join().expect("Logger thread panicked");

    println!("Vector Clock Simulation complete.");
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let usage = || {
        eprintln!("Usage: {} <vector-clock|lamport-clock>", args[0]);
        eprintln!("  vector-clock   Run the Vector Clock simulation");
        eprintln!("  lamport-clock  Run the Lamport Clock simulation");
        eprintln!("  -h, --help     Show this help message");
    };

    match args.get(1).map(String::as_str) {
        Some("vector-clock") => vector_clock_simulation(),
        Some("lamport-clock") => run_lamport_clock_simulation(),
        Some("-h") | Some("--help") => usage(),
        Some(other) => {
            eprintln!("Unknown argument: {}", other);
            usage();
            std::process::exit(1);
        }
        None => {
            usage();
            std::process::exit(1);
        }
    }
}
