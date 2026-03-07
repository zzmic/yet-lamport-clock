use std::collections::HashMap;
use std::sync::mpsc::{self};
use std::time::Duration;
use std::{env, io};

/// Number of nodes in the distributed system.
static NUM_NODES: usize = 5;
/// Duration to run the simulation.
static DURATION: Duration = Duration::from_secs(5);

/// Run the Lamport clock simulation.
fn run_lamport_clock_simulation() -> Result<(), io::Error> {
    println!("Starting Lamport Clock Simulation with {NUM_NODES} nodes for {DURATION:?}...");

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
    for (id, receiver) in receivers.iter_mut().enumerate().take(NUM_NODES) {
        let mut peers = HashMap::new();
        for (peer_id, sender) in senders.iter().enumerate().take(NUM_NODES) {
            if id != peer_id {
                peers.insert(peer_id, sender.clone());
            }
        }
        let receiver = receiver
            .take()
            .ok_or_else(|| io::Error::other(format!("Failed to take a receiver for node {id}")))?;
        handles.push(clock::lamport_clock::Node::spawn(
            id,
            receiver,
            peers,
            event_sender.clone(),
            DURATION,
        ));
    }

    for handle in handles {
        handle.join().map_err(|e| {
            io::Error::other(format!(
                "Thread panicked while waiting for the node to finish: {e:?}"
            ))
        })?;
    }

    drop(event_sender);
    logger_handle.join().map_err(|e| {
        io::Error::other(format!(
            "Thread panicked while waiting for the logger to finish: {e:?}"
        ))
    })?;

    println!("Simulation complete.");

    Ok(())
}

/// Run the vector clock simulation.
fn run_vector_clock_simulation() -> Result<(), io::Error> {
    println!("Starting Vector Clock Simulation with {NUM_NODES} nodes for {DURATION:?}...");

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

    for (id, receiver) in receivers.iter_mut().enumerate().take(NUM_NODES) {
        let mut peers = HashMap::new();
        for (peer_id, sender) in senders.iter().enumerate().take(NUM_NODES) {
            if id != peer_id {
                peers.insert(peer_id, sender.clone());
            }
        }
        let receiver = receiver
            .take()
            .ok_or_else(|| io::Error::other(format!("Failed to take a receiver for node {id}")))?;
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
        handle.join().map_err(|e| {
            io::Error::other(format!(
                "Thread panicked while waiting for the node to finish: {e:?}"
            ))
        })?;
    }

    drop(event_sender);
    logger_handle.join().map_err(|e| {
        io::Error::other(format!(
            "Thread panicked while waiting for the logger to finish: {e:?}"
        ))
    })?;

    println!("Vector Clock Simulation complete.");

    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let print_usage = || {
        eprintln!("Usage: {} <vector-clock|lamport-clock>", args[0]);
        eprintln!("  vector-clock   Run the Vector Clock Simulation");
        eprintln!("  lamport-clock  Run the Lamport Clock Simulation");
        eprintln!("  -h, --help     Show this help message");
    };

    match args.get(1).map(String::as_str) {
        Some("vector-clock") => {
            if let Err(error) = run_vector_clock_simulation() {
                eprintln!("Error: {error}");
                std::process::exit(1);
            }
        }
        Some("lamport-clock") => {
            if let Err(error) = run_lamport_clock_simulation() {
                eprintln!("Error: {error}");
                std::process::exit(1);
            }
        }
        Some("-h" | "--help") => print_usage(),
        Some(other) => {
            eprintln!("Unknown argument: {other}");
            print_usage();
            std::process::exit(1);
        }
        None => {
            print_usage();
            std::process::exit(1);
        }
    }
}
