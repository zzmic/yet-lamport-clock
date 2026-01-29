# yet-lamport-clock

## Overview

**`yet-lamport-clock`** is, perhaps, yet another implementation, with simulation, of [_Lamport clocks_](https://doi.org/10.1145/359545.359563) and its extension and generalization, _vector clocks_, in Rust [Lamport, 1978][Raynal and Singhal, 1996].

It implements Leslie Lamport's logical clock protocol to establish partial ordering (and total ordering with tie breaking) of events in distributed executions. The implementation includes both the core `clock::lamport_clock::LamportClock` data structure and a multi-threaded simulation framework that demonstrates the clock's behavior in a network of processes communicating via message passing. On top of that, it also implements vector clocks in the `clock::vector_clock::VectorClock` data structure, which overcomes some limitations of Lamport clocks in capturing causality.

## How Things Work

The Lamport clock simulation creates a network of `N` processes (default: 5, configurable) that run concurrently for a specified duration (default: 5 seconds, configurable). Each process:

1. Maintains its own `LamportClock` with a (monotonically) increasing logical clock (time) that starts at zero and _strictly_ increases on each event.
2. "Randomly" performs internal events, sends messages to peers, or processes incoming messages.
3. Updates its logical clock according to Lamport's rules on updating the clock (in "scalar time representation") upon each event [Raynal and Singhal, 1996].
4. Logs all events with their logical timestamps for analysis.
5. Emits events to a centralized logger, which produces a _total order_ by sorting on `(logical time, process ID)` to break ties, and prints the logical timestamp for each event.

The Lamport clock captures the **_happens-before_** relation across processes through message passing and clock updates [van Steen and Tanenbaum, 2023]:

> 1. If `a` and `b` are events in the same process and `a` occurs before `b`, then `C(a) < C(b)`, where `C(x)` is the valuation of the Lamport clock at event `x`.
> 2. If `a` is the event of sending a message in process `P_i` and `b` is the event of receiving _that_ message in process `P_j`, then `C(a) < C(b)`. A message cannot be received before it is sent, nor can it be received simultaneously with its sending, as messages take a non-zero (but finite) time to travel from sender to receiver.

Nevertheless, Lamport clocks do not capture causality (at least not fully); i.e., for any pair of events `a` and `b`, if `C(a) < C(b)`, where `C(x)` is the valuation of the Lamport clock at event `x`, it does not necessarily imply that `a` happened before `b`. This limitation is addressed by vector clocks, which are implemented in the `clock::vector_clock::VectorClock` data structure.

The vector clock simulation creates the same network of `N` processes and runs for the same duration, where each process:

1. Maintains a `VectorClock` (a vector of logical clocks) initialized to zeros.
2. "Randomly" performs internal events, sends messages to peers, or processes incoming messages, updating its vector clock accordingly.
3. Merges incoming vector timestamps using element-wise maxima, then ticks its own component.
4. Logs events along with the inferred causal relation between the local clock and received timestamps.
5. Emits events to a centralized logger, which produces a _total order_ by sorting on `(local time, process ID)` for tie breaking, and prints the vector timestamp for each event.

The vector clock(s) capture causality by maintaining the following properties [van Steen and Tanenbaum, 2023]:

> 1. `VC[i]` is the number of events that have occurred so far at process `P_i`, where `VC` is the vector clock. In other words, `VC_{i}[i]` is the local logical clock at process `P_i`.
> 2. If `VC_{i}[j] = k`, then process `P_i` knows that `k` events have occurred at process `P_j` (up to the latest message received from `P_j`). It is thus `P_i`'s knowledge of the local logical clock at process `P_j`.

A further extension of vector clocks that can be similarly implemented (but with a much higher complexity, if implemented in a naive fashion) is _matrix clocks_, where each process maintains a matrix of logical clocks, capturing not only its knowledge of other processes' logical clocks but also each process's knowledge about every other process's knowledge.

## Running the Simulation

To run the Lamport clock simulation, execute the following command in the terminal:

```bash
cargo run -- lamport-clock
```

To run the vector clock simulation, execute:

```bash
cargo run -- vector-clock
```

## Bibliography

For definitions and explanations, please refer to [bib.bib](https://github.com/zzmic/yet-lamport-clock/blob/main/bib.bib), which contains the references, including quotes, used in this project.

## Disclaimer

This project is _not_ an orthodox implementation or artifact from the referenced literature. Therefore, I should be accountable for any errors and flaws in the implementation and documentation, not the original authors of the sources.
