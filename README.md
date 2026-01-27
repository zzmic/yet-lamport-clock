# yet-lamport-clock

## Overview

**`yet-lamport-clock`** is, perhaps, yet another implementation of [Lamport Clock](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) (with simulation) in Rust.

It implements Leslie Lamport's logical clock protocol to establish partial ordering of events in distributed systems. The implementation includes both the core `LamportClock` data structure and a multi-threaded simulation framework that demonstrates the clock's behavior in a network of processes communicating via message passing.

### How It Works

The simulation creates a network of `N` processes (default: 5, configurable) that run concurrently for a specified duration (default: 5 seconds, configurable). Each process:

1. Maintains its own `LamportClock` with a (monotonically) increasing logical time that starts at zero and _strictly_ increases on each event.
2. Randomly (with roughly equal probability) performs internal events, sends messages to peers, or processes incoming messages.
3. Updates its logical clock according to Lamport's rules on updating the clock (in "scalar time representation") upon each event.
4. Logs all events with their logical timestamps for analysis.

The system ensures the **_happens-before_** relation across processes through message passing and clock updates:

> 1. If `a` and `b` are events in the same process and `a` occurs before `b`, then `C(a) < C(b)`, where `C(x)` is the valuation of the Lamport clock at event `x`.
> 2. If `a` is the event of sending a message in process `P_i` and `b` is the event of receiving _that_ message in process `P_j`, then `C(a) < C(b)`.

## Bibliography

For definitions and explanations, please refer to [bib.bib](https://github.com/zzmic/yet-lamport-clock/blob/main/bib.bib), which contains the references (including quotes) used in this project.
