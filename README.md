
Pipeline State Store 

This project implements a low-latency, in-memory state store designed for data engineering workloads such as:
- Streaming deduplication
- Incremental batch processing checkpoints
- Lightweight pipeline metadata tracking

The system is implemented as a single-threaded, event-driven TCP server using non-blocking sockets and I/O multiplexing. It prioritizes predictable latency and deterministic state updates over raw throughput.

It is a purpose-built state backend for data pipelines.
