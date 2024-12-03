# Decentralized P2P System with DHT and Hypercube Topology

## Overview
The goal of this project is to enhance a distributed peer-to-peer (P2P) system based on the previous assignment (PA3). This system achieve mainly two functions: (1) Replicated Topics (Performance optimization and Fault tolerance); (2) Dynamic Topology Configuration. Specifically, this system implements the core functions of each node in the decentralized P2P system, including topic management, message routing and forwarding, network communication, and event logging. It is also responsible for coordination and distributed operations between nodes.

## Features
- **Distributed Hash Table (DHT):** Ensures efficient topic distribution across nodes.
- **Hypercube Topology:** Allows optimized routing and request forwarding.
- **Publish/Subscribe API:** Supports topic creation, deletion, publishing messages, and subscribing to topics.
- **Benchmarking:** Evaluates latency and throughput for various APIs.
- **Replicated Topics:** (1) Deciding When and Where to Place Replicas: This requires an algorithm to determine whether replication is beneficial and where the replicas should reside, potentially based on client proximity or access frequency. (2) Consistency Model Selection: Replicated data introduces synchronization overhead. The consistency model (e.g., eventual consistency, strict consistency) impacts both system performance and correctness. A balance between latency and overhead must be achieved.
- **Fault Tolerance:** (1) Failure Detection: Nodes must detect failed peers and avoid forwarding requests to them. (2) Forwarding to Replicas: Requests intended for failed nodes should be routed to nodes hosting replicas of the target topics.
## Requirements
To install the required dependencies, use the `requirements.txt` file provided. Run the following command:
```bash
pip install -r requirements.txt
```

## Setup
**Step 1: Configure and Start Peers**
1. Configure Nodes: Set up 8 peer nodes, either on the same machine or across different machines. Each peer will have a unique binary identifier.
2. Run Nodes: Run each peer node using the following command:
```bash
python peer_node.py --id <binary_id> --ip <IP_address> --port <port_number>
```
Example:
```bash
python peer_node.py --id 000 --ip 127.0.0.1 --port 6000
```
Replace <binary_id>, <IP_address>, and <port_number> with values specific to each node.

**Step 2: Running the Benchmark Tests**
Use the `benchmark_apis.py` file to evaluate the performance of each API and generate latency/throughput graphs.
1. Run the following command to start benchmarking:
```bash
python benchmark_apis.py
```
2. This script will create .svg files of graphs showing latency and throughput across nodes.

**Step 3: Verify Functionality**
To verify that the APIs and forwarding mechanisms work as expected, use test_apis.py:
```bash
python test_apis.py
```
This test will:
- Validate each API on each node.
- Ensure correct request forwarding across nodes.
- To achieve these two functions: replicated topics for performance optimization and fault tolerance; dynamic topology configuration (add & removal of Hypercube nodes).

## Experiments
Experiments are designed to test various aspects of the system:

1. Hash Function: Assess time complexity, distribution of topics, and runtime cost.
2. Request Forwarding: Verify functionality, measure average response time, and assess maximum throughput.
3. Fault Tolerance: The assumption that all nodes operate reliably no longer holds. The system must handle node failures with Failure Detection and Forwarding to Replicas.
4. The system must support runtime addition and removal of nodes in a hypercube topology. The challenges include Handling Node Unavailability and Node Recovery.

## File Descriptions
- peer_node.py: Main file for initializing and running individual peer nodes. It implements the core functions of each node in the decentralized P2P system, including topic management, message routing and forwarding, network communication, and event logging. It is also responsible for coordination and distributed operations between nodes.
- benchmark_apis.py: Script for benchmarking each API, measuring latency and throughput.
- test_apis.py: Script for functional testing of each API and request forwarding.
- requirements.txt: Contains all necessary Python packages.
- test_hash_function.py: Implementing detailed distributed hashing algorithms.

## Conclusion
This project extends a P2P system with advanced features to support replication, fault tolerance, and dynamic topology configuration. These enhancements aim to improve system reliability, performance, and adaptability in dynamic environments. The implementation must consider trade-offs between performance, consistency, and overhead while meeting the additional requirements of concurrency and scalability.
