# Decentralized P2P System with DHT and Hypercube Topology

## Overview
This project implements a decentralized peer-to-peer (P2P) system using a Distributed Hash Table (DHT) and hypercube topology. The system enables efficient topic-based publish/subscribe interactions across multiple peer nodes, where each node can create, delete, publish, and subscribe to topics. The project also includes mechanisms for request forwarding, performance benchmarking, and latency/throughput evaluation.

## Features
- **Distributed Hash Table (DHT):** Ensures efficient topic distribution across nodes.
- **Hypercube Topology:** Allows optimized routing and request forwarding.
- **Publish/Subscribe API:** Supports topic creation, deletion, publishing messages, and subscribing to topics.
- **Benchmarking:** Evaluates latency and throughput for various APIs.

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

## Experiments
Experiments are designed to test various aspects of the system:

1. Hash Function: Assess time complexity, distribution of topics, and runtime cost.
2. Request Forwarding: Verify functionality, measure average response time, and assess maximum throughput.

## File Descriptions
- peer_node.py: Main file for initializing and running individual peer nodes.
- benchmark_apis.py: Script for benchmarking each API, measuring latency and throughput.
- test_apis.py: Script for functional testing of each API and request forwarding.
- requirements.txt: Contains all necessary Python packages.

## Conclusion
This project enables efficient, scalable, and decentralized P2P communication, with reliable topic-based interactions and optimized message routing in a hypercube network. Use the benchmarking results to further tune performance and ensure an even distribution of topics across nodes.
