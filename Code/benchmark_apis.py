# benchmark.py
import asyncio
import time
import random
import matplotlib.pyplot as plt
from peer_node import PeerNode

async def benchmark_create_topic(peer, topic_name):
    start_time = time.time()
    await peer.create_topic(topic_name)
    end_time = time.time()
    latency = end_time - start_time
    return latency

async def benchmark_publish_message(peer, topic_name, message):
    start_time = time.time()
    await peer.publish_message(topic_name, message)
    end_time = time.time()
    latency = end_time - start_time
    return latency

async def benchmark_subscribe_to_topic(peer, topic_name):
    start_time = time.time()
    await peer.subscribe_to_topic(topic_name)
    end_time = time.time()
    latency = end_time - start_time
    return latency

async def benchmark_delete_topic(peer, topic_name):
    start_time = time.time()
    await peer.delete_topic(topic_name)
    end_time = time.time()
    latency = end_time - start_time
    return latency

async def benchmark_throughput(peers, topic_name, num_requests):
    start_time = time.time()
    tasks = []
    for _ in range(num_requests):
        peer = random.choice(peers)
        tasks.append(peer.publish_message(topic_name, f"Random message {random.randint(1, 1000)}"))
    await asyncio.gather(*tasks)
    end_time = time.time()
    throughput = num_requests / (end_time - start_time)
    return throughput

def create_latency_graph(latencies):
    plt.figure(figsize=(10, 6))
    plt.plot(latencies, marker='o', linestyle='-', color='b')
    plt.title('API Latency Benchmark')
    plt.xlabel('API Call Index')
    plt.ylabel('Latency (seconds)')
    plt.grid(True)
    plt.savefig('graphs/api_latency.svg')
    plt.close()

def create_throughput_graph(throughputs):
    plt.figure(figsize=(10, 6))
    plt.bar(range(len(throughputs)), throughputs, color='g')
    plt.title('API Throughput Benchmark')
    plt.xlabel('Throughput Test Index')
    plt.ylabel('Throughput (requests per second)')
    plt.xticks(range(len(throughputs)))
    plt.grid(axis='y')
    plt.savefig('graphs/api_throughput.svg')
    plt.close()

async def run_benchmark():
    total_peers = 8
    peers = []
    for peer_id in range(total_peers):
        peer = PeerNode(bin(peer_id)[2:].zfill(3), '127.0.0.1', 6000 + peer_id, total_peers)
        peers.append(peer)
    
    topic_name = "BenchmarkTopic"

    # Store latencies for each API call
    latencies = []

    print("Benchmarking topic creation latency...")
    latencies.append(await benchmark_create_topic(peers[0], topic_name))
    
    print("Benchmarking message publishing latency...")
    latencies.append(await benchmark_publish_message(peers[0], topic_name, "Sample message"))
    
    print("Benchmarking subscription latency...")
    latencies.append(await benchmark_subscribe_to_topic(peers[0], topic_name))
    
    print("Benchmarking topic deletion latency...")
    latencies.append(await benchmark_delete_topic(peers[0], topic_name))

    # Benchmark throughput (publishing messages)
    print(f"Benchmarking throughput with 100 requests...")
    throughput = await benchmark_throughput(peers, topic_name, 100)
    
    # Create graphs
    create_latency_graph(latencies)
    create_throughput_graph([throughput])  # single throughput value for bar graph

# Run the benchmark
if __name__ == "__main__":
    asyncio.run(run_benchmark())
