import hashlib
import random
import time
import matplotlib.pyplot as plt
from collections import defaultdict

class PeerNode:
    def __init__(self, peer_id, total_peers):
        self.peer_id = peer_id
        self.total_peers = total_peers

    def hash_function(self, topic):
        hash_val = int(hashlib.sha256(topic.encode()).hexdigest(), 16)
        responsible_peer = hash_val % self.total_peers
        return bin(responsible_peer)[2:].zfill(len(self.peer_id))  # Returns the binary id

def time_hash_function(topics):
    times = []
    for topic in topics:
        start_time = time.time()
        peer = PeerNode('000', 8)  # Example with 8 peers
        peer.hash_function(topic)
        end_time = time.time()
        times.append(end_time - start_time)
    return times

def test_distribution(num_topics, total_peers):
    topic_distribution = defaultdict(int)
    for i in range(num_topics):
        topic_name = f"Topic_{random.randint(1, 10000)}"  # Random topic names
        peer = PeerNode('000', total_peers)
        responsible_peer = peer.hash_function(topic_name)
        topic_distribution[responsible_peer] += 1
    return topic_distribution

def plot_time_cost(times):
    plt.figure(figsize=(10, 6))
    plt.plot(times, marker='o', linestyle='-', color='b')
    plt.title('Hash Function Execution Time')
    plt.xlabel('Topic Index')
    plt.ylabel('Time (seconds)')
    plt.grid(True)
    plt.savefig('graphs/hash_function_time.svg')
    plt.close()

def plot_distribution(distribution, total_topics):
    peers = list(distribution.keys())
    counts = list(distribution.values())
    
    plt.figure(figsize=(10, 6))
    plt.bar(peers, counts, color='g')
    plt.title('Topic Distribution Among Peers')
    plt.xlabel('Peer ID')
    plt.ylabel('Number of Topics')
    plt.xticks(rotation=45)
    plt.grid(axis='y')
    plt.savefig('graphs/hash_function_topic_distribution.svg')
    plt.close()

def main():
    # Part A: Time complexity of hash function
    topic_lengths = [f"TestTopic_{i}" for i in range(1, 101)]  # 100 different topics
    times = time_hash_function(topic_lengths)
    plot_time_cost(times)

    # Part B: Distribution of topics among peers
    num_topics = 1000
    total_peers = 8
    distribution = test_distribution(num_topics, total_peers)
    plot_distribution(distribution, num_topics)

if __name__ == "__main__":
    main()
