import asyncio
import time
import matplotlib.pyplot as plt
from collections import defaultdict
import random
from peer_node import PeerNode

class Experiment:
    def __init__(self, nodes):
        self.nodes = nodes
        self.response_times = []
        self.throughput_data = defaultdict(list)

    async def test_functionality(self):
        """
        Tests whether each node can access topics on all other nodes by forwarding requests.
        """
        success = True
        for i, node in enumerate(self.nodes):
            topic_name = f"FunctionalTest_Topic_{i}"
            await node.create_topic(topic_name)
            for other_node in self.nodes:
                response = await other_node.subscribe_to_topic(topic_name)
                if response is None:
                    print(f"Functionality test failed for {node.peer_id} -> {other_node.peer_id}")
                    success = False
        return success

    async def measure_response_time(self, topic_name, publisher, subscriber):
        """
        Measures the average response time for requests to reach responsible nodes.
        """
        start_time = time.time()
        await publisher.publish_message(topic_name, "Test Message")
        await subscriber.subscribe_to_topic(topic_name)
        end_time = time.time()
        self.response_times.append(end_time - start_time)

    async def benchmark_response_time(self):
        """
        Run response time tests across nodes for an average response time calculation.
        """
        # Run multiple response time tests across different pairs of nodes
        for i in range(50):  # Increase if more tests are needed
            pub_node = random.choice(self.nodes)
            sub_node = random.choice([n for n in self.nodes if n != pub_node])
            topic_name = f"Benchmark_Topic_{i}"
            await pub_node.create_topic(topic_name)
            await self.measure_response_time(topic_name, pub_node, sub_node)

    async def measure_throughput(self, duration=5):
        """
        Measures the max throughput by counting requests processed in a set duration.
        """
        request_count = 0
        start_time = time.time()
        while time.time() - start_time < duration:
            pub_node = random.choice(self.nodes)
            topic_name = f"ThroughputTest_Topic_{request_count}"
            await pub_node.create_topic(topic_name)
            await pub_node.publish_message(topic_name, "Message Content")
            request_count += 1
        self.throughput_data['requests_per_sec'] = request_count / duration

    def plot_response_times(self):
        plt.figure(figsize=(10, 6))
        plt.plot(self.response_times, marker='o', linestyle='-', color='b')
        plt.title('Average Response Time for Request Forwarding')
        plt.xlabel('Request Index')
        plt.ylabel('Response Time (seconds)')
        plt.grid(True)
        plt.savefig('graphs/req_forw_average_response_time.svg')
        plt.close()

    def plot_throughput(self):
        plt.figure(figsize=(10, 6))
        throughput = self.throughput_data['requests_per_sec']
        plt.bar(['Max Throughput'], [throughput], color='g')
        plt.title('Maximum Throughput (Requests per Second)')
        plt.xlabel('Metric')
        plt.ylabel('Requests per Second')
        plt.grid(axis='y')
        plt.savefig('graphs/req_forw_max_throughput.svg')
        plt.close()

async def main():
    # Initialize nodes (assuming nodes are instances of PeerNode)
    total_peers = 8
    nodes = [PeerNode(f'{bin(i)[2:].zfill(3)}', '127.0.0.1', 6000 + i, total_peers) for i in range(total_peers)]
    
    experiment = Experiment(nodes)

    # Part A: Functionality Test
    functionality_test = await experiment.test_functionality()
    print("Functionality Test Passed" if functionality_test else "Functionality Test Failed")

    # Part B: Response Time Benchmarking
    await experiment.benchmark_response_time()
    experiment.plot_response_times()

    # Part C: Throughput Measurement
    await experiment.measure_throughput()
    experiment.plot_throughput()

# Run the experiment
if __name__ == "__main__":
    asyncio.run(main())
