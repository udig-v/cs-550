import asyncio
import time
import matplotlib.pyplot as plt
from peer_node import PeerNode


async def test_peer_operations(peers):
    try:
        print("Step 1: Creating topic on Peer 0")
        await asyncio.wait_for(peers[0].create_topic("TestTopic"), timeout=10)
        print("Topic created by Peer 0")

        print("Step 2: Multiple peers subscribing to the topic")
        for peer in peers:
            await asyncio.wait_for(peer.subscribe_to_topic("TestTopic"), timeout=10)
            print(f"Peer {peer.peer_id} subscribed to 'TestTopic'")

        print("Step 3: Multiple peers publishing to the topic")
        for peer in peers:
            message = f"Message from Peer {peer.peer_id}"
            await asyncio.wait_for(
                peer.publish_message("TestTopic", message), timeout=10
            )
            print(f"Peer {peer.peer_id} published message to 'TestTopic'")

        print("Step 4: Peers retrieving messages from the topic")
        for peer in peers:
            messages = peer.topics.get("TestTopic", [])
            print(f"Messages received by Peer {peer.peer_id}: {messages}")

        print("Step 5: Deleting topic from Peer 0")
        await asyncio.wait_for(peers[0].delete_topic("TestTopic"), timeout=10)

        for peer in peers:
            if "TestTopic" in peer.topics:
                print(f"Topic still exists on Peer {peer.peer_id}")
            else:
                print(f"Topic deleted from Peer {peer.peer_id}")
    except asyncio.TimeoutError:
        print("Operation timed out, potential issue detected in peer communication.")


async def test_dynamic_topology(peers, active_nodes):
    print("\n----------------Dynamic Topology Configuration-------------------")
    print("Simulate node removal...")
    active_nodes.remove(peers[2].peer_id)  # 模拟节点 2 离线
    peers[2].neighbors = peers[2].compute_neighbors()

    await asyncio.sleep(5)  # 等待邻居检测

    print("Simulate node rejoin...")
    active_nodes.append(peers[2].peer_id)  # 模拟节点 2 重新加入
    await peers[0].handle_node_rejoin(peers[2].peer_id)


async def test_replication(peers):
    print("\n----------------Replication Functionality Test-------------------")
    try:
        print("Step 1: Creating topic with replication on Peer 0")
        await asyncio.wait_for(peers[0].create_topic("TestTopic"), timeout=10)
        await asyncio.wait_for(peers[0].create_replicas("TestTopic"), timeout=10)
        print("Topic created with replicas by Peer 0")

        print("Step 2: Publishing message to 'TestTopic' on Peer 0")
        message = "Replication test message from Peer 0"
        await asyncio.wait_for(
            peers[0].publish_message("TestTopic", message), timeout=20
        )
        print(f"Peer 0 published message: {message}")

        print("Step 3: Checking message replication across peers")
        for peer in peers:
            messages = peer.topics.get("TestTopic", [])
            if messages:
                print(f"Peer {peer.peer_id} received messages: {messages}")
            else:
                print(f"Peer {peer.peer_id} did not receive any messages.")
    except asyncio.TimeoutError:
        print("Replication test timed out.")
        
async def test_replication_on_right_node(peers):
    print("\nVerifying topic replication on the right nodes")

    # Create topic and replicas on Peer 0
    await asyncio.wait_for(peers[0].create_topic("TestTopic"), timeout=10)
    await asyncio.wait_for(peers[0].create_replicas("TestTopic"), timeout=10)
    print("Topic created with replicas on Peer 0")

    # Publish a message to the topic
    message = "Replication test message from Peer 0"
    await asyncio.wait_for(peers[0].publish_message("TestTopic", message), timeout=20)
    print(f"Peer 0 published message: {message}")

    # Verify topic replication on other peers
    for peer in peers:
        if "TestTopic" in peer.topics:
            print(f"Peer {peer.peer_id} received messages: {peer.topics['TestTopic']}")
        else:
            print(f"Peer {peer.peer_id} did not receive any messages.")

async def test_node_failure_and_recovery(peers, active_nodes):
    print("\nSimulating node failure and recovery")

    # Simulate node failure (removing Peer 2)
    print("Simulating Peer 7 failure...")
    failed_peer = peers[7]
    await failed_peer.create_topic("TestTopic")  # Create a topic on Peer 7
    print(f"Active nodes before failure: {active_nodes}")
    active_nodes.remove(failed_peer.peer_id)  # Remove Peer 2 from the active nodes
    print(f"Active nodes after failure: {active_nodes}")
    failed_peer.neighbors = failed_peer.compute_neighbors()  # Update neighbors
    await asyncio.sleep(5)  # Wait for the network to detect the failure

    # Ensure Peer 7 has the correct state after recovery
    if "TestTopic" in failed_peer.topics:
        print(f"Peer 7 recovered the topic: {failed_peer.topics['TestTopic']}")
    else:
        print("Peer 7 failed to recover the topic after rejoining.")


async def test_node_rejoin_and_state_recovery(peers, active_nodes):
    print("\nSimulating node rejoin and state recovery")

    # Simulate node failure and recovery for Peer 2 (as in previous test)
    failed_peer = peers[5]

    print("Simulating Peer 5 failure...")
    print(f"Active nodes before failure: {active_nodes}")
    active_nodes.remove(failed_peer.peer_id)  # Remove Peer 2 from the active nodes
    print(f"Active nodes after failure: {active_nodes}")
    failed_peer.neighbors = failed_peer.compute_neighbors()  # Update neighbors
    await asyncio.sleep(5)  # Wait for the network to detect the failure

    # Check if Peer 2 has recovered its topic and data from other peers
    print("Checking if Peer 5 has recovered its topic state...")
    if "TestTopic" in failed_peer.topics:
        print(f"Peer 5 recovered topic: {failed_peer.topics['TestTopic']}")
    else:
        print("Peer 5 failed to recover its topic after rejoining.")


async def test_topic_recovery_from_replica(peers, active_nodes):
    print("\nTesting topic recovery when the right node is offline")

    # Create topic with replication on Peer 0
    await asyncio.wait_for(peers[0].create_topic("TestTopic"), timeout=10)
    await asyncio.wait_for(peers[0].create_replicas("TestTopic"), timeout=10)
    print("Topic created with replicas on Peer 0")

    # Simulate the failure of the "right" node for the topic (assume Peer 0 is the primary node)
    failed_peer = peers[1]
    print("Active nodes before failure: ", active_nodes)
    active_nodes.remove(failed_peer.peer_id)  # Remove Peer 0 from active nodes
    print("Active nodes after failure: ", active_nodes)
    failed_peer.neighbors = failed_peer.compute_neighbors()  # Update neighbors
    await asyncio.sleep(5)  # Wait for the network to detect the failure

    # Try to access the topic from another peer (should get replica)
    print("Trying to access topic from other peers...")
    for peer in peers:
        if peer != failed_peer:
            topic_data = peer.topics.get("TestTopic", [])
            if topic_data:
                print(f"Peer {peer.peer_id} retrieved topic from replica: {topic_data}")
            else:
                print(f"Peer {peer.peer_id} failed to retrieve topic.")

async def benchmark_apis(peers):
    print(
        "\n----------------Benchmarking API Latency and Throughput-------------------"
    )

    latency_results = {
        "create_topic": [],
        "publish_message": [],
        "subscribe_to_topic": [],
        "delete_topic": [],
    }
    throughput_results = {
        "create_topic": 0,
        "publish_message": 0,
        "subscribe_to_topic": 0,
        "delete_topic": 0,
    }
    num_requests = 5  # Number of requests to simulate per API

    for peer in peers:
        for api in latency_results.keys():
            start_time = time.time()
            # Simulating randomly generated workloads for each API
            for _ in range(5):  # Simulate 5 requests for each API
                try:
                    if api == "create_topic":
                        await asyncio.wait_for(
                            peer.create_topic("BenchmarkTopic"), timeout=5
                        )
                    elif api == "publish_message":
                        await asyncio.wait_for(
                            peer.publish_message("BenchmarkTopic", "Benchmark message"),
                            timeout=5,
                        )
                    elif api == "subscribe_to_topic":
                        await asyncio.wait_for(
                            peer.subscribe_to_topic("BenchmarkTopic"), timeout=5
                        )
                    elif api == "delete_topic":
                        await asyncio.wait_for(
                            peer.delete_topic("BenchmarkTopic"), timeout=5
                        )
                except asyncio.TimeoutError:
                    print(f"API {api} timed out on Peer {peer.peer_id}")

            # Calculate latency and throughput
            end_time = time.time()
            latency_results[api].append(
                (end_time - start_time) / num_requests
            )  # Average latency for the API
            throughput_results[api] += num_requests / (
                end_time - start_time
            )  # Throughput (requests per second)

    # Calculate average latency and throughput across all peers
    avg_latency = {
        api: sum(latencies) / len(latencies)
        for api, latencies in latency_results.items()
    }
    avg_throughput = {
        api: throughput / len(peers) for api, throughput in throughput_results.items()
    }

    # Print results
    print("\nAverage Latency (in seconds) for each API:")
    for api, latency in avg_latency.items():
        print(f"{api}: {latency:.4f} seconds")

    print("\nAverage Throughput (requests per second) for each API:")
    for api, throughput in avg_throughput.items():
        print(f"{api}: {throughput:.2f} requests/sec")

    return avg_latency, avg_throughput


def plot_benchmark_results(latency, throughput):
    # Latency graph
    plt.figure(figsize=(10, 5))
    plt.bar(latency.keys(), latency.values(), color='blue', alpha=0.7)
    plt.xlabel('API')
    plt.ylabel('Average Latency (seconds)')
    plt.title('API Latency Benchmark')
    plt.savefig("Out/latency_benchmark.svg")

    # Throughput graph
    plt.figure(figsize=(10, 5))
    plt.bar(throughput.keys(), throughput.values(), color='green', alpha=0.7)
    plt.xlabel('API')
    plt.ylabel('Average Throughput (requests/sec)')
    plt.title('API Throughput Benchmark')
    plt.savefig("Out/throughput_benchmark.svg")

async def main():
    total_peers = 8
    active_nodes = []
    peers = []

    # 初始化节点
    for peer_id in range(total_peers):
        binary_id = bin(peer_id)[2:].zfill(3)
        peer = PeerNode(
            binary_id, "127.0.0.1", 6000 + peer_id, total_peers, active_nodes
        )
        peers.append(peer)
        active_nodes.append(binary_id)

    # 创建服务器任务但不等待它们结束
    server_tasks = [asyncio.create_task(peer.run_peer()) for peer in peers]

    try:
        # 执行标准操作测试
        # await test_peer_operations(peers)

        # 执行动态拓扑测试
        # print("Start testing dynamic topology configuration...")
        # await test_dynamic_topology(peers, active_nodes)
        # print("Dynamic topology configuration testing is complete.")

        # Test replication functionality
        # await test_replication(peers)
        # print("Replication functionality test complete.")

        # avg_latency, avg_throughput = await benchmark_apis(peers)
        
        # plot_benchmark_results(avg_latency, avg_throughput)
        
        await test_replication_on_right_node(peers)
        
        await test_node_failure_and_recovery(peers, active_nodes)
        
        await test_node_rejoin_and_state_recovery(peers, active_nodes)
        
        await test_topic_recovery_from_replica(peers, active_nodes)

    except Exception as e:
        print(f"An error occurred during testing: {e}")
    finally:
        # 适当地停止所有服务器
        for task in server_tasks:
            task.cancel()
        await asyncio.gather(*server_tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
