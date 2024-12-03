import asyncio
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
    print("Simulate node removal...")
    active_nodes.remove(peers[2].peer_id)  # 模拟节点 2 离线
    peers[2].neighbors = peers[2].compute_neighbors()

    await asyncio.sleep(5)  # 等待邻居检测

    print("Simulate node rejoin...")
    active_nodes.append(peers[2].peer_id)  # 模拟节点 2 重新加入
    await peers[0].handle_node_rejoin(peers[2].peer_id)


async def test_topic_replication(peers):
    try:
        print("Step 1: Create topic on Peer 0 and replicate")
        # Create topic on Peer 0
        await asyncio.wait_for(peers[0].create_topic("TestTopic"), timeout=10)
        print("Topic 'TestTopic' created on Peer 0")

        # Simulate replication: Ensure that topics are replicated on other peers
        for i, peer in enumerate(peers):
            if i != 0:
                # Add debug log to track if the topic is correctly replicated
                print(f"Checking if 'TestTopic' is replicated on Peer {peer.peer_id}")
                assert (
                    "TestTopic" in peer.topics
                ), f"TestTopic not found on Peer {peer.peer_id}"
                print(f"Topic 'TestTopic' replicated to Peer {peer.peer_id}")

        print("Step 2: Publish a message to 'TestTopic' and validate replication")
        message = "Message from Peer 0"
        await asyncio.wait_for(
            peers[0].publish_message("TestTopic", message), timeout=10
        )
        print("Message published by Peer 0 to 'TestTopic'")

        # Validate that all peers have received the message (replication of messages)
        for peer in peers:
            messages = peer.topics.get("TestTopic", [])
            assert message in messages, f"Message not replicated on Peer {peer.peer_id}"
            print(f"Message '{message}' replicated on Peer {peer.peer_id}")

        print("Step 3: Simulate a peer failure and ensure replication persists")
        # Simulate peer failure
        print("Simulating Peer 2 failure...")
        await peers[
            2
        ].shutdown()  # Assuming there's a shutdown method that simulates failure

        # Recheck replication on remaining peers
        await asyncio.sleep(2)  # Give it time to detect and reroute
        for peer in peers:
            if peer.peer_id != "2":  # Skip the failed peer
                messages = peer.topics.get("TestTopic", [])
                assert (
                    message in messages
                ), f"Message not replicated on Peer {peer.peer_id} after failure"
                print(
                    f"Message '{message}' replicated on Peer {peer.peer_id} after failure detection"
                )

        print("Step 4: Simulate peer rejoin and ensure replication consistency")
        print("Simulating Peer 2 rejoin...")
        await peers[2].rejoin()  # Assuming a rejoin method that re-establishes peer

        # Ensure that after rejoin, the topic and message are available
        await asyncio.sleep(2)  # Allow time for rejoin and replication
        messages = peers[2].topics.get("TestTopic", [])
        assert message in messages, f"Message not replicated on Peer 2 after rejoin"
        print(f"Message '{message}' replicated on Peer 2 after rejoin")

    except asyncio.TimeoutError:
        print("Operation timed out, potential issue detected in peer communication.")
    except AssertionError as e:
        print(f"Assertion failed: {e}")


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
        await test_peer_operations(peers)

        # 执行动态拓扑测试
        print("Start testing dynamic topology configuration...")
        await test_dynamic_topology(peers, active_nodes)
        print("Dynamic topology configuration testing is complete.")

        # Perform topic replication test
        await test_topic_replication(peers)
        print("Topic replication testing is complete.")

    except Exception as e:
        print(f"An error occurred during testing: {e}")
    finally:
        # 适当地停止所有服务器
        for task in server_tasks:
            task.cancel()
        await asyncio.gather(*server_tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
