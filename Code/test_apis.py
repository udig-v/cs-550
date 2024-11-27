import asyncio
from peer_node import PeerNode

async def test_peer_operations(peers):
    print("Step 1: Creating topic on Peer 0")
    await peers[0].create_topic("TestTopic")

    print("Step 2: Multiple peers subscribing to the topic")
    for peer in peers:
        await peer.subscribe_to_topic("TestTopic")

    print("Step 3: Multiple peers publishing to the topic")
    for peer in peers:
        await peer.publish_message("TestTopic", f"Message from Peer {peer.peer_id}")
    
    print("Step 4: Peers retrieving messages from the topic")
    for peer in peers:
        messages = peer.topics.get("TestTopic", [])
        print(f"Messages received by Peer {peer.peer_id}: {messages}")

    print("Step 5: Deleting topic from Peer 0")
    await peers[0].delete_topic("TestTopic")
    for peer in peers:
        if "TestTopic" in peer.topics:
            print(f"Topic still exists on Peer {peer.peer_id}")
        else:
            print(f"Topic deleted from Peer {peer.peer_id}")

# Define the main method
async def main():
    total_peers = 8
    peers = []
    for peer_id in range(total_peers):
        binary_id = bin(peer_id)[2:].zfill(3)
        peer = PeerNode(binary_id, '127.0.0.1', 6000 + peer_id, total_peers)
        peers.append(peer)
    
    # Run the test on all peers
    await test_peer_operations(peers)

# Start the main event loop
if __name__ == "__main__":
    asyncio.run(main())
