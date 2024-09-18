# main.py

import time
import random
from file_server import FileServer
from client import Client
from network_manager import NetworkManager
import logging

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

    network_manager = NetworkManager()

    # Server IDs
    server_ids = ['server1', 'server2', 'server3']

    # Update the storage_dir to your desired path
    storage_dir = r'D:/'  # Replace with your directory

    # Initialize servers
    servers = {}
    for server_id in server_ids:
        peers = [s for s in server_ids if s != server_id]
        servers[server_id] = FileServer(
            server_id=server_id,
            network_manager=network_manager,
            peers=peers,
            storage_dir=storage_dir  # Set storage directory
        )

    # Randomly select one server to be the leader
    leader_id = random.choice(server_ids)
    leader_server = servers[leader_id]
    leader_server.become_leader()
    print(f"Initial leader is '{leader_id}'")

    # Allow time for followers to recognize the leader
    time.sleep(2)

    # Initialize Client 1 connected to Server 2
    client1 = Client(client_id='client1', connected_server_id='server2', network_manager=network_manager)

    # Client 1 creates a file
    print("\nClient 1 creating file 'test.txt'")
    client1.create_file('test.txt')

    # Allow time for the operation to propagate
    time.sleep(2)

    # Client 1 writes to the file
    print("\nClient 1 writing to 'test.txt'")
    client1.write_file('test.txt', 'Hello from Client 1!')

    # Allow time for replication
    time.sleep(2)

    # Client 1 reads the file
    print("\nClient 1 reading 'test.txt'")
    client1.read_file('test.txt')

    # Initialize Client 2 connected to Server 3
    client2 = Client(client_id='client2', connected_server_id='server3', network_manager=network_manager)

    # Allow time for the client to connect
    time.sleep(2)

    # Client 2 reads the file to verify replication
    print("\nClient 2 reading 'test.txt'")
    client2.read_file('test.txt')

    # Client 2 writes new content to the file
    print("\nClient 2 writing to 'test.txt'")
    client2.write_file('test.txt', 'Hello from Client 2!')

    # Allow time for replication
    time.sleep(5)

    # Client 2 reads the file again
    print("\nClient 2 reading 'test.txt' after writing")
    client2.read_file('test.txt')

    # Client 1 reads the file again to see updates from Client 2
    print("\nClient 1 reading 'test.txt' after Client 2's update")
    client1.read_file('test.txt')

    # Client 1 deletes the file
    print("\nClient 1 deleting 'test.txt'")
    client1.delete_file('test.txt')

    # Allow time for deletion to propagate
    time.sleep(2)

    # Client 2 tries to read the deleted file
    print("\nClient 2 attempting to read 'test.txt' after deletion")
    client2.read_file('test.txt')

    # Simulate leader failure
    print(f"\nSimulating failure of leader '{leader_id}'...")
    # Stop the leader's threads (simplified for this example)
    leader_server.state = 'stopped'
    print(f"Server '{leader_id}' has been stopped.")

    # Allow time for new leader election
    time.sleep(5)

    # Check for new leader
    new_leader_id = None
    for server_id, server in servers.items():
        if server.state == 'leader' and server_id != leader_id:
            new_leader_id = server_id
            print(f"New leader elected: {new_leader_id}")
            break

    if not new_leader_id:
        print("No new leader was elected after failure.")
    else:
        # Client 2 writes to the file after leader failure
        print("\nClient 2 writing to 'test.txt' after leader failure")
        client2.write_file('test.txt', 'New content after leader failure')

        # Allow time for replication
        time.sleep(5)

        # Client 1 reads the file to see updates after leader failure
        print("\nClient 1 reading 'test.txt' after leader failure")
        client1.read_file('test.txt')

    # Allow time for all threads to complete
    time.sleep(5)
