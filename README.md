# Distributed-File-System
This project implements a distributed file system using the Raft consensus algorithm for leader election and log replication. The system provides basic file operations with replication across multiple servers for fault tolerance.

## 🚀 Features

- Distributed file storage and retrieval
- Replication for fault tolerance
- Leader election using Raft consensus algorithm
- Basic file operations (create, read, write, delete)
- Lease-based concurrency control
- Simulated network communication between nodes

## Components

- `main.py`: Entry point of the application, sets up the network and demonstrates system functionality.
- `client.py`: Implements the client interface for interacting with the file system.
- `file_server.py`: Extends RaftNode to implement file operations and manage file storage.
- `raft_node.py`: Implements the Raft consensus algorithm for leader election and log replication.
- `network_manager.py`: Simulates network communication between nodes.
- `file.py`: Defines data structures for files, versions, and leases.
- `message.py`: Defines the message structure for inter-node communication.

## Requirements

- Python 3.7+

## Setup and Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/yourusername/distributed-file-system.git
   cd distributed-file-system



2. Open a terminal in the project folder
  Run python main.py

***Notes : This will start a demo of the system, showing how files are created, read, and updated across multiple servers. Customizing***

In main.py, you can change the number of servers
Also in main.py, you can set where files are stored on your computer

## Acknowledgments

This project is inspired by the Raft consensus algorithm paper by Diego Ongaro and John Ousterhout.
Thanks to all contributors who have helped in developing and improving this distributed file system.
