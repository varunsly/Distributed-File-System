# raft_node.py

import threading
import time
import random
from typing import List
from network_manager import NetworkManager
from message import Message
import logging

class RaftNode:
    def __init__(self, server_id: str, network_manager: NetworkManager, peers: List[str]):
        self.server_id = server_id
        self.network_manager = network_manager
        self.peers = peers  # List of other server IDs
        self.state = 'follower'
        self.current_term = 0
        self.voted_for = None
        self.commit_index = 0
        self.last_applied = 0
        self.next_index = {peer: 0 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        self.log = []  # List of log entries
        self.lock = threading.Lock()
        self.election_timeout = random.uniform(1, 2)  # Adjusted timeout
        self.last_heartbeat = time.time()
        self.leader_id = None  # Keep track of current leader
        self.votes_received = 0

        # Register message handlers
        self.network_manager.register_handler('request_vote', self.handle_request_vote)
        self.network_manager.register_handler('append_entries', self.handle_append_entries)
        self.network_manager.register_handler('vote_response', self.handle_vote_response)
        self.network_manager.register_handler('append_entries_response', self.handle_append_entries_response)

        # Start threads
        threading.Thread(target=self.run_election_timer, daemon=True, name=f"{self.server_id}_election_timer").start()
        threading.Thread(target=self.process_messages, daemon=True, name=f"{self.server_id}_message_processor").start()

    def become_leader(self):
        with self.lock:
            self.state = 'leader'
            self.leader_id = self.server_id
            logging.info(f"{self.server_id}: Became leader in term {self.current_term}")
            threading.Thread(target=self.send_heartbeats, daemon=True, name=f"{self.server_id}_heartbeats").start()

    def run_election_timer(self):
        try:
            while self.state != 'stopped':
                time.sleep(0.1)
                with self.lock:
                    if self.state == 'leader':
                        continue
                    if (time.time() - self.last_heartbeat) >= self.election_timeout:
                        logging.debug(f"{self.server_id}: Election timeout, starting election")
                        self.start_election()
                        self.last_heartbeat = time.time()
        except Exception as e:
            logging.error(f"Error in run_election_timer on server '{self.server_id}': {e}")

    def start_election(self):
        with self.lock:
            self.state = 'candidate'
            self.current_term += 1
            self.voted_for = self.server_id
            self.votes_received = 1  # Reset votes received
            self.election_timeout = random.uniform(1, 2)  # Reset election timeout
            logging.debug(f"{self.server_id}: Starting election for term {self.current_term}")
            logging.debug(f"{self.server_id}: Voted for self in term {self.current_term}")

        for peer in self.peers:
            logging.debug(f"{self.server_id}: Sending request_vote to '{peer}'")
            self.network_manager.send_message(Message('request_vote', {
                'term': self.current_term,
                'candidate_id': self.server_id,
                'last_log_index': len(self.log),
                'last_log_term': self.log[-1]['term'] if self.log else 0,
                'source_id': self.server_id
            }), peer)

    def handle_request_vote(self, data):
        with self.lock:
            term = data['term']
            candidate_id = data['candidate_id']
            response = {
                'term': self.current_term,
                'vote_granted': False,
                'source_id': self.server_id
            }
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.state = 'follower'
                self.last_heartbeat = time.time()
                self.election_timeout = random.uniform(1, 2)  # Reset election timeout
            if (self.voted_for in [None, candidate_id]) and term >= self.current_term:
                self.voted_for = candidate_id
                response['vote_granted'] = True
                self.current_term = term
                logging.debug(f"{self.server_id}: Voted for {candidate_id} in term {term}")
            self.network_manager.send_message(Message('vote_response', response), candidate_id)

    def handle_vote_response(self, data):
        with self.lock:
            if self.state != 'candidate':
                return
            term = data['term']
            if term > self.current_term:
                self.current_term = term
                self.state = 'follower'
                self.voted_for = None
                return
            elif term < self.current_term:
                # Ignore old term
                return

            if data['vote_granted']:
                self.votes_received += 1
                logging.debug(f"{self.server_id}: Received vote from {data['source_id']}")
                if self.votes_received > (len(self.peers) + 1) // 2:
                    self.state = 'leader'
                    self.leader_id = self.server_id
                    logging.info(f"{self.server_id}: Became leader in term {self.current_term}")
                    threading.Thread(target=self.send_heartbeats, daemon=True, name=f"{self.server_id}_heartbeats").start()

    def send_heartbeats(self):
        try:
            while self.state == 'leader':
                for peer in self.peers:
                    self.network_manager.send_message(Message('append_entries', {
                        'term': self.current_term,
                        'leader_id': self.server_id,
                        'prev_log_index': len(self.log) - 1,
                        'prev_log_term': self.log[-1]['term'] if self.log else 0,
                        'entries': [],
                        'leader_commit': self.commit_index
                    }), peer)
                time.sleep(0.5)
        except Exception as e:
            logging.error(f"Error in send_heartbeats on server '{self.server_id}': {e}")

    def handle_append_entries(self, data):
        with self.lock:
            term = data['term']
            leader_id = data['leader_id']
            if term >= self.current_term:
                self.current_term = term
                self.state = 'follower'
                self.voted_for = leader_id
                self.leader_id = leader_id
                self.last_heartbeat = time.time()
                self.election_timeout = random.uniform(1, 2)  # Reset election timeout
                response = {
                    'term': self.current_term,
                    'success': True
                }
                logging.debug(f"{self.server_id}: Received heartbeat from leader {leader_id}")
            else:
                response = {
                    'term': self.current_term,
                    'success': False
                }
            self.network_manager.send_message(Message('append_entries_response', response), leader_id)

    def handle_append_entries_response(self, data):
        with self.lock:
            term = data['term']
            if term > self.current_term:
                self.current_term = term
                self.state = 'follower'
                self.voted_for = None
                self.leader_id = None
                logging.debug(f"{self.server_id}: Stepping down to follower due to higher term {term}")
                return
            # Process success/failure response
            # Update next_index and match_index if necessary
            # For simplicity, we'll log the response
            logging.debug(f"{self.server_id}: Received append_entries_response from follower")

    def process_messages(self):
        try:
            while self.state != 'stopped':
                message = self.network_manager.receive_message(self.server_id)
                if message:
                    handler = self.network_manager.handlers.get(message.type)
                    if handler:
                        handler(message.data)
                    else:
                        logging.warning(f"{self.server_id}: No handler for message type '{message.type}'")
                else:
                    time.sleep(0.1)
        except Exception as e:
            logging.error(f"Error in process_messages on server '{self.server_id}': {e}")
