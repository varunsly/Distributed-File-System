# file_server.py

import threading
import time
import json
import os
from typing import Dict
from file import File, FileVersion, Lease
from network_manager import NetworkManager
from raft_node import RaftNode
from message import Message
import logging

class FileServer(RaftNode):
    def __init__(self, server_id: str, network_manager: NetworkManager, peers: list, storage_dir: str):
        super().__init__(server_id, network_manager, peers)
        self.files: Dict[str, File] = {}
        self.storage_dir = storage_dir
        self.lock = threading.Lock()

        # Register message handlers
        self.network_manager.register_handler('create_file', self.handle_create_file)
        self.network_manager.register_handler('read_file', self.handle_read_file)
        self.network_manager.register_handler('write_file', self.handle_write_file)
        self.network_manager.register_handler('delete_file', self.handle_delete_file)
        self.network_manager.register_handler('request_lease', self.handle_request_lease)
        self.network_manager.register_handler('release_lease', self.handle_release_lease)

        # Start lease management thread
        threading.Thread(target=self.manage_leases, daemon=True, name=f"{self.server_id}_lease_manager").start()

    def create_file(self, filename: str):
        with self.lock:
            logging.debug(f"Attempting to create file '{filename}' on server '{self.server_id}'")
            if filename not in self.files:
                new_file = File(filename, self.server_id)
                new_file.add_version("")  # Initial empty content
                self.files[filename] = new_file
                self.save_file(filename)
                logging.info(f"File '{filename}' created on server '{self.server_id}'")
                # Replicate to followers
                if self.state == 'leader':
                    self.replicate_log({'operation': 'create_file', 'filename': filename})
                return True
            else:
                logging.warning(f"File '{filename}' already exists on server '{self.server_id}'")
                return False

    def read_file(self, filename: str) -> str:
        with self.lock:
            if filename in self.files:
                latest_version = self.files[filename].get_latest_version()
                content = latest_version.content if latest_version else ""
                logging.info(f"File '{filename}' read on server '{self.server_id}' with content: {content}")
                return content
            else:
                logging.warning(f"File '{filename}' not found on server '{self.server_id}'")
                return ""

    def write_file(self, filename: str, content: str):
        with self.lock:
            if self.state != 'leader':
                # Forward to leader
                if self.leader_id:
                    logging.debug(f"Server '{self.server_id}' forwarding write request for '{filename}' to leader '{self.leader_id}'")
                    self.network_manager.send_message(Message('write_file', {
                        'filename': filename,
                        'content': content,
                        'client_id': self.client_id
                    }), self.leader_id)
                else:
                    logging.error(f"Server '{self.server_id}' does not know the leader to forward the write request.")
                return
            if filename in self.files:
                self.files[filename].add_version(content)
                self.save_file(filename)
                logging.info(f"File '{filename}' updated on server '{self.server_id}'")
                # Replicate to followers
                self.replicate_log({'operation': 'write_file', 'filename': filename, 'content': content})
            else:
                logging.warning(f"File '{filename}' not found on server '{self.server_id}'")

    def delete_file(self, filename: str):
        with self.lock:
            if filename in self.files:
                del self.files[filename]
                file_path = os.path.join(self.storage_dir, f"{filename}_{self.server_id}.json")
                if os.path.exists(file_path):
                    os.remove(file_path)
                logging.info(f"File '{filename}' deleted on server '{self.server_id}'")
                # Replicate deletion to followers
                if self.state == 'leader':
                    self.replicate_log({'operation': 'delete_file', 'filename': filename})
                return True
            else:
                logging.warning(f"File '{filename}' not found on server '{self.server_id}'")
                return False

    def replicate_log(self, entry):
        with self.lock:
            self.log.append({'term': self.current_term, 'entry': entry})
            logging.debug(f"{self.server_id}: Appended to log: {entry}")
            # For simplicity, we'll assume immediate success in replication
            # In a full implementation, we'd wait for confirmations from followers

    def handle_create_file(self, data):
        try:
            success = self.create_file(data['filename'])
            # Send acknowledgment to client
            response_message = Message('create_file_response', {
                'success': success
            })
            self.network_manager.send_message(response_message, data['client_id'])
            logging.debug(f"Sent create_file_response to client '{data['client_id']}'")
        except Exception as e:
            logging.error(f"Error handling create_file on server '{self.server_id}': {e}")

    def handle_read_file(self, data):
        try:
            content = self.read_file(data['filename'])
            # Send content back to client
            response_message = Message('read_file_response', {
                'content': content
            })
            self.network_manager.send_message(response_message, data['client_id'])
            logging.debug(f"Sent read_file_response to client '{data['client_id']}' for file '{data['filename']}'")
        except Exception as e:
            logging.error(f"Error handling read_file for '{data['filename']}' on server '{self.server_id}': {e}")

    def handle_write_file(self, data):
        try:
            if self.state != 'leader' and self.leader_id:
                # Forward to leader
                self.network_manager.send_message(Message('write_file', data), self.leader_id)
                logging.debug(f"Server '{self.server_id}' forwarded write_file to leader '{self.leader_id}'")
            elif self.state == 'leader':
                self.write_file(data['filename'], data['content'])
                # Send acknowledgment to client
                response_message = Message('write_file_response', {
                    'success': True
                })
                self.network_manager.send_message(response_message, data['client_id'])
                logging.debug(f"Sent write_file_response to client '{data['client_id']}'")
            else:
                logging.error(f"Server '{self.server_id}' cannot handle write_file request at this time.")
        except Exception as e:
            logging.error(f"Error handling write_file on server '{self.server_id}': {e}")

    def handle_delete_file(self, data):
        try:
            success = self.delete_file(data['filename'])
            # Send acknowledgment to client
            response_message = Message('delete_file_response', {
                'success': success
            })
            self.network_manager.send_message(response_message, data['client_id'])
            logging.debug(f"Sent delete_file_response to client '{data['client_id']}'")
        except Exception as e:
            logging.error(f"Error handling delete_file on server '{self.server_id}': {e}")

    def manage_leases(self):
        try:
            while True:
                with self.lock:
                    for filename, file in self.files.items():
                        if file.lease and file.lease.is_expired():
                            logging.info(f"Lease expired for file '{filename}'")
                            file.lease = None
                time.sleep(1)
        except Exception as e:
            logging.error(f"Error in manage_leases on server '{self.server_id}': {e}")

    def handle_request_lease(self, data):
        filename = data['filename']
        duration = data['duration']
        lessee_id = data['lessee_id']
        with self.lock:
            if filename in self.files:
                file = self.files[filename]
                if file.lease is None or file.lease.is_expired():
                    file.lease = Lease(lessee_id, time.time() + duration)
                    logging.info(f"Lease granted for file '{filename}' to server '{lessee_id}'")
                    return True
                else:
                    logging.warning(f"Lease request for file '{filename}' denied")
                    return False
            else:
                logging.warning(f"File '{filename}' not found on server '{self.server_id}'")
                return False

    def handle_release_lease(self, data):
        filename = data['filename']
        lessee_id = data['lessee_id']
        with self.lock:
            if filename in self.files:
                file = self.files[filename]
                if file.lease and file.lease.lessee == lessee_id:
                    file.lease = None
                    logging.info(f"Lease released for file '{filename}' by server '{lessee_id}'")
                    return True
                else:
                    logging.warning(f"Lease release for file '{filename}' by server '{lessee_id}' denied")
                    return False
            else:
                logging.warning(f"File '{filename}' not found on server '{self.server_id}'")
                return False

    def save_file(self, filename: str):
        with self.lock:
            file_path = os.path.join(self.storage_dir, f"{filename}_{self.server_id}.json")
            try:
                os.makedirs(self.storage_dir, exist_ok=True)
                with open(file_path, 'w') as f:
                    file_data = {
                        'filename': filename,
                        'owner_server_id': self.files[filename].owner_server_id,
                        'versions': [{'content': v.content, 'timestamp': v.timestamp, 'version': v.version} for v in self.files[filename].versions]
                    }
                    json.dump(file_data, f)
                    logging.info(f"File '{filename}' saved on server '{self.server_id}' at '{file_path}'")
            except Exception as e:
                logging.error(f"Error saving file '{filename}' on server '{self.server_id}': {e}")
