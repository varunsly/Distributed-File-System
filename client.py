# client.py

from network_manager import NetworkManager
from message import Message
import threading
import time
import logging

class Client:
    def __init__(self, client_id: str, connected_server_id: str, network_manager: NetworkManager):
        self.client_id = client_id
        self.connected_server_id = connected_server_id
        self.network_manager = network_manager
        self.message_queue = []
        self.lock = threading.Lock()
        threading.Thread(target=self.process_responses, daemon=True, name=f"{self.client_id}_response_processor").start()

    def send_request(self, message):
        self.network_manager.send_message(message, self.connected_server_id)

    def receive_response(self):
        with self.lock:
            if self.message_queue:
                return self.message_queue.pop(0)
            else:
                return None

    def process_responses(self):
        while True:
            message = self.network_manager.receive_message(self.client_id)
            if message:
                with self.lock:
                    self.message_queue.append(message)
                    logging.debug(f"Client '{self.client_id}' received message of type '{message.type}'")
            else:
                time.sleep(0.1)

    def create_file(self, filename: str):
        message = Message('create_file', {
            'filename': filename,
            'client_id': self.client_id
        })
        self.send_request(message)
        # Wait for response
        start_time = time.time()
        while time.time() - start_time < 5:
            response = self.receive_response()
            if response:
                if response.type == 'create_file_response':
                    success = response.data['success']
                    print(f"Client '{self.client_id}' create file {'succeeded' if success else 'failed'}.")
                    return
                else:
                    logging.warning(f"Client '{self.client_id}' received unexpected message type '{response.type}'")
            time.sleep(0.1)
        print(f"Client '{self.client_id}' did not receive a response for creating file '{filename}'.")

    def read_file(self, filename: str):
        message = Message('read_file', {
            'filename': filename,
            'client_id': self.client_id
        })
        self.send_request(message)
        # Wait for response
        start_time = time.time()
        while time.time() - start_time < 5:
            response = self.receive_response()
            if response:
                if response.type == 'read_file_response':
                    content = response.data['content']
                    print(f"Client '{self.client_id}' read file '{filename}': {content}")
                    return
                else:
                    logging.warning(f"Client '{self.client_id}' received unexpected message type '{response.type}'")
            time.sleep(0.1)
        print(f"Client '{self.client_id}' did not receive a response for reading file '{filename}'.")

    def write_file(self, filename: str, content: str):
        message = Message('write_file', {
            'filename': filename,
            'content': content,
            'client_id': self.client_id
        })
        self.send_request(message)
        # Wait for response
        start_time = time.time()
        while time.time() - start_time < 5:
            response = self.receive_response()
            if response:
                if response.type == 'write_file_response':
                    success = response.data['success']
                    print(f"Client '{self.client_id}' write to file '{filename}' {'succeeded' if success else 'failed'}.")
                    return
                else:
                    logging.warning(f"Client '{self.client_id}' received unexpected message type '{response.type}'")
            time.sleep(0.1)
        print(f"Client '{self.client_id}' did not receive a response for writing to file '{filename}'.")

    def delete_file(self, filename: str):
        message = Message('delete_file', {
            'filename': filename,
            'client_id': self.client_id
        })
        self.send_request(message)
        # Wait for response
        start_time = time.time()
        while time.time() - start_time < 5:
            response = self.receive_response()
            if response:
                if response.type == 'delete_file_response':
                    success = response.data['success']
                    print(f"Client '{self.client_id}' delete file '{filename}' {'succeeded' if success else 'failed'}.")
                    return
                else:
                    logging.warning(f"Client '{self.client_id}' received unexpected message type '{response.type}'")
            time.sleep(0.1)
        print(f"Client '{self.client_id}' did not receive a response for deleting file '{filename}'.")
