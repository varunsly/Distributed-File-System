# network_manager.py

import threading
from typing import Callable, Dict
import logging

class NetworkManager:
    def __init__(self):
        self.handlers: Dict[str, Callable] = {}
        self.message_queues: Dict[str, list] = {}
        self.lock = threading.Lock()

    def register_handler(self, msg_type: str, handler: Callable):
        self.handlers[msg_type] = handler

    def send_message(self, message, recipient_id):
        with self.lock:
            if recipient_id not in self.message_queues:
                self.message_queues[recipient_id] = []
            self.message_queues[recipient_id].append(message)
            logging.debug(f"Message of type '{message.type}' sent to '{recipient_id}'")

    def receive_message(self, server_id):
        with self.lock:
            if server_id in self.message_queues and self.message_queues[server_id]:
                message = self.message_queues[server_id].pop(0)
                logging.debug(f"Server '{server_id}' received message of type '{message.type}'")
                return message
            else:
                return None
