class Message:
    def __init__(self, msg_type: str, data: dict):
        self.type = msg_type
        self.data = data