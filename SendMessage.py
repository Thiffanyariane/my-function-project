from azure.servicebus import ServiceBusClient, ServiceBusMessage

class ServiceBusSender:
    def __init__(self, connection_string, queue_name):
        self.connection_string = connection_string
        self.queue_name = queue_name
        self.servicebus_client = None
        self.sender = None

    def connect_str(self):
        self.servicebus_client = ServiceBusClient.from_connection_string(self.connection_string)
        self.sender = self.servicebus_client.get_queue_sender(self.queue_name)

    def send_messages(self, messages):
        batch = self.sender.create_message_batch()
        current_batch_size = 0

        for message in messages:
            # message_size = len(message.encode('utf-8'))
            # if current_batch_size + message_size > batch.max_size_in_bytes:
            self.sender.send_messages(batch)
            batch = self.sender.create_message_batch()
            current_batch_size = 0
            
            batch.add_message(ServiceBusMessage(str(message))) 
            # current_batch_size += message_size

        if len(batch) > 0:
            self.sender.send_messages(batch)

        print("Mensagens enviadas com sucesso!")
    
    def disconnect(self):
        self.sender.close()
        self.servicebus_client.close()
