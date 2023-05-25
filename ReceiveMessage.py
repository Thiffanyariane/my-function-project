from azure.servicebus import ServiceBusClient, ServiceBusSubQueue
from azure.servicebus.exceptions import ServiceBusError
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info('Iniciando recebimento de mensagens')

class ServiceBusReceiver:
    def __init__(self, connection_string, queue_name):
        self.connection_string = connection_string
        self.queue_name = queue_name
        self.servicebus_client = None
        self.receiver = None
        self.dlq_receiver = None


    def connect_str(self):
        self.servicebus_client = ServiceBusClient.from_connection_string(self.connection_string)
        self.receiver = self.servicebus_client.get_queue_receiver(self.queue_name)
        self.dlq_receiver = self.servicebus_client.get_queue_receiver(self.queue_name, sub_queue=ServiceBusSubQueue.DEAD_LETTER)

    def receive_messages(self):
        while True:
            try:
                messages = self.receiver.receive_messages(max_message_count=10, max_wait_time=10)

                if not messages:
                    continue

                for message in messages:
                    logger.info(f'Mensagem Recebida: {str(message)}')
                    self.receiver.complete_message(message)

            except ServiceBusError as e:
                logger.error(f'Erro ao receber mensagem: {str(message)}')
                self.receiver.dead_letter_message(message)
            except TimeoutError as e:
                logger.error(f'Tempo de espera excedido: {str(message)}')
                self.receiver.dead_letter_message(message)
                break
    
        try:
            self.dlq_receiver.receive_messages(max_message_count=10, max_wait_time=10)

            for message in self.dlq_receiver:
                logger.info(f'Mensagem enviada ao Dead Letter: {str(message)}')
                self.dlq_receiver.complete_message(message)

        except ServiceBusError as e:
            logger.error(f'Erro ao receber mensagem do Dead Letter{str(message)}')
    
    def diconnect(self):
        self.receiver.close()
        self.dlq_receiver.close()
        self.servicebus_client.close()
    