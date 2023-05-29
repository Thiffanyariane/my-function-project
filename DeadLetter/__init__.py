# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
from azure.servicebus import ServiceBusClient, ServiceBusSubQueue

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main(name: str) -> str:
    from_connection_string = "SERVICEBUS CONNECTION STR"
    queue_name = "parse"

    servicebus_client = ServiceBusClient.from_connection_string(from_connection_string)
    dlq_receiver = servicebus_client.get_queue_receiver(queue_name, sub_queue=ServiceBusSubQueue.DEAD_LETTER)

    with dlq_receiver:
        for message in dlq_receiver:
            logging.info(f'Mensagem Recebida do Dead Letter')
            dlq_receiver.complete_message(message)
    
    return f"Hello {name}!"
