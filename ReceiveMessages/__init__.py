# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import time
import logging
from ReceiveMessage import ServiceBusReceiver

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main(name: str) -> str:
    from_connection_string = "STRING END POINT AZURE SERVICE BUS"
    queue_name = "etl"

    receiver = ServiceBusReceiver(from_connection_string, queue_name)
    receiver.connect_str()

    messages = receiver.receive_messages()

    for message in messages:
        logging.info(f'Mensagem Recebida: {str(message)}')
        receiver.complete_message(message)

    receiver.disconnect()
    return f"Hello {name}!"
