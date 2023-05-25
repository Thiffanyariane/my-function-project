import json
import logging
from NoSqlConfig import NoSqlConfig
from SendMessage import ServiceBusSender

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='send_messages.log')
logging.info("Iniciando o envio de mensagens.")

def main(name: str) -> str:
    from_connection_string = "STRING END POINT AZURE SERVICE BUS"
    queue_name = "etl"
    collection_empresas = NoSqlConfig().getCollection('empresas_mei').find()

    list_empresas = list(collection_empresas)
    logging.warning(f"Localizou {len(list_empresas)} empresas.")
    
    max_batch_size = 1000
    current_batch = [] 
    sender = ServiceBusSender(from_connection_string, queue_name)
    sender.connect_str()

    for empresa in list_empresas:
        message_body = json.dumps({
            "db_name": empresa['db_name'],
            "cnpj": empresa['cnpj'],
            "uf": empresa['uf']
        })
        message_size = len(message_body.encode('utf-8'))
        
        logging.info("Tamanho da mensagem: %s bytes", message_size)
        
        if sum([len(msg) for msg in current_batch]) + message_size > max_batch_size:
            sender.send_messages(current_batch)
            current_batch = [] 
            logging.warning("Novo lote de mensagens criado!")

        current_batch.append(message_body)

    if current_batch > 0:
        sender.send_messages(current_batch) 

    sender.disconnect()
    return f"Hello {name}!"
