import json
import logging
from NoSqlConfig import NoSqlConfig
from SendMessage import ServiceBusSender


def main(name: str) -> str:
    from_connection_string = "STRING END POINT AZURE SERVICE BUS"
    queue_name = "etl"
    collection_empresas = NoSqlConfig().getCollection('empresas_mei').find()

    list_empresas = list(collection_empresas)
    logging.warning(f"Localizou {len(list_empresas)} empresas.")
    empresas = []
    for empresa in list_empresas:
        empresas.append(
            {
                "db_name": empresa['db_name'],
                "cnpj": empresa['cnpj'],
                "uf": empresa['uf']
            }
        )

    sender = ServiceBusSender(from_connection_string, queue_name)
    sender.connect_str()

    sender.send_messages(empresas)

    sender.disconnect()
    return f"Hello {name}!"
