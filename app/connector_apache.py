import time
import datetime
import re
import json
import psycopg2
from kafka import KafkaProducer
from kafka import KafkaConsumer
import logging
import threading
from typing import Dict, Any
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurações do Kafka
CONFIG: Dict[str, Any] = {
    'log_file_path': '/var/log/apache2/access.log',
    'error_log_file_path': '/var/log/apache2/error.log',
    'kafka_server': os.getenv('KAFKA_SERVER'),
    'kafka_topic': os.getenv('KAFKA_TOPIC'),
    'sleep_time': 5,
}

# Configurações de conexão PostgreSQL
POSTGRESQL_CONFIG = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '55432'),
}

# Padrão regex para logs do Apache
APACHE_LOG_PATTERN = r'^([\da-fA-F:.]+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(?:([A-Z]+) (.*?) (HTTP\/\d\.\d)|-)" (\d{3}) (\d+|-) "(.*?)" "(.*?)"'
ERROR_LOG_PATTERN = r'^\[(.+?)\] \[([^\]]+)\] \[([^\]]+)\] (.+)$'

# Função para criar o KafkaProducer
def create_kafka_producer(server: str) -> KafkaProducer:
    """Cria e retorna uma instância do produtor Kafka."""
    try:
        return KafkaProducer(bootstrap_servers=server)
    except Exception as e:
        logger.error(f"Falha ao criar o produtor Kafka: {e}")
        raise

# Função para parsear uma linha de log de acesso do Apache
def parse_log_line(line: str) -> Dict[str, str]:
    """Analisa uma linha de log de acesso e retorna um dicionário com os campos extraídos."""
    match = re.match(APACHE_LOG_PATTERN, line)
    if not match:
        raise ValueError(f"Formato de linha de log de acesso inválido: {line}")
    
    return {
        'ip': match.group(1),
        'identity': match.group(2),
        'username': match.group(3),
        'timestamp': match.group(4),
        'method': match.group(5) or '-',
        'path': match.group(6) or '-',
        'protocol': match.group(7) or '-',
        'status': match.group(8),
        'size': match.group(9),
        'referer': match.group(10),
        'user_agent': match.group(11)
    }

# Função para parsear uma linha de log de erro do Apache
def parse_error_log_line(line: str) -> Dict[str, str]:
    """Analisa uma linha de log de erro e retorna um dicionário com os campos extraídos."""
    match = re.match(ERROR_LOG_PATTERN, line)
    if not match:
        raise ValueError(f"Formato de linha de log de erro inválido: {line}")
    
    return {
        'timestamp': match.group(1),
        'module': match.group(2),
        'severity': match.group(3),
        'message': match.group(4)
    }

# Função para inserir dados no PostgreSQL (logs de acesso)
def insert_log_into_postgresql(log_data: Dict[str, Any]):
    """Insere os dados de log de acesso no PostgreSQL."""
    try:
        connection = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = connection.cursor()

        insert_query = """
        INSERT INTO apache_logs (ip, identity, username, timestamp, method, path, protocol, status, size, referer, user_agent)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            log_data['ip'],
            log_data['identity'],
            log_data['username'],
            log_data['timestamp'],
            log_data['method'],
            log_data['path'],
            log_data['protocol'],
            log_data['status'],
            log_data['size'],
            log_data['referer'],
            log_data['user_agent']
        ))

        connection.commit()
        cursor.close()
        connection.close()
        logger.info("Log de acesso inserido com sucesso no PostgreSQL")
    except Exception as e:
        logger.error(f"Falha ao inserir log de acesso no PostgreSQL: {e}")

# Função para inserir dados no PostgreSQL (logs de erro)
def insert_error_log_into_postgresql(log_data: Dict[str, Any]):
    """Insere os dados de log de erro no PostgreSQL."""
    try:
        connection = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = connection.cursor()

        insert_query = """
        INSERT INTO apache_error_logs (timestamp, module, severity, message)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            log_data['timestamp'],
            log_data['module'],
            log_data['severity'],
            log_data['message']
        ))

        connection.commit()
        cursor.close()
        connection.close()
        logger.info("Log de erro inserido com sucesso no PostgreSQL")
    except Exception as e:
        logger.error(f"Falha ao inserir log de erro no PostgreSQL: {e}")

# Consumidor Kafka para logs de acesso
def consume_logs_from_kafka():
    """Consome logs de acesso do Kafka e os insere no PostgreSQL."""
    consumer = KafkaConsumer(
        CONFIG['kafka_topic'],
        bootstrap_servers=CONFIG['kafka_server'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='apache-log-consumers'
    )

    for message in consumer:
        log_data_str = message.value.decode('ascii')
        
        try:
            # Converte a string em um dicionário usando JSON
            log_data = json.loads(log_data_str)
            
            # Inserir log no PostgreSQL
            insert_log_into_postgresql(log_data)
        except json.JSONDecodeError as e:
            logger.error(f"Falha ao decodificar JSON: {e}")
        except Exception as e:
            logger.error(f"Erro ao inserir o log de acesso no PostgreSQL: {e}")

# Função principal para ler logs e enviar para o Kafka e PostgreSQL
def main():
    """Função principal para ler logs de acesso e erro do Apache."""
    producer = create_kafka_producer(CONFIG['kafka_server'])

    try:
        with open(CONFIG['log_file_path'], 'r') as access_log_file, \
             open(CONFIG['error_log_file_path'], 'r') as error_log_file:
            
            # Move para o final dos arquivos
            access_log_file.seek(0, 2)
            error_log_file.seek(0, 2)
            
            while True:
                access_line = access_log_file.readline()
                error_line = error_log_file.readline()

                # Processa logs de acesso e envia ao Kafka
                if access_line:
                    try:
                        log_data = parse_log_line(access_line.strip())
                        producer.send(CONFIG['kafka_topic'], value=json.dumps(log_data).encode('ascii'))
                        logger.info(f"Mensagem de access log enviada para o Kafka")
                    except ValueError as ve:
                        logger.warning(f"Pulando linha de log de acesso inválida: {ve}")
                    except Exception as e:
                        logger.error(f"Erro ao processar linha de log de acesso: {e}")

                # Processa logs de erro e insere diretamente no PostgreSQL
                if error_line:
                    try:
                        error_log_data = parse_error_log_line(error_line.strip())
                        insert_error_log_into_postgresql(error_log_data)
                        logger.info(f"Log de erro processado e inserido no PostgreSQL")
                    except ValueError as ve:
                        logger.warning(f"Pulando linha de log de erro inválida: {ve}")
                    except Exception as e:
                        logger.error(f"Erro ao processar linha de log de erro: {e}")

                time.sleep(CONFIG['sleep_time'])

    except KeyboardInterrupt:
        logger.info("Parando o conector...")
    finally:
        producer.close()
        logger.info("Conector parado.")

if __name__ == "__main__":
    # Cria uma thread para o produtor (main)
    producer_thread = threading.Thread(target=main)
    
    # Cria uma thread para o consumidor (consume_logs_from_kafka)
    consumer_thread = threading.Thread(target=consume_logs_from_kafka)
    
    # Inicia ambas as threads
    producer_thread.start()
    consumer_thread.start()
    
    # Espera que ambas as threads terminem
    producer_thread.join()
    consumer_thread.join()
