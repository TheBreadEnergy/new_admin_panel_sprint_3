import os

from dotenv import load_dotenv


load_dotenv()

# Database
POSTGRES_CONFIG = {
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('DB_PORT', 5432),
    'database': os.environ.get('POSTGRES_DB'),
    'user': os.environ.get('POSTGRES_USER'),
    'password': os.environ.get('POSTGRES_PASSWORD'),
}

# Elastic
ELASTICSEARCH_CONFIG = {
    'host': os.environ.get('ELASTIC_HOST', 'localhost'),
    'port': os.environ.get('ELASTIC_PORT', 9200),
    'scheme': 'http'
}
