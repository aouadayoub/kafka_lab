from opensearchpy import OpenSearch
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize OpenSearch client
Client = OpenSearch(
    hosts=["http://52.6.122.23:9200"],  # Remplacez par l'adresse IP publique de votre instance EC2
    http_compress=True,  
    timeout=30,  
    retries=5,
    use_ssl=False,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False
)

INDEX_NAME = "wikimedia_changes"

try:
    # Test connection by checking if the index exists
    if Client.indices.exists(index=INDEX_NAME):
        logger.info(f"Index '{INDEX_NAME}' exists.")
    else:
        logger.info(f"Index '{INDEX_NAME}' does not exist.")
except Exception as e:
    logger.error(f"Error connecting to OpenSearch: {e}")
