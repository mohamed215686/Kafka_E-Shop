from confluent_kafka.admin import AdminClient 
 
config = { 
    "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094" 
} 
 
admin_client = AdminClient(config) 
 
try: 
    metadata = admin_client.list_topics(timeout=10) 
    print("Connexion au cluster Kafka réussie !") 
    print(f"Nombre de brokers détectés : {len(metadata.brokers)}") 
 
    for broker_id, broker in metadata.brokers.items(): 
        print(f"Broker {broker_id} : {broker.host}:{broker.port}") 
 
except Exception as error: 
    print("Échec de la connexion au cluster Kafka.") 
    print(f"Détail de l'erreur : {error}") 