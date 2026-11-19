import json 
from confluent_kafka import Producer 


def delivery_report(error, message): 
    if error is not None: 
        print(f"Erreur de livraison : {error}") 
 
    else: 
        print( 
            f"Message livré : topic={message.topic()}, " 
            f"partition={message.partition()}, " 
            f"offset={message.offset()}" 
        ) 

config = { 
    "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094", 
    "client.id": "E-Shop-producer", 
    "acks": "all", 
    "enable.idempotence": True 
} 

producer = Producer(config)

orders = [ 
    { 
        "event_id": "EVT-004", 
        "event_type": "ORDER_CREATED", 
        "order_id": "ORD-1010", 
        "customer_id": "C004", 
        "amount": 256, 
        "status": "PAID" 
    }, 
    { 
        "event_id": "EVT-005", 
        "event_type": "ORDER_CREATED", 
        "order_id": "ORD-1009", 
        "customer_id": "C001", 
        "amount": 230, 
        "status": "PENDING" 
    }, 
    { 
        "event_id": "EVT-005", 
        "event_type": "ORDER_CREATED", 
        "order_id": "ORD-1007", 
        "customer_id": "C002", 
        "amount": 400, 
        "status": "PAID" 
    }
] 

for order in orders: 
 
    key =  str(order["customer_id"])
 
    value = json.dumps(order).encode("utf-8")
 
    producer.produce( 
        topic="orders", 
        key=key, 
        value=value ,
        on_delivery=delivery_report) 
    
    producer.poll(0) 

    print(f"Commande publiée : {order['order_id']}") 


# Attendre la fin des publications pendant 10 secondes maximum 
remaining = producer.flush(10) 
 
# Vérifier si tous les messages ont été traités 
if remaining == 0: 
    print("Toutes les publications ont été traitées.") 
else: 
    print(f"{remaining} message(s) encore en attente.") 

