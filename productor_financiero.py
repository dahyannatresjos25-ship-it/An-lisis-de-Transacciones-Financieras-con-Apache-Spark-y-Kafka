import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

tipos = ['TRANSFERENCIA', 'PAGO_TARJETA', 'RETIRO_CAJERO', 'DEPOSITO']

while True:
    data = {
        "trx_id": random.randint(100000, 999999),
        "tipo_trx": random.choice(tipos),
        "monto": round(random.uniform(50.0, 2000.0), 2),
        "timestamp": int(time.time())
    }
    producer.send('transacciones_vivo', value=data)
    print(f"Transacción enviada: {data}")
    time.sleep(2) # Simula llegada cada 2 segundos
