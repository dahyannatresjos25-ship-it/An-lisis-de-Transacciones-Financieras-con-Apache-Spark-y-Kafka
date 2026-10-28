import csv
import random
from datetime import datetime, timedelta

tipos = ['TRANSFERENCIA', 'PAGO_TARJETA', 'RETIRO_CAJERO', 'DEPOSITO']
with open('transacciones_historicas.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['trx_id', 'tipo_trx', 'monto', 'fecha'])
    for i in range(1, 100001):
        tipo = random.choice(tipos)
        monto = round(random.uniform(10.0, 5000.0), 2)
        fecha = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d %H:%M:%S')
        writer.writerow([i, tipo, monto, fecha])
print("Dataset transacciones_historicas.csv generado con exito (100,000 registros).")
