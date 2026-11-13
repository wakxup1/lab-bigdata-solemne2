import json
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

TOPIC_NAME = 'ridesharing_eventos'
TOTAL_EVENTOS = 150

print(f"Iniciando envío de {TOTAL_EVENTOS} eventos al tópico '{TOPIC_NAME}'...")

servicios = ['UberX', 'DiDi Express', 'VIP']
errores_enviados = 0
enviados_exitosos = 0

start_time = datetime.now() - timedelta(days=7)

for i in range(1, TOTAL_EVENTOS + 1):
    try:
        fecha_evento = start_time + timedelta(seconds=random.randint(0, 604800))
        hora = fecha_evento.hour
        servicio = random.choice(servicios)
        
        es_hora_punta = 1 if (7 <= hora <= 9) or (18 <= hora <= 20) else 0
        tarifa_base = 3000 if servicio == 'VIP' else 2000
        tarifa = tarifa_base + (1500 if es_hora_punta else 0) + random.randint(500, 4000)

        if i % 250 == 0:
            tarifa = None  # Nulo
            errores_enviados += 1
        elif i % 500 == 0:
            tarifa = -999.0  # Fuera de rango
            errores_enviados += 1

        evento = {
            "id_evento": f"EVT-{i:05d}",
            "marca_temporal": fecha_evento.strftime("%Y-%m-%d %H:%M:%S"),
            "tipo_servicio": servicio,
            "tarifa": tarifa,
            "hora_dia": hora,
            "distancia_km": round(random.uniform(1.0, 25.0), 2)
        }

        producer.send(TOPIC_NAME, value=evento)
        enviados_exitosos += 1

        if i % 1000 == 0:
            print(f"Enviados {i} eventos...")

    except Exception as e:
        print(f"Error al enviar el evento {i}: {e}")
        errores_enviados += 1

producer.flush()
print("¡Envío finalizado!")
print(f"Total exitosos: {enviados_exitosos}")
print(f"Errores / Anomalías introducidas: {errores_enviados}")