import json
from kafka import KafkaConsumer

TOPIC_NAME = 'ridesharing_eventos'
OUTPUT_FILE = 'viajes_raw.jsonl'

print("Iniciando consumidor Kafka... (Esperando eventos)")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='grupo-consumidor-ridesharing',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=10000  # Se detiene solo si pasan 10 segundos sin recibir datos
)

contador = 0
mensajes_mal_formados = 0
muestra = []

with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    for mensaje in consumer:
        try:
            data = mensaje.value
            if isinstance(data, dict) and "id_evento" in data:
                f.write(json.dumps(data) + '\n')
                contador += 1
                if contador <= 5:
                    muestra.append(data)
            else:
                mensajes_mal_formados += 1
        except Exception as e:
            mensajes_mal_formados += 1
            print(f"Error procesando mensaje: {e}")

print("\n--- Recepción Finalizada ---")
print(f"Total de eventos guardados en '{OUTPUT_FILE}': {contador}")
print(f"Mensajes no interpretados: {mensajes_mal_formados}")
print("\nMuestra de mensajes recibidos:")
for m in muestra:
    print(m)