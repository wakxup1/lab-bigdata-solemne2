1. Dependencias y versiones utilizadas:
   - Python 3.10+
   - Apache Kafka 7.3.0 (Confluent vía Docker)
   - PySpark 3.3+
   - kafka-python, pandas, matplotlib

2. Configuración básica de conexión y nombre del tópico:
   - Broker Kafka: localhost:9092
   - Tópico: ridesharing_eventos

3. Orden y comandos de ejecución:
   Paso A: Iniciar Docker Compose para Kafka y Zookeeper
   -> docker compose up -d

   Paso B: Ejecutar el consumidor en una terminal (para escuchar y guardar en archivo)
   -> python consumidor.py

   Paso C: Ejecutar el productor en otra terminal (para enviar los 5000 eventos)
   -> python productor.py

   Paso D: Ejecutar el script de análisis con PySpark
   -> python analisis_spark.py

4. Ubicación del archivo de datos:
   - El archivo generado por el consumidor se guarda localmente en la raíz como: viajes_raw.jsonl