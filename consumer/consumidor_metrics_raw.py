"""
Módulo Consumidor RAW - Arquitectura de Monitoreo de Ingesta
------------------------------------------------------------
Este script actúa como el consumidor principal de ingesta de Apache Kafka. 
Implementa un patrón de extracción y carga directa (EL) sin transformación. 
Su responsabilidad exclusiva es capturar el flujo de métricas en bruto (raw data) 
en tiempo real y persistirlo de forma inmutable en MongoDB, creando un registro 
histórico auditable. Garantiza la semántica de entrega "At-Least-Once" mediante 
la gestión manual de offsets y opera de forma aislada en su propio Consumer Group.
"""

import os
import json
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# ==============================================================================
# CONFIGURACIÓN GENERAL Y VARIABLES DE ENTORNO
# ==============================================================================
# Cargamos las variables ocultas del archivo .env
load_dotenv()

# Configuración de Kafka
KAFKA_BROKER = "127.0.0.1:29092"
TOPIC_NAME = "system-metrics-topic"
# Identificador único para este grupo de consumidores (evita conflictos con el script KPI)
GROUP_ID = "grupo_raw_iabd03" 

# Configuración de MongoDB Atlas
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "monitoring_db" # Nombre de tu base de datos en Atlas
COLLECTION_NAME = "system_metrics_raw" # Nombre exacto que pide el profesor

# ==============================================================================
# FUNCIÓN AUXILIAR
# ==============================================================================
def obtener_hora():
    """Devuelve la hora actual formateada para los logs."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================
def main():
    print(f"[{obtener_hora()}] Inicializando Consumidor RAW para almacenamiento en bruto...")

    # ==========================================================================
    # 1. CONEXIÓN A MONGODB ATLAS
    # ==========================================================================
    try:
        # Conectamos al cluster de Atlas usando la URI del .env
        mongo_client = MongoClient(MONGO_URI)
        # Hacemos un "ping" para forzar la comprobación de la conexión
        mongo_client.admin.command('ping')
        
        db = mongo_client[DB_NAME]
        collection = db[COLLECTION_NAME]
        print(f"[{obtener_hora()}] [*] Conexión establecida exitosamente con MongoDB Atlas.")
    except ConnectionFailure as e:
        print(f"[{obtener_hora()}] [!] Error crítico al conectar con MongoDB: {e}")
        return
    except Exception as e:
        print(f"[{obtener_hora()}] [!] Error inesperado con MongoDB: {e}")
        return

    # ==========================================================================
    # 2. CONEXIÓN A KAFKA (CONSUMIDOR)
    # ==========================================================================
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=GROUP_ID,
            # Leemos desde el principio si hay mensajes atrasados (earliest)
            auto_offset_reset='earliest',
            # DESACTIVAMOS el auto-commit para gestionar los offsets manualmente
            # Esto garantiza que no marquemos un mensaje como leído si falla la inserción en Mongo
            enable_auto_commit=False,
            # Deserializamos: De bytes UTF-8 a cadena de texto, y de texto a Diccionario Python
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print(f"[{obtener_hora()}] [*] Conectado a Kafka. Escuchando topic: {TOPIC_NAME}...")
        print("-" * 50)
    except Exception as e:
        print(f"[{obtener_hora()}] [!] Error crítico de conexión con el broker de Kafka: {e}")
        return

    # ==========================================================================
    # 3. BUCLE DE CONSUMO E INSERCIÓN
    # ==========================================================================
    try:
        # El consumidor se queda escuchando indefinidamente
        for message in consumer:
            # Extraemos el payload (el diccionario con los datos del servidor)
            payload = message.value
            
            # Insertamos el documento tal cual (en bruto/raw) en MongoDB
            # Nota: Mongo le añadirá automáticamente un campo '_id' único
            result = collection.insert_one(payload)
            
            server = payload.get("server_id", "Desconocido")
            
            # Si la inserción fue exitosa, confirmamos el Offset a Kafka (COMMIT)
            # Si el script se cayera antes de esta línea, Kafka nos volvería a mandar el mensaje
            # al reiniciar, evitando la pérdida de datos.
            consumer.commit()
            
            print(f"[{obtener_hora()}] [+] Métrica de {server} guardada en Mongo | ID: {result.inserted_id}| Offset: {message.offset}")

    except KeyboardInterrupt:
        print(f"\n[{obtener_hora()}] [*] Ejecución interrumpida por el usuario (SIGINT).")
    except Exception as e:
        print(f"\n[{obtener_hora()}] [!] Excepción no controlada durante el consumo: {e}")
    finally:
        # Limpieza ordenada de recursos
        print(f"[{obtener_hora()}] [*] Cerrando conexiones...")
        if 'consumer' in locals():
            consumer.close()
        if 'mongo_client' in locals():
            mongo_client.close()
        print(f"[{obtener_hora()}] [*] Proceso finalizado.")

if __name__ == "__main__":
    if not MONGO_URI:
        print(f"[{obtener_hora()}] [!] ERROR: No se ha encontrado MONGO_URI en el archivo .env")
    else:
        main()