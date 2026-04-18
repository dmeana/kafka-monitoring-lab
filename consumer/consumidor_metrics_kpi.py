"""
Módulo Consumidor KPI - Arquitectura de Monitoreo Analítico
-----------------------------------------------------------
Este script actúa como un consumidor analítico de Apache Kafka. 
Implementa un patrón de procesamiento por lotes mediante una "Ventana Tumbling" 
(Tumbling Window) basada en el recuento de mensajes. Su responsabilidad es 
agregar datos en tiempo real, calcular Indicadores Clave de Rendimiento (KPIs) 
y persistir los resultados agregados en MongoDB, garantizando la semántica 
"At-Least-Once" mediante la gestión manual de offsets.
"""

import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# ==============================================================================
# CONFIGURACIÓN GENERAL Y VARIABLES DE ENTORNO
# ==============================================================================
# Carga de credenciales y variables de entorno desde el archivo .env local
load_dotenv()

# Configuración del clúster de Kafka
KAFKA_BROKER = "127.0.0.1:29092"
TOPIC_NAME = "system-metrics-topic"

# Identificador del Consumer Group. 
# CRÍTICO: Debe ser diferente al del consumidor RAW ("grupo_raw_iabd03").
# Al usar un grupo distinto, Kafka aplica el patrón Publish/Subscribe, 
# enviando una copia idéntica del stream de datos a este script de forma concurrente.
GROUP_ID = "grupo_kpi_iabd03" 

# Configuración de almacenamiento en MongoDB Atlas
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "monitoring_db" 
COLLECTION_NAME = "system_metrics_kpis" # Colección dedicada exclusivamente a agregaciones

# Umbral de la Ventana Tumbling. Define la cantidad exacta de mensajes 
# que se deben acumular en memoria antes de disparar el cálculo analítico.
WINDOW_SIZE = 20

# ==============================================================================
# FUNCIONES AUXILIARES
# ==============================================================================
def obtener_hora():
    """Genera un timestamp formateado para la trazabilidad de los logs estándar."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ==============================================================================
# LÓGICA PRINCIPAL DEL CONSUMIDOR Y PROCESAMIENTO
# ==============================================================================
def main():
    print(f"[{obtener_hora()}] Inicializando Consumidor KPI (Ventana de {WINDOW_SIZE} mensajes)...")

    # ==========================================================================
    # 1. INICIALIZACIÓN DE CONEXIÓN A MONGODB ATLAS
    # ==========================================================================
    try:
        # Instanciación del cliente de MongoDB mediante la URI segura
        mongo_client = MongoClient(MONGO_URI)
        
        # Ejecución de un comando 'ping' para validar activamente la topología 
        # de red y la autenticación antes de iniciar el consumo.
        mongo_client.admin.command('ping')
        
        db = mongo_client[DB_NAME]
        collection = db[COLLECTION_NAME]
        print(f"[{obtener_hora()}] [*] Conectado a MongoDB Atlas. Colección objetivo: {COLLECTION_NAME}")
    except ConnectionFailure as e:
        print(f"[{obtener_hora()}] [!] Error crítico de red al conectar con MongoDB: {e}")
        return
    except Exception as e:
        print(f"[{obtener_hora()}] [!] Error de base de datos inesperado: {e}")
        return

    # ==========================================================================
    # 2. CONFIGURACIÓN E INICIALIZACIÓN DEL CLIENTE KAFKA
    # ==========================================================================
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=GROUP_ID,
            
            # auto_offset_reset='earliest': Garantiza que, en caso de primera ejecución, 
            # se procese el histórico completo para no perder métricas pasadas.
            auto_offset_reset='earliest',
            
            # enable_auto_commit=False: Deshabilita la confirmación asíncrona en segundo plano.
            # Fundamental para no marcar la ventana como procesada si ocurre un fallo a mitad del cálculo.
            enable_auto_commit=False,
            
            # value_deserializer: Función lambda para decodificar los bytes UTF-8 a diccionarios Python.
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print(f"[{obtener_hora()}] [*] Conectado a Kafka. Escuchando topic: {TOPIC_NAME}...")
        print("-" * 60)
    except Exception as e:
        print(f"[{obtener_hora()}] [!] Error crítico de conexión con el broker de Kafka: {e}")
        return

    # ==========================================================================
    # 3. GESTIÓN DEL ESTADO DE LA VENTANA (STATE MANAGEMENT)
    # ==========================================================================
    # Estructura de datos temporal para almacenar los mensajes en crudo hasta alcanzar el umbral
    ventana_mensajes = []
    # Marca de tiempo para calcular la tasa de procesamiento (throughput) de la ventana
    inicio_ventana = time.time()

    # ==========================================================================
    # 4. BUCLE DE INGESTA, AGREGACIÓN Y VOLCADO
    # ==========================================================================
    try:
        # El consumidor mantiene una conexión long-polling con el broker
        for message in consumer:
            payload = message.value
            ventana_mensajes.append(payload)
            
            # Trazabilidad visual del llenado del buffer
            print(f"    [{obtener_hora()}] Acumulando mensaje {len(ventana_mensajes)}/{WINDOW_SIZE} (Offset Kafka: {message.offset})")

            # --- EVALUACIÓN DE LA CONDICIÓN DE DISPARO (TRIGGER) ---
            if len(ventana_mensajes) == WINDOW_SIZE:
                fin_ventana = time.time()
                duracion_segundos = round(fin_ventana - inicio_ventana, 2)
                
                print(f"[{obtener_hora()}] [*] Ventana completada. Procesando agregaciones matemáticas...")

                # --- A. EXTRACCIÓN Y TRANSFORMACIÓN DE DATOS (ETL) ---
                # Usamos list comprehensions para aislar los vectores numéricos de la ventana
                cpus = [m["metrics"]["cpu_percent"] for m in ventana_mensajes]
                mems = [m["metrics"]["memory_percent"] for m in ventana_mensajes]
                disks = [m["metrics"]["disk_io_mbps"] for m in ventana_mensajes]
                nets = [m["metrics"]["network_mbps"] for m in ventana_mensajes]
                errores = [m["metrics"]["error_count"] for m in ventana_mensajes]

                # --- B. CONSTRUCCIÓN DEL DOCUMENTO KPI ---
                # Estructuramos el payload final cumpliendo con el esquema requerido
                kpi_document = {
                    "timestamp_kpi": datetime.now().isoformat(),
                    "duracion_ventana_segundos": duracion_segundos,
                    "numero_mensajes": WINDOW_SIZE,
                    "kpis": {
                        "promedio_cpu": round(sum(cpus) / WINDOW_SIZE, 2),
                        "promedio_memoria": round(sum(mems) / WINDOW_SIZE, 2),
                        "promedio_disco": round(sum(disks) / WINDOW_SIZE, 2),
                        "promedio_red": round(sum(nets) / WINDOW_SIZE, 2),
                        "suma_errores": sum(errores),
                        # Prevención de división por cero en entornos de ejecución hiper-rápidos
                        "tasa_procesamiento_msg_sec": round(WINDOW_SIZE / duracion_segundos, 2) if duracion_segundos > 0 else 0
                    }
                }

                # --- C. PERSISTENCIA Y COMMIT TRANSACCIONAL ---
                # PASO 1: Inserción del documento analítico en la base de datos
                result = collection.insert_one(kpi_document)
                
                # PASO 2: Confirmación de offsets en Kafka (Commit síncrono).
                # Solo se ejecuta si la inserción en BD fue exitosa, garantizando consistencia.
                consumer.commit()

                print(f"[{obtener_hora()}] [+] Documento KPI persistido | Mongo ID: {result.inserted_id}")
                print(f"[{obtener_hora()}]     -> Resumen: CPU Promedio {kpi_document['kpis']['promedio_cpu']}% | Errores Totales: {kpi_document['kpis']['suma_errores']}")
                print("-" * 60)

                # --- D. REINICIO DE ESTADO (RESET) ---
                # Vaciado del buffer de memoria y reinicio del cronómetro para el siguiente lote
                ventana_mensajes = []
                inicio_ventana = time.time()

    except KeyboardInterrupt:
        print(f"\n[{obtener_hora()}] [*] Ejecución interrumpida por señal de usuario (SIGINT).")
    except Exception as e:
        print(f"\n[{obtener_hora()}] [!] Excepción crítica no controlada en la tubería de datos: {e}")
    finally:
        # Garantizar la liberación de sockets y descriptores de archivo en el SO
        print(f"[{obtener_hora()}] [*] Procediendo al cierre ordenado de conexiones...")
        if 'consumer' in locals(): 
            consumer.close()
        if 'mongo_client' in locals(): 
            mongo_client.close()
        print(f"[{obtener_hora()}] [*] Proceso finalizado.")

if __name__ == "__main__":
    if not MONGO_URI:
        print(f"[{obtener_hora()}] [!] ERROR FATAL: No se ha encontrado la variable MONGO_URI en el entorno.")
    else:
        main()