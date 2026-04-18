"""
Módulo Productor - Simulador de Telemetría y Publicador Kafka
-------------------------------------------------------------
Este script actúa como el origen de datos (Data Source) en la arquitectura 
de monitoreo. Su responsabilidad es simular la generación continua de métricas 
de hardware y red (telemetría) para una flota de servidores, estructurar los 
eventos en formato JSON y publicarlos en un topic de Apache Kafka. 
Implementa un patrón de publicación síncrona, exigiendo confirmación de entrega 
(ACKs) al broker para garantizar la trazabilidad de extremo a extremo (End-to-End) 
antes del procesamiento analítico downstream.
"""

import random
import time
from datetime import datetime, timezone
import uuid
import json
from kafka import KafkaProducer

# ==============================================================================
# CONFIGURACIÓN DEL PRODUCTOR
# ==============================================================================

# Identificadores de los servidores monitorizados en la simulación.
SERVER_IDS = ["web01", "web02", "db01", "app01", "cache01"]

# Intervalo de tiempo (en segundos) entre el envío de cada bloque de métricas.
REPORTING_INTERVAL_SECONDS = 10

# Nombre del topic de Kafka de destino. 
TOPIC_NAME = "system-metrics-topic" 

# Dirección IP y puerto del broker de Kafka.
KAFKA_BROKER = "127.0.0.1:29092"

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
    print("Inicializando Productor de métricas para Apache Kafka...")
    
    # ==========================================================================
    # 1. INICIALIZACIÓN DEL CLIENTE KAFKA
    # ==========================================================================
    try:
        # Instanciación de KafkaProducer.
        # value_serializer: Función lambda para serializar diccionarios Python 
        # a formato JSON y codificarlos en bytes (UTF-8) para su transmisión.
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )
        print(f"[{obtener_hora()}] [*] Conexión establecida con Kafka en {KAFKA_BROKER}")
    except Exception as e:
        print(f"[{obtener_hora()}] [!] Error crítico de conexión con el broker: {e}")
        return

    print(f"[{obtener_hora()}] [*] Servidores configurados: {SERVER_IDS}")
    print(f"[{obtener_hora()}] [*] Topic objetivo: {TOPIC_NAME}")
    print("-" * 50)

    # ==========================================================================
    # 2. BUCLE PRINCIPAL DE GENERACIÓN Y ENVÍO DE MÉTRICAS
    # ==========================================================================
    try:
        while True:
            print(f"\n[{obtener_hora()}] Iniciando recolección de métricas...")

            # Iteración sobre el inventario de servidores
            for server_id in SERVER_IDS:
                
                # --- A. Simulación de variables de estado ---
                
                # Métrica: Uso de CPU (%)
                # Generación base (5-75%) con probabilidad del 10% de pico de carga (85-98%)
                cpu_percent = random.uniform(5.0, 75.0)
                if random.random() < 0.1: 
                    cpu_percent = random.uniform(85.0, 98.0)

                # Métrica: Uso de Memoria (%)
                # Generación base (20-85%) con probabilidad del 5% de pico de carga (90-99%)
                memory_percent = random.uniform(20.0, 85.0)
                if random.random() < 0.05: 
                    memory_percent = random.uniform(90.0, 99.0)

                # Métricas: I/O de Disco (MB/s) y Tráfico de Red (Mbps)
                disk_io_mbps = random.uniform(0.1, 50.0)
                network_mbps = random.uniform(1.0, 100.0)

                # Métrica: Tasa de Errores
                # Probabilidad del 8% de registrar entre 1 y 3 errores en el ciclo actual
                error_count = 0
                if random.random() < 0.08: 
                    error_count = random.randint(1, 3)

                # --- B. Estructuración del cuerpo del mensaje (JSON) ---
                # Construcción del diccionario conforme a la especificación requerida
                metric_message = {
                    "server_id": server_id,
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    "metrics": {
                        "cpu_percent": round(cpu_percent, 2),
                        "memory_percent": round(memory_percent, 2),
                        "disk_io_mbps": round(disk_io_mbps, 2),
                        "network_mbps": round(network_mbps, 2),
                        "error_count": error_count
                    },
                    "message_uuid": str(uuid.uuid4())
                }

                # --- C. Transmisión del mensaje CON RECIBO DE ENTREGA (Síncrono) ---
                try:
                    # producer.send() es asíncrono por defecto. Al encadenar .get(timeout=10),
                    # bloqueamos la ejecución un máximo de 10 segundos esperando el "ACK" (Acuse de recibo)
                    # del broker de Kafka. Esto garantiza una trazabilidad de extremo a extremo.
                    respuesta_kafka = producer.send(TOPIC_NAME, value=metric_message).get(timeout=10)
                    
                    # Extraemos de los metadatos el número de secuencia exacto (Offset) 
                    # que el clúster le ha asignado al mensaje. Fundamental para auditoría de datos.
                    offset_asignado = respuesta_kafka.offset
                    
                    print(f"    [{obtener_hora()}] [+] Métrica enviada - Servidor: {server_id} | Offset Kafka: {offset_asignado}")
                    
                except Exception as e:
                    # Capturamos posibles Timeouts, desconexiones súbitas o rechazos del broker
                    print(f"    [{obtener_hora()}] [!] Error de transmisión a Kafka (Servidor {server_id}): {e}")

            # Forzar la escritura de los buffers locales hacia el broker
            producer.flush()
            print(f"[{obtener_hora()}] [*] Ciclo completado. Intervalo de espera: {REPORTING_INTERVAL_SECONDS}s")
            
            # Suspensión del hilo hasta el próximo ciclo
            time.sleep(REPORTING_INTERVAL_SECONDS)

    # ==========================================================================
    # 3. MANEJO DE EXCEPCIONES Y LIMPIEZA DE RECURSOS
    # ==========================================================================
    except KeyboardInterrupt:
        print(f"\n[{obtener_hora()}] [*] Ejecución interrumpida por señal de usuario (SIGINT).")
    except Exception as e:
        print(f"\n[{obtener_hora()}] [!] Excepción no controlada durante el ciclo principal: {e}")
    finally:
        # Garantizar el cierre del socket de conexión con el broker
        print(f"[{obtener_hora()}] [*] Procediendo al cierre ordenado del cliente Kafka...")
        if 'producer' in locals():
            producer.close()
        print(f"[{obtener_hora()}] [*] Proceso finalizado.")

if __name__ == "__main__":
    main()