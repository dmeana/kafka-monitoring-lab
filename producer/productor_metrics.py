# TODO: Implement Kafka Producer
import random
import time
from datetime import datetime, timezone
import uuid
import json

# --- Configuración ---
# Lista exacta de servidores que pide el enunciado
SERVER_IDS = ["web01", "web02", "db01", "app01", "cache01"]
REPORTING_INTERVAL_SECONDS = 10  # Tiempo de espera entre cada ciclo 

def main():
    print("Iniciando simulación de generación de métricas (Modo Prueba: Sin Kafka)...")
    print(f"Servidores simulados: {SERVER_IDS}")
    print(f"Intervalo de reporte: {REPORTING_INTERVAL_SECONDS} segundos")
    print("-" * 30)

    try:
        while True:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Generando reporte de métricas...")

            # Iterar sobre cada servidor para generar sus métricas individualmente
            for server_id in SERVER_IDS:

                # --- 1. SIMULACIÓN MATEMÁTICA ---
                # Uso de CPU base (5% - 75%)
                cpu_percent = random.uniform(5.0, 75.0)
                if random.random() < 0.1:  # 10% de probabilidad de tener un pico de CPU
                    cpu_percent = random.uniform(85.0, 98.0)

                # Uso de Memoria base (20% - 85%)
                memory_percent = random.uniform(20.0, 85.0)
                if random.random() < 0.05:  # 5% de probabilidad de tener un pico de RAM
                    memory_percent = random.uniform(90.0, 99.0)

                # Disco y Red
                disk_io_mbps = random.uniform(0.1, 50.0)
                network_mbps = random.uniform(1.0, 100.0)

                # Simulación de errores (poco frecuentes)
                error_count = 0
                if random.random() < 0.08:  # 8% de probabilidad de tener algún error
                    error_count = random.randint(1, 3)

                # --- 2. CREACIÓN DEL MENSAJE JSON ---
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
                    "message_uuid": str(uuid.uuid4()) # Identificador único
                }

                # --- 3. SALIDA POR PANTALLA (Para comprobar que funciona) ---
                print(f" -> Generado para {server_id}:")
                # Imprimimos el JSON formateado para que sea fácil de leer
                print(json.dumps(metric_message, indent=2))
            
            print(f"\nReporte completo de los 5 servidores generado. Esperando {REPORTING_INTERVAL_SECONDS} segundos...")
            time.sleep(REPORTING_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nSimulación detenida manualmente por el usuario.")

if __name__ == "__main__":
    main()