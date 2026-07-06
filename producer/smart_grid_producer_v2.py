"""
Smart Grid Data Producer - Versión con Confluent Kafka
Compatible con Python 3.14
"""

from confluent_kafka import Producer
import json
import time
import random
import logging
import sys

# ============================================================
# CONFIGURACIÓN
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "smartgrid"
EVENT_INTERVAL = 5

# ============================================================
# FUNCIONES DE GENERACIÓN DE DATOS
# ============================================================
def generate_smart_grid_data():
    """Genera datos simulados de la red eléctrica"""
    return {
        "timestamp": time.time(),
        "voltage_v": round(random.uniform(215, 245), 2),
        "current_a": round(random.uniform(100, 500), 2),
        "power_kw": round(random.uniform(20, 120), 2),
        "solar_kw": round(random.uniform(0, 250), 2),
        "wind_kw": round(random.uniform(0, 180), 2),
        "fault_num": random.randint(0, 3),
        "fault_indicator": random.choice(['normal', 'unstable', 'bad', 'blackout']),
        "temperature_c": round(random.uniform(18, 38), 1),
        "humidity_%": round(random.uniform(35, 95), 1),
        "electricity_price_gbp_per_kwh": round(random.uniform(0.08, 0.28), 3)
    }

# ============================================================
# CONEXIÓN A KAFKA
# ============================================================
def create_producer():
    """Crea el productor de Kafka"""
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'client.id': 'smart_grid_producer',
        'delivery.timeout.ms': 10000,
        'request.timeout.ms': 5000
    }
    
    try:
        producer = Producer(conf)
        logger.info("✅ Conectado a Kafka exitosamente")
        return producer
    except Exception as e:
        logger.error(f"❌ Error conectando a Kafka: {e}")
        sys.exit(1)

# ============================================================
# FUNCIÓN DE DELIVERY REPORT
# ============================================================
def delivery_report(err, msg):
    if err is not None:
        logger.error(f"❌ Error enviando mensaje: {err}")
    else:
        logger.info(f"✅ Mensaje enviado a {msg.topic()} [{msg.partition()}] en offset {msg.offset()}")

# ============================================================
# BUCLE PRINCIPAL
# ============================================================
if __name__ == "__main__":
    producer = create_producer()
    
    logger.info("=" * 60)
    logger.info("🚀 INICIANDO PRODUCTOR DE SMART GRID")
    logger.info(f"📤 Kafka Broker: {KAFKA_BOOTSTRAP}")
    logger.info(f"📡 Topic: {TOPIC}")
    logger.info(f"⏱️  Intervalo: {EVENT_INTERVAL}s")
    logger.info("=" * 60)
    
    event_counter = 0
    
    try:
        while True:
            # Generar datos
            data = generate_smart_grid_data()
            
            # Enviar a Kafka
            producer.produce(
                TOPIC,
                value=json.dumps(data).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)
            producer.flush()
            
            event_counter += 1
            logger.info(f"✅ Evento #{event_counter}: Power={data['power_kw']:.2f}kW, Fault={data['fault_indicator']}")
            
            time.sleep(EVENT_INTERVAL)
            
    except KeyboardInterrupt:
        logger.info("\n👋 Productor detenido por el usuario")
        
    except Exception as e:
        logger.error(f"❌ Error inesperado: {e}")
        
    finally:
        producer.flush()
        logger.info("✅ Productor cerrado correctamente")
