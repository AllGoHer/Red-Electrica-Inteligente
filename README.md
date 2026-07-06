# Red-Electrica-Inteligente
________________________________________________________________________________________________________________________________________________________________________________________________________________
📊 Descripción General del Proyecto
Este proyecto implementa un pipeline de streaming en tiempo real de nivel producción para el monitoreo y análisis de redes eléctricas inteligentes. Simula un flujo completo de ingeniería de datos que ingiere, procesa y visualiza datos de la red eléctrica en tiempo real utilizando herramientas estándar de la industria.

No es una demo simple. Es una arquitectura lista para producción que demuestra:

✅ Ingesta de datos en tiempo real con Apache Kafka

✅ Procesamiento de streams con Apache Flink SQL

✅ Persistencia de datos con PostgreSQL

✅ Monitoreo del sistema con Prometheus

✅ Visualización con Grafana y Kafka UI

El pipeline procesa datos simulados de medidores inteligentes, generando métricas agregadas (potencia promedio, generación solar, precios de electricidad) y alertas en tiempo real para anomalías en la red.

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🏗️ Arquitectura del Sistema
________________________________________________________________________________________________________________________________________________________________________________________________________________

![image](https://github.com/user-attachments/assets/f9c5ac42-8b39-4078-8c76-9e1812286489)

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🛠️ Tecnologías Utilizadas
________________________________________________________________________________________________________________________________________________________________________________________________________________
| Componente | Tecnología |	Propósito |
|------------|------------|-----------|
| Broker de Streaming |	Apache Kafka | Ingesta de datos en tiempo real |
| Procesamiento | Apache Flink SQL | Análisis en tiempo real |
| Base de Datos | PostgreSQL | Almacenamiento persistente |
| Monitoreo | Prometheus | Métricas de salud del sistema |
| Visualización | Grafana |	Dashboards y análisis |
| Orquestación | Docker Compose | Infraestructura local |
| Generación de Datos | Python (Confluent Kafka) | Simulador de red eléctrica |


________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📁 Estructura del Proyecto
________________________________________________________________________________________________________________________________________________________________________________________________________________

![image](https://github.com/user-attachments/assets/d5af3122-321a-4895-acc3-424bba08aaf3)

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🚀 Guía de Inicio Rápido
________________________________________________________________________________________________________________________________________________________________________________________________________________

**Requisitos Previos**

* Docker Desktop instalado

* Python 3.9+ (para el productor local)

* Git

________________________________________________________________________________________________________________________________________________________________________________________________________________
**Paso 1: Clonar el Repositorio**

bash:

      git clone https://github.com/tu-usuario/smart-grid-project.git
      cd smart-grid-project

________________________________________________________________________________________________________________________________________________________________________________________________________________
**Paso 2: Levantar la Infraestructura**

bash:

      docker-compose up -d

Esto inicia:

* PostgreSQL (puerto 5432)

* Apache Kafka (puertos 9092, 29092)

* Apache Flink JobManager (puerto 8081)

* Apache Flink TaskManager

* Prometheus (puerto 9090)

* Grafana (puerto 3000)

* Kafka UI (puerto 8090)

________________________________________________________________________________________________________________________________________________________________________________________________________________
**Paso 3: Crear el Tema en Kafka**

bash:

      docker exec smart_grid_kafka /opt/kafka/bin/kafka-topics.sh \
       --create \
       --topic smartgrid \
       --bootstrap-server localhost:9092 \
       --partitions 3 \
       --replication-factor 1

________________________________________________________________________________________________________________________________________________________________________________________________________________
**Paso 4: Instalar Dependencias**

bash:

      python -m pip install confluent-kafka

________________________________________________________________________________________________________________________________________________________________________________________________________________
**Paso 5: Ejecutar el Productor de Datos**

bash:

      cd producer
      python smart_grid_producer_v2.py

________________________________________________________________________________________________________________________________________________________________________________________________________________
**Paso 6: Descargar los Conectores de Flink**

bash:

      docker exec -it smart_grid_flink_jobmanager bash
      cd /opt/flink/lib
      wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.0/flink-sql-connector-kafka-1.17.0.jar
      wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/1.16.0/flink-connector-jdbc-1.16.0.jar
      wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
      exit
      docker-compose restart jobmanager taskmanager

________________________________________________________________________________________________________________________________________________________________________________________________________________
**Paso 7: Ejecutar el Procesamiento con Flink SQL**

bash:

      docker exec -it smart_grid_flink_jobmanager bash
      cd /opt/flink
      ./bin/sql-client.sh -f /opt/flink/usrlib-sql/smart_grid.sql -j /opt/flink/lib/flink-sql-connector-kafka-1.17.0.jar

________________________________________________________________________________________________________________________________________________________________________________________________________________
**Paso 8: Verificar los Resultados**

bash:

      docker exec -it smart_grid_postgres psql -U admin -d smartgrid


Ejecuta consultas:

          sql
          -- Ver métricas agregadas
          SELECT * FROM grid_metrics ORDER BY window_start DESC LIMIT 10;

          -- Ver alertas en tiempo real
          SELECT * FROM grid_alerts ORDER BY alert_time DESC LIMIT 10;

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📊 Monitoreo y Visualización
________________________________________________________________________________________________________________________________________________________________________________________________________________

**Dashboard de Flink**

* URL: http://localhost:8081

* Monitorea jobs en ejecución, estado de tareas y uso de recursos

**Kafka UI**

* URL: http://localhost:8090

* Visualiza topics, mensajes y grupos de consumidores

**Grafana**

* URL: http://localhost:3000

* Usuario: admin

* Contraseña: admin

* Configura PostgreSQL como fuente de datos para crear dashboards personalizados

**Prometheus**

* URL: http://localhost:9090

* Monitorea métricas del sistema y salud de servicios

________________________________________________________________________________________________________________________________________________________________________________________________________________

**🧪 Detalles del Procesamiento de Datos**
________________________________________________________________________________________________________________________________________________________________________________________________________________

**Fuente de Datos (Productor)**

El productor en Python simula sensores de la red eléctrica generando datos cada 5 segundos:

json:

      {
        "timestamp": 1765642790.4662578,
        "voltage_v": 231.96,
        "current_a": 397.19,
        "power_kw": 86.6,
        "solar_kw": 114.84,
        "wind_kw": 128.9,
        "fault_num": 0,
        "fault_indicator": "normal",
        "temperature_c": 26.1,
        "humidity_%": 74.9,
        "electricity_price_gbp_per_kwh": 0.091
      }

________________________________________________________________________________________________________________________________________________________________________________________________________________
**Procesamiento en Streaming (Flink SQL)**
________________________________________________________________________________________________________________________________________________________________________________________________________________
Flink SQL procesa los datos en tiempo real con:

1. Ventanas deslizantes de 1 minuto para agregaciones

2. Watermarking (5 segundos) para manejar datos tardíos

3. Generación de alertas en tiempo real para anomalías

4. Validación de calidad de datos

________________________________________________________________________________________________________________________________________________________________________________________________________________
**Tablas de Salida**

<mark>**grid_metrics**</mark> - **Métricas Agregadas**

| Columna | Descripción |
|---------|-------------|
| window_start | Inicio de la ventana de agregación |
| window_end | Fin de la ventana de agregación |
| avg_power_kw | Potencia promedio consumida |
| max_fault_num | Severidad máxima de falla |
| total_solar_generated | Energía solar total generada |
| avg_price | Precio promedio de electricidad |

<mark>**grid_alerts**</mark> - **Alertas en Tiempo Real**

| Columna | Descripción |
|---------|-------------|
| alert_time | Timestamp de la alerta |
| alert_type | Tipo de anomalía (CAÍDA_VOLTAJE, SOBRECARGA, RIESGO_APAGÓN) |
| fault_num | Nivel de severidad (1-3) |
| power_kw | Potencia al momento de la alerta |
| severity | Criticidad (ALTA, CRÍTICA) |

________________________________________________________________________________________________________________________________________________________________________________________________________________
**💡 Características Principales**

✅ Streaming en Tiempo Real

* Ingesta continua de datos a través de Kafka

* Latencia de procesamiento inferior a 5 segundos

✅ Tolerancia a Fallos

* Checkpointing para procesamiento exactly-once

* Recuperación de estado ante fallos

✅ Calidad de Datos

* Validación de esquemas con Flink SQL

* Detección automática de anomalías

✅ Arquitectura Escalable

* Microservicios contenerizados

* Capacidad de escalado horizontal

✅ Monitoreo en Producción

* Exportación de métricas a Prometheus

* Dashboards en Grafana listos para usar

________________________________________________________________________________________________________________________________________________________________________________________________________________

**🛑 Detener el Proyecto**

bash:

      # Detener el productor (Ctrl+C en la terminal del productor)
      # Detener Flink SQL (Ctrl+C en la terminal de Flink)
      docker-compose down

______________________________________________________________________________________________________________________________________________________________________________________________________________

**📚 Recursos y Referencias**

* Documentación de Apache Kafka

* Documentación de Apache Flink

* Guía de Flink SQL

* Documentación de PostgreSQL

* Documentación de Grafana

________________________________________________________________________________________________________________________________________________________________________________________________________________

**📬 Conéctate**
________________________________________________________________________________________________________________________________________________________________________________________________________________

GitHub: https://github.com/AllGoHer

LinkedIn: https://www.linkedin.com/in/allan-gonzales-heredia-13a557b5/

Correo Electronico: allgoher007@gmail.com

Portfolio: 

Construido con ❤️ y ☕ por <mark>**Allan Gonzales Heredia**</mark>

Última actualización: Julio 2026

________________________________________________________________________________________________________________________________________________________________________________________________________________

⚡ Comandos Rápidos de Referencia
________________________________________________________________________________________________________________________________________________________________________________________________________________

bash:

      # Ejecución completa del pipeline
      docker-compose up -d
      docker exec smart_grid_kafka /opt/kafka/bin/kafka-topics.sh --create --topic smartgrid --bootstrap-server localhost:9092
      python producer/smart_grid_producer_v2.py
      docker exec -it smart_grid_flink_jobmanager bash -c "cd /opt/flink && ./bin/sql-client.sh -f /opt/flink/usrlib-sql/smart_grid.sql -j /opt/flink/lib/*.jar"

      # Verificar datos en PostgreSQL
      docker exec -it smart_grid_postgres psql -U admin -d smartgrid
      SELECT * FROM grid_metrics ORDER BY window_start DESC LIMIT 10;

      # Ver mensajes en Kafka
      docker exec smart_grid_kafka /opt/kafka/bin/kafka-console-consumer.sh --topic smartgrid --bootstrap-server localhost:9092 --from-beginning --max-messages 3



