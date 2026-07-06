# Red-Electrica-Inteligente
__________________________________________________________________________________________________________________________________________________________________________________________________________________

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
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    PIPELINE DE DATOS EN TIEMPO REAL - VERSIÓN SENIOR                │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────────────┐   │
│  │   PRODUCTOR      │─────▶│     KAFKA        │─────▶│     FLINK SQL           │   │
│  │  (Python)        │      │   (Broker)       │      │   (Procesamiento)        │   │
│  │  smart_grid_     │      │   Topic:         │      │                          │   │
│  │  producer.py     │      │   smartgrid      │      │  • Agregaciones          │   │
│  └──────────────────┘      └──────────────────┘      │  • Alertas en tiempo real│   │
│                                                      └──────────┬───────────────┘   │
│                                                                 │                   │
│                                                              ▼                      │
│                                              ┌──────────────────────────────────┐   │
│                                              │    POSTGRESQL                    │   │
│                                              │    (Base de Datos)               │   │
│                                              │    • Métricas agregadas          │   │
│                                              │    • Alertas                     │   │
│                                              └──────────────────────────────────┘   │
│                                                                                     │
│  ┌──────────────────┐       ┌──────────────────┐                                    │
│  │   PROMETHEUS     │       │    GRAFANA       │                                    │
│  │   (Monitoreo)    │◀────▶│   (Dashboards)   │                                    │
│  └──────────────────┘       └──────────────────┘                                    │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
