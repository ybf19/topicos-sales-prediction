# 📊 topicos-sales-prediction

## 📌 Descripción del Proyecto
Proyecto de Big Data que implementa una **arquitectura Medallón** (Bronze, Silver, Gold) para procesar y analizar datos de ventas de e-commerce. Los datos son procesados con PySpark, almacenados en formato Parquet, y posteriormente exportados a MongoDB para su visualización en Power BI.

El objetivo es transformar datos crudos en información analítica confiable que permita:
- Identificar patrones de ventas
- Analizar comportamiento de clientes
- Generar KPIs estratégicos
- Facilitar la toma de decisiones comerciales

Dataset utilizado: [E-commerce Sales Prediction Dataset](https://www.kaggle.com/datasets/nevildhinoja/e-commerce-sales-prediction-dataset) (1000 registros, 7 columnas)

## 🏗️ Arquitectura Implementada

CSV (Datos crudos)
        ↓
BRONZE (Raw - Parquet en HDFS)
        ↓
SILVER (Datos limpios y transformados)
        ↓
GOLD (KPIs y métricas agregadas)
        ↓
MongoDB (Persistencia NoSQL)
        ↓
Power BI (Visualización)

🔹 Bronze (Raw Layer)
- Ingesta de datos CSV en HDFS
- Conversión a formato Parquet
- Sin transformaciones

🔹 Silver (Curated Layer)
- Limpieza de nulos
-Eliminación de duplicados
-Corrección de tipos de datos
-Estandarización de columnas

🔹 Gold (Business Layer)
Cálculo de KPIs:
  - Total de ventas
  - Ventas por categoría
  - Ventas por cliente
  - Ticket promedio

## 🛠️ Tecnologías Utilizadas
| Tecnología | Versión | Propósito |
|------------|---------|-----------|
| Python | 3.12 | Lenguaje principal |
| PySpark | 3.5.8 | Procesamiento distribuido |
| Hadoop HDFS | 3.3.6 | Sistema de archivos distribuido |
| Hive | 3.1.3 | Consultas SQL sobre Big Data |
| MongoDB | 8.0 | Base de datos NoSQL |
| Power BI | - | Visualización de datos |
| Git/GitHub | - | Control de versiones |
| Jenkins | - | Integración continua |

## 👥 Integrantes
- Bazan Fernandez, Yover Ivan
- Perez Silva, Hayler
- Rojas Arevalo, Alejandra Nicole
##
© 2026. Todos los derechos reservados.
