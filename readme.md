# 📊 topicos-sales-prediction

## 📌 Descripción del Proyecto
Proyecto de Big Data que implementa una **arquitectura Medallón** (Bronze, Silver, Gold) para procesar y analizar datos de ventas de e-commerce. Los datos son procesados con PySpark, almacenados en formato Parquet, y posteriormente exportados a MongoDB para su visualización en Power BI.

Dataset utilizado: [E-commerce Sales Prediction Dataset](https://www.kaggle.com/datasets/nevildhinoja/e-commerce-sales-prediction-dataset) (1000 registros, 7 columnas)

## 🏗️ Arquitectura Implementada
DATOS CRUDOS (CSV) → BRONZE (Parquet) → SILVER (Limpieza) → GOLD (KPIs) → MONGODB → POWER BI

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