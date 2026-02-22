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

## 📂 Estructura del Proyecto
topicos-sales-prediction/
├── .venv/ # Entorno virtual
├── config/ # Archivos de configuración
│ ├── mongodb_config.json
│ └── spark_config.json
├── data/ # Datos procesados (ignorado por git)
│ ├── bronze/ # Datos crudos en Parquet
│ ├── silver/ # Datos limpios
│ └── gold/ # KPIs y métricas
├── documentation/ # Anexos y evidencias
│ └── anexos/ # Capturas para el informe
│ ├── anexo1_foto_equipo.png
│ ├── anexo2_vscode_structure.png
│ ├── anexo3_github_structure.png
│ ├── anexo4_spark_executions/
│ ├── anexo5_mongodb_export/
│ └── anexo6_jenkins/
├── jenkins/ # Pipeline de CI/CD
│ └── Jenkinsfile
├── notebooks/ # Análisis exploratorio
├── scripts/ # Código PySpark por capas
│ ├── bronze_ingest/
│ │ └── load_to_hdfs.py
│ ├── silver_transform/
│ │ └── clean_data.py
│ ├── gold_aggregate/
│ │ └── calculate_kpis.py
│ └── mongodb_export/
│ └── export_to_mongodb.py
├── sql/ # Consultas Hive
│ ├── create_tables.hql
│ └── analytical_queries.hql
├── .gitignore
├── README.md
└── requirements.txt

## 👥 Integrantes
- Bazan Fernandez, Yover Ivan
- Perez Silva, Hayler
- Rojas Arevalo, Alejandra Nicole
© 2026. Todos los derechos reservados.