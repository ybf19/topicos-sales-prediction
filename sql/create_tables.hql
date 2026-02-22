-- =====================================================
-- SCRIPT: create_tables.hql
-- PROPOSITO: Crear tablas externas en Hive para las capas Silver y Gold
-- AUTOR: Proyecto Big Data
-- FECHA: 2026-02-22
-- =====================================================

-- 1. CREAR BASE DE DATOS
CREATE DATABASE IF NOT EXISTS ecommerce_db;
USE ecommerce_db;

-- =====================================================
-- TABLAS DE LA CAPA SILVER (DATOS LIMPIOS)
-- =====================================================

-- 1.1 Tabla de ventas limpias (capa Silver)
CREATE EXTERNAL TABLE IF NOT EXISTS ventas_silver (
    fecha STRING,
    categoria_producto STRING,
    precio DOUBLE,
    descuento DOUBLE,
    segmento_cliente STRING,
    gasto_marketing DOUBLE,
    unidades_vendidas INT
)
COMMENT 'Datos limpios de ventas - Capa Silver'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/hadoop/warehouse/ecommerce_db/ventas_silver'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =====================================================
-- TABLAS DE LA CAPA GOLD (KPIS Y MÉTRICAS)
-- =====================================================

-- 1.2 Tabla de ventas por categoría (Gold)
CREATE EXTERNAL TABLE IF NOT EXISTS ventas_categoria_gold (
    categoria_producto STRING,
    total_ventas DOUBLE,
    precio_promedio DOUBLE,
    unidades_vendidas INT,
    num_transacciones INT
)
COMMENT 'Ventas agregadas por categoría - Capa Gold'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/hadoop/warehouse/ecommerce_db/ventas_categoria_gold';

-- 1.3 Tabla de descuentos por segmento (Gold)
CREATE EXTERNAL TABLE IF NOT EXISTS descuentos_segmento_gold (
    segmento_cliente STRING,
    descuento_promedio DOUBLE,
    precio_promedio DOUBLE,
    num_compras INT
)
COMMENT 'Descuentos promedio por segmento - Capa Gold'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/hadoop/warehouse/ecommerce_db/descuentos_segmento_gold';

-- 1.4 Tabla de top categorías (Gold)
CREATE EXTERNAL TABLE IF NOT EXISTS top_categorias_gold (
    categoria_producto STRING,
    unidades_vendidas INT
)
COMMENT 'Top 5 categorías más vendidas - Capa Gold'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/hadoop/warehouse/ecommerce_db/top_categorias_gold';

-- =====================================================
-- VERIFICACIÓN DE TABLAS CREADAS
-- =====================================================
SHOW TABLES;