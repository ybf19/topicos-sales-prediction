#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script: bronze_ingest/load_to_hdfs.py
Propósito: Cargar datos crudos desde CSV a formato Parquet (capa Bronze)
"""

from pyspark.sql import SparkSession
import os
import sys

def main():
    print("="*60)
    print("🚀 INICIANDO INGESTA A CAPA BRONZE")
    print("="*60)
    
    # 1. Crear sesión de Spark
    print("\n📊 Creando sesión de Spark...")
    spark = SparkSession.builder \
        .appName("Bronze_Ingest_Ecommerce") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    
    # 2. Ruta del CSV (ya verificada)
    ruta_csv = "/mnt/c/Users/Usuario/Downloads/Ecommerce_Sales_Prediction_Dataset.csv"
    
    # 3. Verificar que el CSV existe
    print(f"\n🔍 Verificando archivo CSV: {ruta_csv}")
    
    if not os.path.exists(ruta_csv):
        print(f"❌ ERROR: No se encuentra el archivo")
        sys.exit(1)
    
    print(f"✅ Archivo encontrado: {ruta_csv}")
    
    # 4. Leer CSV con Spark
    print("\n📂 Leyendo archivo CSV...")
    try:
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(f"file://{ruta_csv}")
        
        print(f"✅ CSV cargado exitosamente")
        
    except Exception as e:
        print(f"❌ Error al leer el CSV: {str(e)}")
        sys.exit(1)
    
    # 5. Mostrar información del dataset
    num_filas = df.count()
    num_columnas = len(df.columns)
    
    print(f"\n📊 ESTADÍSTICAS DEL DATASET:")
    print(f"   • Filas: {num_filas}")
    print(f"   • Columnas: {num_columnas}")
    
    print("\n📋 Esquema de datos:")
    df.printSchema()
    
    print("\n👀 Primeras 5 filas:")
    df.show(5, truncate=False)
    
    # 6. Guardar en LOCAL (capa Bronze)
    ruta_local = "/home/hadoop/topicos-sales-prediction/data/bronze/ecommerce"
    
    print(f"\n💾 Guardando datos en LOCAL...")
    print(f"   Ruta: {ruta_local}")
    
    # Crear directorio si no existe
    os.makedirs(ruta_local, exist_ok=True)
    
    try:
        df.write \
            .mode("overwrite") \
            .format("parquet") \
            .option("compression", "snappy") \
            .save(f"file://{ruta_local}")
        
        print(f"✅ Datos guardados exitosamente en LOCAL")
        
    except Exception as e:
        print(f"❌ Error al guardar: {str(e)}")
        sys.exit(1)
    
    # 7. Verificar archivos guardados
    print("\n🔍 Archivos guardados:")
    os.system(f"ls -la {ruta_local}")
    
    # 8. Resumen final
    print("\n" + "="*60)
    print("✅ INGESTA A BRONZE COMPLETADA EXITOSAMENTE")
    print("="*60)
    print(f"\n📁 Datos guardados en: {ruta_local}")
    print(f"📊 Total de registros: {num_filas}")
    print(f"📋 Columnas disponibles: {num_columnas}")
    
    # 9. Cerrar sesión
    spark.stop()
    print("\n👋 Sesión de Spark cerrada")

if __name__ == "__main__":
    main()