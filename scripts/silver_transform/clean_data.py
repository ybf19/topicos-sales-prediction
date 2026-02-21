#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script: silver_transform/clean_data.py
Propósito: Limpiar y transformar datos de la capa Bronze (capa Silver)
Autor: Proyecto Big Data
Fecha: 2026-02-21
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, trim, upper, regexp_replace
from pyspark.sql.types import DateType
import os
import sys

def main():
    print("="*60)
    print("🚀 INICIANDO TRANSFORMACIÓN A CAPA SILVER")
    print("="*60)
    
    # 1. Crear sesión de Spark
    print("\n📊 Creando sesión de Spark...")
    spark = SparkSession.builder \
        .appName("Silver_Transform_Ecommerce") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    
    # 2. Ruta de entrada (Bronze) y salida (Silver)
    ruta_bronze = "/home/hadoop/topicos-sales-prediction/data/bronze/ecommerce"
    ruta_silver = "/home/hadoop/topicos-sales-prediction/data/silver/ecommerce"
    
    # 3. Verificar que existe la carpeta Bronze
    print(f"\n🔍 Verificando datos en Bronze: {ruta_bronze}")
    if not os.path.exists(ruta_bronze):
        print(f"❌ ERROR: No se encuentra la carpeta Bronze")
        print(f"   Ejecuta primero el script bronze_ingest/load_to_hdfs.py")
        sys.exit(1)
    
    print(f"✅ Datos Bronze encontrados")
    
    # 4. Leer datos desde Bronze
    print(f"\n📂 Leyendo datos desde Bronze...")
    try:
        df_bronze = spark.read.parquet(f"file://{ruta_bronze}")
        num_filas_original = df_bronze.count()
        print(f"✅ Datos cargados: {num_filas_original} filas, {len(df_bronze.columns)} columnas")
    except Exception as e:
        print(f"❌ Error al leer datos Bronze: {str(e)}")
        sys.exit(1)
    
    print("\n📋 Esquema original:")
    df_bronze.printSchema()
    
    print("\n👀 Primeras 5 filas (originales):")
    df_bronze.show(5, truncate=False)
    
    # 5. PROCESO DE LIMPIEZA - TRANSFORMACIONES SILVER
    
    print("\n🧹 Iniciando limpieza de datos...")
    df_silver = df_bronze
    
    # 5.1 Eliminar filas completamente nulas
    filas_iniciales = df_silver.count()
    df_silver = df_silver.dropna(how='all')
    filas_sin_nulas_total = df_silver.count()
    if filas_iniciales > filas_sin_nulas_total:
        print(f"   • Eliminadas {filas_iniciales - filas_sin_nulas_total} filas completamente nulas")
    
    # 5.2 Verificar y manejar nulos por columna
    print("\n📊 Verificando valores nulos por columna:")
    for col_name in df_silver.columns:
        nulos = df_silver.filter(df_silver[col_name].isNull()).count()
        if nulos > 0:
            print(f"   • {col_name}: {nulos} valores nulos")
            # Para columnas numéricas, rellenar con 0 o media
            if col_name in ["Price", "Discount", "Marketing_Spend", "Units_Sold"]:
                media = df_silver.select(col_name).agg({col_name: "avg"}).collect()[0][0]
                df_silver = df_silver.fillna({col_name: media if media else 0})
                print(f"     → Rellenados con media: {media:.2f}")
            # Para columnas string, rellenar con 'Desconocido'
            else:
                df_silver = df_silver.fillna({col_name: "Desconocido"})
                print(f"     → Rellenados con 'Desconocido'")
    
    # 5.3 Eliminar duplicados
    df_silver = df_silver.dropDuplicates()
    filas_sin_duplicados = df_silver.count()
    if filas_sin_duplicados < filas_sin_nulas_total:
        print(f"\n   • Eliminados {filas_sin_nulos_total - filas_sin_duplicados} duplicados")
    
    # 5.4 Limpiar y estandarizar textos
    print("\n✏️ Estandarizando campos de texto...")
    # Convertir a mayúsculas y limpiar espacios
    for col_name in ["Product_Category", "Customer_Segment"]:
        if col_name in df_silver.columns:
            df_silver = df_silver.withColumn(col_name, trim(upper(col_name)))
            print(f"   • {col_name}: convertido a mayúsculas y sin espacios")
    
    # 5.5 Convertir fechas a formato estándar
    print("\n📅 Procesando fechas...")
    if "Date" in df_silver.columns:
        # Intentar convertir a formato fecha
        try:
            df_silver = df_silver.withColumn("Date", 
                to_date(col("Date"), "dd-MM-yyyy"))
            print(f"   • Date: convertido a tipo fecha (dd-MM-yyyy)")
        except:
            print(f"   ⚠️ Date: no se pudo convertir, se mantiene como string")
    
    # 5.6 Verificar rangos válidos en datos numéricos
    print("\n🔍 Validando rangos numéricos:")
    # Precios positivos
    if "Price" in df_silver.columns:
        precios_negativos = df_silver.filter(col("Price") < 0).count()
        if precios_negativos > 0:
            df_silver = df_silver.withColumn("Price", when(col("Price") < 0, 0).otherwise(col("Price")))
            print(f"   • Price: {precios_negativos} valores negativos corregidos a 0")
    
    # Descuentos entre 0 y 100
    if "Discount" in df_silver.columns:
        descuentos_invalidos = df_silver.filter((col("Discount") < 0) | (col("Discount") > 100)).count()
        if descuentos_invalidos > 0:
            df_silver = df_silver.withColumn("Discount", 
                when(col("Discount") < 0, 0)
                .when(col("Discount") > 100, 100)
                .otherwise(col("Discount")))
            print(f"   • Discount: {descuentos_invalidos} valores fuera de rango corregidos (0-100)")
    
    # Unidades vendidas positivas
    if "Units_Sold" in df_silver.columns:
        unidades_negativas = df_silver.filter(col("Units_Sold") < 0).count()
        if unidades_negativas > 0:
            df_silver = df_silver.withColumn("Units_Sold", when(col("Units_Sold") < 0, 0).otherwise(col("Units_Sold")))
            print(f"   • Units_Sold: {unidades_negativas} valores negativos corregidos a 0")
    
    # 6. Mostrar resultado de la limpieza
    num_filas_final = df_silver.count()
    print(f"\n📊 ESTADÍSTICAS DE LIMPIEZA:")
    print(f"   • Filas originales: {num_filas_original}")
    print(f"   • Filas después de limpieza: {num_filas_final}")
    print(f"   • Filas eliminadas: {num_filas_original - num_filas_final}")
    print(f"   • Columnas: {len(df_silver.columns)}")
    
    print("\n📋 Esquema después de limpieza:")
    df_silver.printSchema()
    
    print("\n👀 Primeras 5 filas (limpias):")
    df_silver.show(5, truncate=False)
    
    # 7. Guardar en Silver
    print(f"\n💾 Guardando datos en capa Silver...")
    print(f"   Ruta: {ruta_silver}")
    
    # Crear directorio si no existe
    os.makedirs(ruta_silver, exist_ok=True)
    
    try:
        df_silver.write \
            .mode("overwrite") \
            .format("parquet") \
            .option("compression", "snappy") \
            .save(f"file://{ruta_silver}")
        
        print(f"✅ Datos guardados exitosamente en Silver")
        
    except Exception as e:
        print(f"❌ Error al guardar en Silver: {str(e)}")
        sys.exit(1)
    
    # 8. Verificar archivos guardados
    print("\n🔍 Archivos guardados en Silver:")
    os.system(f"ls -la {ruta_silver}")
    
    # 9. Resumen final
    print("\n" + "="*60)
    print("✅ TRANSFORMACIÓN A SILVER COMPLETADA EXITOSAMENTE")
    print("="*60)
    print(f"\n📁 Datos originales: {ruta_bronze}")
    print(f"📁 Datos limpios: {ruta_silver}")
    print(f"📊 Total de registros limpios: {num_filas_final}")
    print(f"📋 Columnas disponibles: {len(df_silver.columns)}")
    print(f"✨ Porcentaje de datos preservado: {(num_filas_final/num_filas_original*100):.2f}%")
    
    # 10. Cerrar sesión
    spark.stop()
    print("\n👋 Sesión de Spark cerrada")

if __name__ == "__main__":
    main()