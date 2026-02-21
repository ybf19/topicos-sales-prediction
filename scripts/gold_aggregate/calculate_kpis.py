#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script: gold_aggregate/calculate_kpis.py
Propósito: Calcular KPIs y métricas de negocio desde capa Silver (capa Gold)
Autor: Proyecto Big Data
Fecha: 2026-02-21
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, desc, round, corr
import os
import sys

def main():
    print("="*60)
    print("🚀 INICIANDO CÁLCULO DE KPIS - CAPA GOLD")
    print("="*60)
    
    # 1. Crear sesión de Spark
    print("\n📊 Creando sesión de Spark...")
    spark = SparkSession.builder \
        .appName("Gold_KPIs_Ecommerce") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    
    # 2. Rutas
    ruta_silver = "/home/hadoop/topicos-sales-prediction/data/silver/ecommerce"
    ruta_gold = "/home/hadoop/topicos-sales-prediction/data/gold/ecommerce"
    
    # 3. Verificar Silver
    print(f"\n🔍 Verificando datos en Silver: {ruta_silver}")
    if not os.path.exists(ruta_silver):
        print(f"❌ ERROR: No se encuentra la carpeta Silver")
        print(f"   Ejecuta primero silver_transform/clean_data.py")
        sys.exit(1)
    print(f"✅ Datos Silver encontrados")
    
    # 4. Leer datos limpios
    print(f"\n📂 Leyendo datos desde Silver...")
    df = spark.read.parquet(f"file://{ruta_silver}")
    print(f"✅ Datos cargados: {df.count()} filas")
    
    print("\n📋 Esquema de datos:")
    df.printSchema()
    
    # 5. CALCULAR KPIS
    
    print("\n" + "="*60)
    print("📊 CALCULANDO KPIS DE NEGOCIO")
    print("="*60)
    
    # 5.1 KPI 1: Ventas totales (ingresos = Price * Units_Sold)
    print("\n💰 KPI 1: Ventas Totales")
    df_ventas = df.withColumn("Total_Sales", col("Price") * col("Units_Sold"))
    ventas_totales = df_ventas.agg(sum("Total_Sales")).collect()[0][0]
    print(f"   • Ventas totales: ${ventas_totales:,.2f}")
    
    # 5.2 KPI 2: Ventas por categoría de producto
    print("\n📦 KPI 2: Ventas por Categoría de Producto")
    ventas_categoria = df_ventas.groupBy("Product_Category") \
        .agg(
            round(sum("Total_Sales"), 2).alias("Total_Ventas"),
            round(avg("Price"), 2).alias("Precio_Promedio"),
            sum("Units_Sold").alias("Unidades_Vendidas"),
            count("*").alias("Num_Transacciones")
        ) \
        .orderBy(desc("Total_Ventas"))
    
    ventas_categoria.show(truncate=False)
    
    # 5.3 KPI 3: Descuento promedio por segmento de cliente
    print("\n🎯 KPI 3: Descuento Promedio por Segmento de Cliente")
    descuento_segmento = df.groupBy("Customer_Segment") \
        .agg(
            round(avg("Discount"), 2).alias("Descuento_Promedio_%"),
            round(avg("Price"), 2).alias("Precio_Promedio"),
            count("*").alias("Num_Compras")
        ) \
        .orderBy(desc("Descuento_Promedio_%"))
    
    descuento_segmento.show(truncate=False)
    
    # 5.4 KPI 4: Top 5 categorías por unidades vendidas
    print("\n🏆 KPI 4: Top 5 Categorías más Vendidas (por unidades)")
    top_categorias = df.groupBy("Product_Category") \
        .agg(sum("Units_Sold").alias("Unidades_Vendidas")) \
        .orderBy(desc("Unidades_Vendidas")) \
        .limit(5)
    
    top_categorias.show(truncate=False)
    
    # 5.5 KPI 5: Correlación entre Marketing Spend y Units Sold
    print("\n📈 KPI 5: Correlación Marketing Spend vs Unidades Vendidas")
    correlacion = df.select(corr("Marketing_Spend", "Units_Sold")).collect()[0][0]
    print(f"   • Coeficiente de correlación: {correlacion:.4f}")
    if correlacion > 0.5:
        print(f"   • Interpretación: Fuerte correlación positiva")
    elif correlacion > 0.3:
        print(f"   • Interpretación: Correlación positiva moderada")
    elif correlacion > 0:
        print(f"   • Interpretación: Correlación positiva débil")
    elif correlacion < 0:
        print(f"   • Interpretación: Correlación negativa")
    else:
        print(f"   • Interpretación: Sin correlación")
    
    # 5.6 KPI 6: Resumen general
    print("\n📊 KPI 6: Resumen General del Dataset")
    resumen = df.agg(
        count("*").alias("Total_Transacciones"),
        round(avg("Price"), 2).alias("Precio_Promedio"),
        round(avg("Discount"), 2).alias("Descuento_Promedio_%"),
        round(avg("Marketing_Spend"), 2).alias("Marketing_Spend_Promedio"),
        sum("Units_Sold").alias("Total_Unidades_Vendidas")
    ).collect()[0]
    
    print(f"   • Total transacciones: {resumen['Total_Transacciones']}")
    print(f"   • Precio promedio: ${resumen['Precio_Promedio']}")
    print(f"   • Descuento promedio: {resumen['Descuento_Promedio_%']}%")
    print(f"   • Marketing spend promedio: ${resumen['Marketing_Spend_Promedio']:,.2f}")
    print(f"   • Total unidades vendidas: {resumen['Total_Unidades_Vendidas']}")
    
    # 6. Guardar resultados en Gold (para Power BI / MongoDB)
    print(f"\n💾 Guardando KPIs en capa Gold...")
    print(f"   Ruta: {ruta_gold}")
    
    # Crear directorio
    os.makedirs(ruta_gold, exist_ok=True)
    
    # Guardar cada KPI como archivo separado (opcional)
    ventas_categoria.write.mode("overwrite").option("header", "true").csv(f"file://{ruta_gold}/ventas_categoria")
    descuento_segmento.write.mode("overwrite").option("header", "true").csv(f"file://{ruta_gold}/descuento_segmento")
    top_categorias.write.mode("overwrite").option("header", "true").csv(f"file://{ruta_gold}/top_categorias")
    
    # Guardar también el dataset completo con columna de ventas
    df_ventas.write.mode("overwrite").parquet(f"file://{ruta_gold}/datos_completos")
    
    print(f"✅ KPIs guardados exitosamente en Gold")
    
    # 7. Verificar archivos
    print("\n🔍 Archivos guardados en Gold:")
    os.system(f"ls -la {ruta_gold}")
    
    # 8. Resumen final
    print("\n" + "="*60)
    print("✅ CÁLCULO DE KPIS COMPLETADO EXITOSAMENTE")
    print("="*60)
    print(f"\n📁 Datos origen: {ruta_silver}")
    print(f"📁 KPIs guardados: {ruta_gold}")
    print("\n📊 KPIs disponibles para Power BI:")
    print("   • ventas_categoria/ - Ventas por categoría")
    print("   • descuento_segmento/ - Descuentos por segmento")
    print("   • top_categorias/ - Top 5 categorías")
    print("   • datos_completos/ - Dataset completo con ventas calculadas")
    
    spark.stop()
    print("\n👋 Sesión de Spark cerrada")

if __name__ == "__main__":
    main()