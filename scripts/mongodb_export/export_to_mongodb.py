#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script: mongodb_export/export_to_mongodb.py
Propósito: Exportar datos de la capa Gold a MongoDB para Power BI
"""

from pyspark.sql import SparkSession
import pandas as pd
from pymongo import MongoClient
import os
import sys
import glob

def main():
    print("="*60)
    print("🚀 EXPORTANDO DATOS GOLD A MONGODB")
    print("="*60)
    
    # 1. Crear sesión de Spark
    print("\n📊 Creando sesión de Spark...")
    spark = SparkSession.builder \
        .appName("MongoDB_Export_Ecommerce") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # 2. Rutas de los datos Gold
    ruta_gold = "/home/hadoop/topicos-sales-prediction/data/gold/ecommerce"
    
    # 3. Verificar que existen los datos
    print(f"\n🔍 Verificando datos Gold: {ruta_gold}")
    if not os.path.exists(ruta_gold):
        print(f"❌ ERROR: No se encuentra la carpeta Gold")
        sys.exit(1)
    
    # 4. Conectar a MongoDB (local)
    print("\n🔌 Conectando a MongoDB...")
    try:
        client = MongoClient('localhost', 27017)
        db = client['ecommerce_db']
        print(f"✅ Conexión exitosa a MongoDB")
        print(f"   • Base de datos: ecommerce_db")
    except Exception as e:
        print(f"❌ Error conectando a MongoDB: {str(e)}")
        print("   Asegúrate que MongoDB esté corriendo: mongod --dbpath /data/db")
        sys.exit(1)
    
    # 5. Exportar cada KPI a una colección diferente
    
    # 5.1 Exportar ventas por categoría
    print("\n📦 Exportando ventas por categoría...")
    ruta_categoria = f"{ruta_gold}/ventas_categoria"
    if os.path.exists(ruta_categoria):
        df_categoria = spark.read.option("header", "true").csv(f"file://{ruta_categoria}")
        pd_categoria = df_categoria.toPandas()
        db['ventas_categoria'].delete_many({})
        db['ventas_categoria'].insert_many(pd_categoria.to_dict('records'))
        print(f"   • {len(pd_categoria)} registros guardados en colección 'ventas_categoria'")
    
    # 5.2 Exportar descuentos por segmento
    print("\n🎯 Exportando descuentos por segmento...")
    ruta_segmento = f"{ruta_gold}/descuento_segmento"
    if os.path.exists(ruta_segmento):
        df_segmento = spark.read.option("header", "true").csv(f"file://{ruta_segmento}")
        pd_segmento = df_segmento.toPandas()
        db['descuento_segmento'].delete_many({})
        db['descuento_segmento'].insert_many(pd_segmento.to_dict('records'))
        print(f"   • {len(pd_segmento)} registros guardados en colección 'descuento_segmento'")
    
    # 5.3 Exportar top categorías
    print("\n🏆 Exportando top categorías...")
    ruta_top = f"{ruta_gold}/top_categorias"
    if os.path.exists(ruta_top):
        df_top = spark.read.option("header", "true").csv(f"file://{ruta_top}")
        pd_top = df_top.toPandas()
        db['top_categorias'].delete_many({})
        db['top_categorias'].insert_many(pd_top.to_dict('records'))
        print(f"   • {len(pd_top)} registros guardados en colección 'top_categorias'")
    
      # 5.4 Exportar datos completos (para análisis detallado)
    print("\n📊 Exportando datos completos...")
    ruta_completa = f"{ruta_gold}/datos_completos"
    if os.path.exists(ruta_completa):
        df_completo = spark.read.parquet(f"file://{ruta_completa}")
        # Limitar a 1000 filas para no saturar MongoDB
        pd_completo = df_completo.limit(1000).toPandas()
        
        # CONVERTIR FECHAS A STRING (para que MongoDB las acepte)
        if 'Date' in pd_completo.columns:
            pd_completo['Date'] = pd_completo['Date'].astype(str)
        
        db['datos_completos'].delete_many({})
        db['datos_completos'].insert_many(pd_completo.to_dict('records'))
        print(f"   • {len(pd_completo)} registros guardados en colección 'datos_completos'")
    
    # 6. Listar colecciones creadas
    print("\n📋 Colecciones en MongoDB:")
    collections = db.list_collection_names()
    for collection in collections:
        count = db[collection].count_documents({})
        print(f"   • {collection}: {count} documentos")
    
    # 7. Resumen final
    print("\n" + "="*60)
    print("✅ EXPORTACIÓN A MONGODB COMPLETADA EXITOSAMENTE")
    print("="*60)
    print("\n📁 Datos exportados desde:", ruta_gold)
    print("🗄️  Base de datos MongoDB: ecommerce_db")
    
    spark.stop()
    print("\n👋 Sesión de Spark cerrada")

if __name__ == "__main__":
    main()