-- =====================================================
-- SCRIPT: analytical_queries.hql
-- PROPOSITO: Consultas analíticas para responder preguntas de negocio
-- AUTOR: Proyecto Big Data
-- FECHA: 2026-02-22
-- =====================================================

USE ecommerce_db;

-- =====================================================
-- PREGUNTA 1: ¿Cuáles son las categorías con mayores ventas?
-- =====================================================
SELECT 'PREGUNTA 1: Top 3 categorías por ventas totales' AS consulta;

SELECT 
    categoria_producto,
    total_ventas,
    unidades_vendidas,
    num_transacciones
FROM ventas_categoria_gold
ORDER BY total_ventas DESC
LIMIT 3;

-- =====================================================
-- PREGUNTA 2: ¿Qué segmento de clientes recibe mayores descuentos?
-- =====================================================
SELECT 'PREGUNTA 2: Descuento promedio por segmento' AS consulta;

SELECT 
    segmento_cliente,
    descuento_promedio,
    precio_promedio,
    num_compras
FROM descuentos_segmento_gold
ORDER BY descuento_promedio DESC;

-- =====================================================
-- PREGUNTA 3: ¿Cuál es la categoría más vendida (por unidades)?
-- =====================================================
SELECT 'PREGUNTA 3: Categoría más vendida' AS consulta;

SELECT 
    categoria_producto,
    unidades_vendidas
FROM top_categorias_gold
ORDER BY unidades_vendidas DESC
LIMIT 1;

-- =====================================================
-- PREGUNTA 4: ¿Cuál es el precio promedio por categoría?
-- =====================================================
SELECT 'PREGUNTA 4: Precio promedio por categoría' AS consulta;

SELECT 
    categoria_producto,
    precio_promedio,
    unidades_vendidas
FROM ventas_categoria_gold
ORDER BY precio_promedio DESC;

-- =====================================================
-- PREGUNTA 5: ¿Qué relación hay entre descuento y unidades vendidas?
-- =====================================================
SELECT 'PREGUNTA 5: Correlación descuento vs unidades (desde datos completos)' AS consulta;

SELECT 
    descuento,
    AVG(unidades_vendidas) as unidades_promedio
FROM ventas_silver
GROUP BY descuento
ORDER BY descuento;

-- =====================================================
-- PREGUNTA 6: Resumen general del negocio
-- =====================================================
SELECT 'PREGUNTA 6: Resumen general de ventas' AS consulta;

SELECT 
    COUNT(*) as total_transacciones,
    SUM(unidades_vendidas) as total_unidades,
    AVG(precio) as precio_promedio_general,
    AVG(descuento) as descuento_promedio_general
FROM ventas_silver;

-- =====================================================
-- PREGUNTA 7: Estacionalidad de ventas (por mes)
-- =====================================================
SELECT 'PREGUNTA 7: Ventas por mes' AS consulta;

SELECT 
    SUBSTR(fecha, 4, 2) as mes,
    COUNT(*) as num_ventas,
    SUM(unidades_vendidas) as unidades_totales
FROM ventas_silver
GROUP BY SUBSTR(fecha, 4, 2)
ORDER BY mes;