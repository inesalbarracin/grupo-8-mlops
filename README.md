# Grupo 8 - MLOps  
**Repositorio Grupo 8 - Trabajo Final MLOps**

**Integrantes:**  
- Inés Albarracín  
- Nicolás Cardonatti  
- Nicolás Granato  
- Martín Gvozdenovich

Este repositorio contiene el código del trabajo final desarrollado en el marco de la materia **MLOps**. El objetivo es generar recomendaciones para advertisers en base a distintas métricas (TOP_CTR y TOP_PRODUCT), desplegar un pipeline en Airflow para procesar datos y guardar los resultados en una base de datos RDS, así como exponer la información a través de una API creada con FastAPI.

---

## Scripts

### `tp_pipeline.py`
Este archivo contiene las funciones para el armado del pipeline de Airflow:

- **filtrar_datos**:  
  Lee los CSV en S3 y filtra según advertisers activos y fecha actual. Genera dos CSV filtrados (`filtered_adsviews` y `filtered_productviews`) en S3.

- **calcular_top_ctr**:  
  Toma `filtered_adsviews` y calcula el CTR por advertiser. Selecciona las 20 ads con mayor CTR y guarda `results/top_ctr_results_{current_date}.csv` en S3.

- **calcular_top_product**:  
  Similar a la función anterior, pero calcula los productos más vistos (`top_product_results_{current_date}.csv` en S3).

- **guardar_top_ctr_en_rds**:  
  Exporta los datos de `top_ctr_results_{current_date}.csv` a la tabla `resultados_topctr` en RDS.

- **guardar_top_product_en_rds**:  
  Exporta los datos de `top_product_results_{current_date}.csv` a la tabla `resultados_topproduct` en RDS.

Por último, arma el pipeline y actualiza el DAG diariamente.

### `main.py`
Este archivo define una **API en FastAPI** que se conecta a la base de datos PostgreSQL (RDS) para obtener y procesar datos sobre recomendaciones.

#### Endpoints:

- **GET /**  
  Devuelve un mensaje de bienvenida sencillo, para verificar que la API está activa.

- **GET /recommendations/{adv}/{model}**  
  Retorna las recomendaciones de productos para un `advertiser_id` específico, utilizando `TOP_CTR` o `TOP_PRODUCT` para el día actual.

- **GET /stats/**  
  - Cantidad total de advertisers únicos.  
  - Top 5 advertisers con más variación entre días consecutivos.  
  - Coincidencias entre modelos (productos recomendados por ambos).

- **GET /history/{adv}/**  
  Historial de los últimos 7 días de TOP_CTR y TOP_PRODUCT para un `advertiser_id`. Incluye clicks, impressions, ctr, views y fechas.

---

## Requerimientos

- Python
- Airflow instalado y configurado correctamente (se utilizó un entorno virtual con las librerías y dependencias)
- FastAPI y dependencias (ver `requirements.txt`)
- Acceso a S3 para leer/escribir CSV
- Acceso a una instancia RDS de PostgreSQL
- Credenciales de AWS configuradas (para S3) y conexión AWS en Airflow

---


