import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import date
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import io
import psycopg2
import numpy as np

# Función para filtrar los datos por fecha y advertisers activos
def filtrar_datos():
    print("Filtrando datos por fecha y advertisers activos...")

    # Crear el hook de S3
    hook = S3Hook(aws_conn_id='s3_connection')

    # Descargar archivos desde S3
    product_views_file = hook.read_key(key="product_views.csv", bucket_name="grupo-8-mlops")
    ads_views_file = hook.read_key(key="ads_views.csv", bucket_name="grupo-8-mlops")
    advertiser_ids_file = hook.read_key(key="advertiser_ids.csv", bucket_name="grupo-8-mlops")

    # Leer los archivos CSV en pandas desde el contenido en memoria
    product_views = pd.read_csv(io.StringIO(product_views_file))  
    ads_views = pd.read_csv(io.StringIO(ads_views_file)) 
    advertiser_ids = pd.read_csv(io.StringIO(advertiser_ids_file))

    # Convertir la columna 'date' a formato datetime para asegurar la comparación correcta
    product_views['date'] = pd.to_datetime(product_views['date'], errors='coerce')
    ads_views['date'] = pd.to_datetime(ads_views['date'], errors='coerce')

    # Filtrar por los advertisers activos
    filtered_product_views = product_views[product_views['advertiser_id'].isin(advertiser_ids['advertiser_id'])]
    filtered_ads_views = ads_views[ads_views['advertiser_id'].isin(advertiser_ids['advertiser_id'])]

    # Filtrar por la fecha de hoy
    current_date = pd.Timestamp(date.today())
    filtered_product_views = filtered_product_views[filtered_product_views['date'] == current_date]
    filtered_ads_views = filtered_ads_views[filtered_ads_views['date'] == current_date]

    # Guardar los datos filtrados en S3
    hook.load_string(string_data=filtered_product_views.to_csv(index=False), key="results/filtered_product_views.csv", bucket_name="grupo-8-mlops", replace=True)
    hook.load_string(string_data=filtered_ads_views.to_csv(index=False), key="results/filtered_ads_views.csv", bucket_name="grupo-8-mlops", replace=True)

    print("Datos filtrados guardados en S3.")

    

def calcular_top_ctr():
    print("Calculando TopCTR para cada advertiser...")

    # Crear el hook de S3
    hook = S3Hook(aws_conn_id='s3_connection')

    # Descargar el archivo filtrado desde S3
    filtered_ads_views_file = hook.read_key(key="results/filtered_ads_views.csv", bucket_name="grupo-8-mlops")
    filtered_ads_views = pd.read_csv(io.StringIO(filtered_ads_views_file))

    # Agrupar por advertiser_id y product_id para contar los clicks y las impresiones
    grouped = filtered_ads_views.groupby(['advertiser_id', 'product_id'])['type'].value_counts().unstack(fill_value=0)

    # Calcular el CTR por producto, manejando la división por cero
    grouped['ctr'] = np.where(
        grouped['impression'] == 0, 
        0, 
        grouped['click'] / grouped['impression']
    )

    # Resetear el índice
    grouped = grouped.reset_index()

    # Ordenar los resultados por advertiser_id y CTR (descendente)
    top_ctr = grouped.sort_values(['advertiser_id', 'ctr'], ascending=[True, False])

    # Filtrar advertisers activos únicos desde el archivo filtrado
    advertiser_ids_file = hook.read_key(key="advertiser_ids.csv", bucket_name="grupo-8-mlops")
    advertiser_ids = pd.read_csv(io.StringIO(advertiser_ids_file))

    # Crear una lista completa de advertiser_id para asegurarnos de que todos los activos estén presentes
    advertisers = advertiser_ids['advertiser_id'].unique()
    all_results = []

    for advertiser in advertisers:
        advertiser_data = top_ctr[(top_ctr['advertiser_id'] == advertiser) & (top_ctr['ctr'] > 0)]  # Filtrar CTR > 0
        if not advertiser_data.empty:
            # Traer hasta 20 productos si existen
            advertiser_top = advertiser_data.head(20)
        else:
            # Si no hay datos, incluir una entrada vacía
            advertiser_top = pd.DataFrame([{
                'advertiser_id': advertiser,
                'product_id': "-",
                'click': 0,
                'impression': 0,
                'ctr': 0
            }])
        all_results.append(advertiser_top)

    # Concatenar todos los resultados en un único DataFrame
    final_top_ctr = pd.concat(all_results, ignore_index=True)

    # Agregar la fecha de ejecución
    current_date = pd.Timestamp(date.today()).strftime('%Y-%m-%d')
    final_top_ctr['date'] = current_date  # Nueva columna con la fecha actual

    # Crear el nombre del archivo con la fecha actual
    file_name = f"results/top_ctr_results_{current_date}.csv"

    # Guardar los resultados en S3 con fecha incluida en el nombre
    hook.load_string(string_data=final_top_ctr.to_csv(index=False), key=file_name, bucket_name="grupo-8-mlops", replace=True)

    print(f"TopCTR calculado y guardado en S3 con el nombre: {file_name}")


# Función para calcular TopProduct
def calcular_top_product():
    print("Calculando TopProduct...")

    # Crear el hook de S3
    hook = S3Hook(aws_conn_id='s3_connection')

    # Descargar el archivo de vistas de productos filtrado desde S3
    filtered_product_views_file = hook.read_key(key="results/filtered_product_views.csv", bucket_name="grupo-8-mlops")
    filtered_product_views = pd.read_csv(io.StringIO(filtered_product_views_file))

    # Obtener la lista única de advertisers activos
    all_advertisers = filtered_product_views['advertiser_id'].unique()

    # Agrupar por advertiser_id y product_id para contar las vistas
    grouped = filtered_product_views.groupby(['advertiser_id', 'product_id']).size().reset_index(name='views')

    # Ordenar por advertiser_id y views en orden descendente
    grouped = grouped.sort_values(['advertiser_id', 'views'], ascending=[True, False])

    # Crear lista para almacenar resultados
    all_results = []

    # Procesar cada advertiser_id
    for advertiser in all_advertisers:
        advertiser_data = grouped[grouped['advertiser_id'] == advertiser]

        if not advertiser_data.empty:
            # Traer hasta 20 productos si existen
            advertiser_top = advertiser_data.head(20)
        else:
            # Si no hay datos, incluir una entrada vacía para ese advertiser_id
            advertiser_top = pd.DataFrame([{
                'advertiser_id': advertiser,
                'product_id': "-",
                'views': 0
            }])

        all_results.append(advertiser_top)

    # Combinar todos los resultados
    top_product = pd.concat(all_results).reset_index(drop=True)

    # Crear el nombre del archivo con la fecha actual
    current_date = pd.Timestamp(date.today()).strftime('%Y-%m-%d')
    top_product['date'] = current_date  # Nueva columna con la fecha actual
    
    file_name = f"results/top_product_results_{current_date}.csv"

    # Guardar el resultado de TopProduct en S3
    hook.load_string(string_data=top_product.to_csv(index=False), key=file_name, bucket_name="grupo-8-mlops", replace=True)

    print(f"TopProduct calculado y guardado en S3 con el nombre: {file_name}.")

    

def guardar_top_ctr_en_rds():
    print("Guardando TopCTR en RDS...")

    # Detalles de conexión
    host = "grupo-8-rds.cf4i6e6cwv74.us-east-1.rds.amazonaws.com"  # Cambia por tu host real
    database = "postgres"
    user = "grupo8rds"
    password = "grupo8rds"
    port = 5432

    # Crear conexión
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )
    cursor = conn.cursor()

    # Leer el archivo `TopCTR` desde S3
    hook = S3Hook(aws_conn_id='s3_connection')
    current_date = pd.Timestamp(date.today()).strftime('%Y-%m-%d')
    file_name = f"results/top_ctr_results_{current_date}.csv"
    top_ctr_file = hook.read_key(key=file_name, bucket_name="grupo-8-mlops")
    top_ctr = pd.read_csv(io.StringIO(top_ctr_file))

    # Crear la tabla si no existe
    create_table_query = """
    CREATE TABLE IF NOT EXISTS resultados_topctr (
        date DATE,
        advertiser_id VARCHAR(255),
        product_id VARCHAR(255),
        click INT,
        impression INT,
        ctr FLOAT,
        PRIMARY KEY (date, advertiser_id, product_id)
    );
    """
    cursor.execute(create_table_query)

    # Insertar o actualizar datos en la tabla
    for _, row in top_ctr.iterrows():
        insert_query = """
        INSERT INTO resultados_topctr (date, advertiser_id, product_id, click, impression, ctr)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, advertiser_id, product_id)
        DO UPDATE SET click = EXCLUDED.click, impression = EXCLUDED.impression, ctr = EXCLUDED.ctr;
        """
        cursor.execute(insert_query, (
            row['date'], row['advertiser_id'], row['product_id'], row['click'], row['impression'], row['ctr']
        ))

    # Confirmar cambios y cerrar la conexión
    conn.commit()
    cursor.close()
    conn.close()
    print("TopCTR guardado exitosamente en RDS.")


def guardar_top_product_en_rds():
    print("Guardando TopProduct en RDS...")

    # Detalles de conexión
    host = "grupo-8-rds.cf4i6e6cwv74.us-east-1.rds.amazonaws.com"  # Cambia por tu host real
    database = "postgres"
    user = "grupo8rds"
    password = "grupo8rds"
    port = 5432

    # Crear conexión
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )
    cursor = conn.cursor()

    # Leer el archivo `TopProduct` desde S3
    hook = S3Hook(aws_conn_id='s3_connection')
    current_date = pd.Timestamp(date.today()).strftime('%Y-%m-%d')
    file_name = f"results/top_product_results_{current_date}.csv"
    top_product_file = hook.read_key(key=file_name, bucket_name="grupo-8-mlops")
    top_product = pd.read_csv(io.StringIO(top_product_file))

    # Crear la tabla si no existe
    create_table_query = """
    CREATE TABLE IF NOT EXISTS resultados_topproduct (
        date DATE,
        advertiser_id VARCHAR(255),
        product_id VARCHAR(255),
        views INT,
        PRIMARY KEY (date, advertiser_id, product_id)
    );
    """
    cursor.execute(create_table_query)

    # Insertar o actualizar datos en la tabla
    for _, row in top_product.iterrows():
        insert_query = """
        INSERT INTO resultados_topproduct (date, advertiser_id, product_id, views)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date, advertiser_id, product_id)
        DO UPDATE SET views = EXCLUDED.views;
        """
        cursor.execute(insert_query, (
            row['date'], row['advertiser_id'], row['product_id'], row['views']
        ))

    # Confirmar cambios y cerrar la conexión
    conn.commit()
    cursor.close()
    conn.close()
    print("TopProduct guardado exitosamente en RDS.")


    

# Actualizar el DAG para incluir las tareas de guardar en RDS
with DAG(
    dag_id='tp_pipeline',
    schedule_interval='@daily',  # Se ejecuta diariamente
    start_date=datetime.datetime(2024, 11, 20),
    catchup=False,
) as dag:

    # Tareas existentes
    filtrar_datos_task = PythonOperator(
        task_id='filtrar_datos',
        python_callable=filtrar_datos,
    )

    calcular_top_ctr_task = PythonOperator(
        task_id='calcular_top_ctr',
        python_callable=calcular_top_ctr,
    )

    calcular_top_product_task = PythonOperator(
        task_id='calcular_top_product',
        python_callable=calcular_top_product,
    )

    guardar_top_ctr_task = PythonOperator(
        task_id='guardar_top_ctr_en_rds',
        python_callable=guardar_top_ctr_en_rds,
    )

    guardar_top_product_task = PythonOperator(
        task_id='guardar_top_product_en_rds',
        python_callable=guardar_top_product_en_rds,
    )

    # Dependencias del DAG
    filtrar_datos_task >> [calcular_top_ctr_task, calcular_top_product_task]
    calcular_top_ctr_task >> guardar_top_ctr_task
    calcular_top_product_task >> guardar_top_product_task





