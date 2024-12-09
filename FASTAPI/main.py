from fastapi import FastAPI, HTTPException
from psycopg2 import connect
import psycopg2.extras
from datetime import date, timedelta

app = FastAPI()

# Endpoint raíz
@app.get("/")
def read_root():
    return {"message": "FastAPI - Grupo 8"}

# Configuración de la conexión a la base de datos
def get_connection():
    return connect(
        host="grupo-8-rds.cf4i6e6cwv74.us-east-1.rds.amazonaws.com",
        database="postgres",
        user="grupo8rds",
        password="grupo8rds",
        port=5432
    )

# Endpoint para obtener recomendaciones
@app.get("/recommendations/{adv}/{model}")
async def get_recommendations(adv: str, model: str):
    valid_models = ["TOP_CTR", "TOP_PRODUCT"]
    if model not in valid_models:
        raise HTTPException(status_code=400, detail="Invalid model. Use TOP_CTR or TOP_PRODUCT.")

    # Determinar la tabla según el modelo
    table = "resultados_topctr" if model == "TOP_CTR" else "resultados_topproduct"

    # Obtener la fecha actual
    current_date = date.today().strftime('%Y-%m-%d')

    try:
        # Conexión a la base de datos
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                query = f"""
                    SELECT product_id 
                    FROM {table} 
                    WHERE advertiser_id = %s AND date = %s
                """
                cursor.execute(query, (adv, current_date))
                results = cursor.fetchall()

        # Convertir resultados en JSON
        recommendations = [row['product_id'] for row in results]
        return {
            "advertiser_id": adv,
            "model": model,
            "date": current_date,
            "recommendations": recommendations
        }

    except psycopg2.OperationalError as oe:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(oe)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
        


# Endpoint para estadísticas
@app.get("/stats/")
async def get_stats():
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                # Cantidad de advertisers únicos
                cursor.execute("""
                    SELECT COUNT(DISTINCT advertiser_id) AS total_advertisers
                    FROM (
                        SELECT advertiser_id FROM resultados_topctr
                        UNION
                        SELECT advertiser_id FROM resultados_topproduct
                    ) AS advertisers;
                """)
                total_advertisers = cursor.fetchone()['total_advertisers']

                # Advertisers que más varían sus recomendaciones por día
                cursor.execute("""
                    WITH daily_recommendations AS (
                        SELECT advertiser_id, date, ARRAY_AGG(product_id ORDER BY product_id) AS products
                        FROM resultados_topctr
                        GROUP BY advertiser_id, date
                    )
                    SELECT advertiser_id, COUNT(*) AS changes
                    FROM (
                        SELECT t1.advertiser_id, t1.date, t1.products, t2.products AS next_day_products
                        FROM daily_recommendations t1
                        LEFT JOIN daily_recommendations t2
                        ON t1.advertiser_id = t2.advertiser_id AND t1.date = t2.date - INTERVAL '1 day'
                    ) AS comparison
                    WHERE products IS DISTINCT FROM next_day_products
                    GROUP BY advertiser_id
                    ORDER BY changes DESC
                    LIMIT 5;
                """)
                top_changers = cursor.fetchall()

                # Coincidencias entre modelos
                cursor.execute("""
                    SELECT COUNT(*) AS matches
                    FROM resultados_topctr ctr
                    JOIN resultados_topproduct product
                    ON ctr.advertiser_id = product.advertiser_id
                    AND ctr.product_id = product.product_id
                    AND ctr.date = product.date;
                """)
                matches = cursor.fetchone()['matches']

        return {
            "total_advertisers": total_advertisers,
            "top_changers": [{"advertiser_id": row['advertiser_id'], "changes": row['changes']} for row in top_changers],
            "model_matches": matches
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching stats: {str(e)}")

# Endpoint para el historial de un advertiser
@app.get("/history/{adv}/")
async def get_history(adv: str):
    try:
        seven_days_ago = date.today() - timedelta(days=7)

        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                # Obtener historial de TOP_CTR
                cursor.execute("""
                    SELECT date, product_id, click, impression, ctr
                    FROM resultados_topctr
                    WHERE advertiser_id = %s AND date >= %s
                    ORDER BY date DESC;
                """, (adv, seven_days_ago))
                top_ctr_history = cursor.fetchall()

                # Obtener historial de TOP_PRODUCT
                cursor.execute("""
                    SELECT date, product_id, views
                    FROM resultados_topproduct
                    WHERE advertiser_id = %s AND date >= %s
                    ORDER BY date DESC;
                """, (adv, seven_days_ago))
                top_product_history = cursor.fetchall()

        return {
            "advertiser_id": adv,
            "history": {
                "TOP_CTR": [{"date": row['date'], "product_id": row['product_id'], "click": row['click'], "impression": row['impression'], "ctr": row['ctr']} for row in top_ctr_history],
                "TOP_PRODUCT": [{"date": row['date'], "product_id": row['product_id'], "views": row['views']} for row in top_product_history]
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching history for advertiser {adv}: {str(e)}")

