from fastapi import FastAPI
import os, psycopg2, csv, io
from typing import Optional
from fastapi.responses import StreamingResponse, JSONResponse

app = FastAPI(title="Feature Delivery API")

PG_HOST = os.getenv("POSTGRES_HOST","postgres")
PG_DB   = os.getenv("POSTGRES_DB","warehouse")
PG_USER = os.getenv("POSTGRES_USER","postgres")
PG_PW   = os.getenv("POSTGRES_PASSWORD","pgpass")

def get_conn():
    return psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PW)

@app.get("/features")
def features(year: int, quarter: int, zone: Optional[str] = None, format: str = "json"):
    conn = get_conn()
    cur = conn.cursor()
    if zone:
        cur.execute("""SELECT year, quarter, zone, trips, revenue, avg_trip_distance
                        FROM features.zone_quarter_agg
                        WHERE year=%s AND quarter=%s AND zone=%s
                        ORDER BY zone""", (year, quarter, zone))
    else:
        cur.execute("""SELECT year, quarter, zone, trips, revenue, avg_trip_distance
                        FROM features.zone_quarter_agg
                        WHERE year=%s AND quarter=%s
                        ORDER BY zone""", (year, quarter))
    rows = cur.fetchall()
    cur.close(); conn.close()

    if format == "csv":
        stream = io.StringIO()
        writer = csv.writer(stream)
        writer.writerow(["year","quarter","zone","trips","revenue","avg_trip_distance"])
        for r in rows:
            writer.writerow(r)
        stream.seek(0)
        return StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    else:
        data = [{
            "year": r[0], "quarter": r[1], "zone": r[2],
            "trips": int(r[3]), "revenue": float(r[4]), "avg_trip_distance": float(r[5])
        } for r in rows]
        return JSONResponse(data)