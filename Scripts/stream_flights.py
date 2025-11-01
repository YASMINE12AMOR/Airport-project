# stream_airports_to_pg.py
# Spark 3.5.x — nécessite les jars: postgresql-42.x.y.jar et spark-sql-kafka-0-10_2.12-3.5.x.jar

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, size, expr, current_timestamp, coalesce, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DoubleType, LongType,
    ArrayType, IntegerType
)

# =========================================================
# 1) SparkSession
# =========================================================
spark = (SparkSession.builder
    .appName("AirportsStream_PG")
    # .config("spark.jars", "/path/postgresql-42.6.0.jar,/path/spark-sql-kafka-0-10_2.12-3.5.1.jar")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =========================================================
# 2) Schémas
# =========================================================
runway_schema = StructType([
    StructField("designator", StringType()),
    StructField("trueHeading", DoubleType()),
    StructField("mainRunway", BooleanType()),
    StructField("surface", StructType([
        StructField("composition", ArrayType(IntegerType())),
        StructField("mainComposite", IntegerType()),
        StructField("condition", IntegerType()),
    ])),
    StructField("dimension", StructType([
        StructField("length", StructType([
            StructField("value", DoubleType()),
            StructField("unit", IntegerType()),
        ])),
        StructField("width", StructType([
            StructField("value", DoubleType()),
            StructField("unit", IntegerType()),
        ])),
    ])),
])

airport_schema = StructType([
    StructField("_id", StringType()),
    StructField("name", StringType()),
    StructField("icaoCode", StringType()),
    StructField("iataCode", StringType()),
    StructField("type", IntegerType()),
    StructField("country", StringType()),
    StructField("geometry", StructType([
        StructField("type", StringType()),
        StructField("coordinates", ArrayType(DoubleType())),
    ])),
    StructField("elevation", StructType([
        StructField("value", DoubleType()),
        StructField("unit", IntegerType()),
        StructField("referenceDatum", IntegerType()),
    ])),
    StructField("runways", ArrayType(runway_schema)),
    StructField("createdAt", StringType()),
    StructField("updatedAt", StringType()),
])

wrapped_schema = StructType([
    StructField("items", ArrayType(airport_schema)),
])

# =========================================================
# 3) Source Kafka
# =========================================================
raw_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "flights_positions")
    .option("startingOffsets", "latest")
    .load()
)

json_df = raw_df.select(col("value").cast("string").alias("json_str"))

parsed_wrapped = json_df.select(from_json(col("json_str"), wrapped_schema).alias("data_wrapped"))
parsed_direct  = json_df.select(from_json(col("json_str"), ArrayType(airport_schema)).alias("data_array"))

airports_wrapped = parsed_wrapped.selectExpr("explode(data_wrapped.items) as airport")
airports_direct  = parsed_direct.selectExpr("explode(data_array) as airport")

airports_union = airports_wrapped.unionByName(airports_direct, allowMissingColumns=True)

# =========================================================
# 4) Aplatissement & normalisation
# =========================================================
airports_flat_raw = airports_union.select(
    col("airport._id").alias("airport_id"),
    col("airport.name").alias("name"),
    col("airport.icaoCode").alias("icao"),
    col("airport.iataCode").alias("iata"),
    col("airport.country").alias("country"),
    col("airport.geometry.coordinates").getItem(1).alias("lat"),
    col("airport.geometry.coordinates").getItem(0).alias("lon"),
    col("airport.elevation.value").alias("elevation_m"),
    size(col("airport.runways")).alias("runway_count"),
    expr("""
        aggregate(
            airport.runways,
            cast(0.0 as double),
            (acc, r) -> CASE
                WHEN r.dimension.length.value IS NULL THEN acc
                WHEN r.dimension.length.value > acc THEN r.dimension.length.value
                ELSE acc
            END
        )
    """).alias("max_runway_length_m")
)

# Nettoyage minimum pour respecter la table (pas d'ID null, runway_count >= 0, types ok)
airports_flat = (airports_flat_raw
    .filter(col("airport_id").isNotNull())
    .withColumn("runway_count", coalesce(col("runway_count"), lit(0)).cast("int"))
    .withColumn("max_runway_length_m", col("max_runway_length_m").cast("double"))
    .withColumn("elevation_m", col("elevation_m").cast("double"))
)

# =========================================================
# 5) Config JDBC
# =========================================================
JDBC_URL   = "jdbc:postgresql://postgres:5432/mydb"
JDBC_USER  = "admin"
JDBC_PSW   = "admin"
JDBC_DRV   = "org.postgresql.Driver"
JDBC_TABLE = "airports_clean"

jdbc_opts = {
    "url": JDBC_URL,
    "user": JDBC_USER,
    "password": JDBC_PSW,
    "driver": JDBC_DRV,
    "batchsize": "1000",
    "isolationLevel": "READ_COMMITTED",
    # "truncate": "false",
}

# =========================================================
# 6) foreachBatch -> append Postgres (avec dédup intra-batch)
#    (si tu veux un UPSERT, vois la variante en bas)
# =========================================================
def write_to_postgres(df, batch_id: int):
    out = (df
        .dropDuplicates(["airport_id"])
        .withColumn("ingested_at", current_timestamp())  # OK si la colonne existe dans PG (ou ignorée sinon)
    )

    n = out.count()
    if n == 0:
        print(f"[PG] batch_id={batch_id} -> nothing to write")
        return

    print(f"[PG] batch_id={batch_id} -> writing {n} rows to {JDBC_TABLE}")

    (out.write
        .format("jdbc")
        .options(**jdbc_opts)
        .option("dbtable", JDBC_TABLE)
        .mode("append")
        .save())

# =========================================================
# 7) Démarrage des sinks
# =========================================================
# Console (debug)
console_q = (airports_flat.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start())

# Postgres
pg_q = (airports_flat.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/tmp/chk_airports_pg")
    .trigger(processingTime="10 seconds")
    .start())

spark.streams.awaitAnyTermination()

# =========================================================
# --- Variante UPSERT (ON CONFLICT) ---
# Décommente et utilise à la place de write_to_postgres si tu veux dédupliquer entre batches.
# Nécessite que airports_clean ait une contrainte UNIQUE/PK sur (airport_id).
#
# from pyspark.sql import DataFrame
# import psycopg2
#
# def upsert_to_postgres(df: DataFrame, batch_id: int):
#     # Écrit dans une table staging (créée une fois) puis fusionne
#     staging = f"{JDBC_TABLE}_stg"
#     (df.write
#         .format("jdbc")
#         .options(**jdbc_opts)
#         .option("dbtable", staging)
#         .mode("overwrite")   # réécrit la staging à chaque batch
#         .save())
#
#     sql = f"""
#     INSERT INTO {JDBC_TABLE} AS t
#       (airport_id, name, icao, iata, country, lat, lon, elevation_m, runway_count, max_runway_length_m, ingested_at)
#     SELECT airport_id, name, icao, iata, country, lat, lon, elevation_m, runway_count, max_runway_length_m, now()
#     FROM {staging}
#     ON CONFLICT (airport_id) DO UPDATE SET
#       name = EXCLUDED.name,
#       icao = EXCLUDED.icao,
#       iata = EXCLUDED.iata,
#       country = EXCLUDED.country,
#       lat = EXCLUDED.lat,
#       lon = EXCLUDED.lon,
#       elevation_m = EXCLUDED.elevation_m,
#       runway_count = EXCLUDED.runway_count,
#       max_runway_length_m = EXCLUDED.max_runway_length_m,
#       ingested_at = now();
#     """
#     import jaydebeapi  # ou psycopg2 si accessible côté driver
#     # Ici, ouvre une connexion et exécute le SQL (selon lib dispo dans ton runtime).
