# src/etl/schemas.py
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# Esquema laxo para lectura inicial (algunas columnas llegan como string y se castean después)
BASICS_SCHEMA_LOOSE = StructType([
    StructField("tconst",         StringType(),  True),
    StructField("titleType",      StringType(),  True),
    StructField("primaryTitle",   StringType(),  True),  # no se usa, pero puede estar
    StructField("originalTitle",  StringType(),  True),  # no se usa
    StructField("isAdult",        StringType(),  True),  # castearemos a int
    StructField("startYear",      StringType(),  True),  # no se usa aquí
    StructField("endYear",        StringType(),  True),  # no se usa
    StructField("runtimeMinutes", StringType(),  True),  # castearemos a double
    StructField("genres",         StringType(),  True),
])

RATINGS_SCHEMA_LOOSE = StructType([
    StructField("tconst",        StringType(), True),
    StructField("averageRating", StringType(), True),  # castearemos a double
    StructField("numVotes",      StringType(), True),  # castearemos a int
])