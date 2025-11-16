# src/etl/preprocess.py
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from .schemas import BASICS_SCHEMA, RATINGS_SCHEMA

#Definimos parametros de limpieza
CLIP_RUNTIME_LOW  = 30.0
CLIP_RUNTIME_HIGH = 240.0

def load_basics(spark: SparkSession, path: str) -> DataFrame:
    #Lee title.basics.tsv como DataFrame Spark.
    
    df = (spark.read
          .option("header", True)
          .option("sep", "\t")
          .option("nullValue", r"\N")
          .schema(BASICS_SCHEMA)
          .csv(path))
    return df

def load_ratings(spark: SparkSession, path: str) -> DataFrame:
    #Lee title.ratings.tsv como DataFrame Spark
   
    df = (spark.read
          .option("header", True)
          .option("sep", "\t")
          .option("nullValue", r"\N")
          .schema(RATINGS_SCHEMA)
          .csv(path))
    return df

def preprocess_basics(df: DataFrame) -> DataFrame:
    """
    Preprocesamos 'basics':
    - Selecciona columnas de interés
    - Castea tipos: isAdult a int, runtimeMinutes a double
    - Rellena isAdult nulo con 0
    - Clip de runtimeMinutes a [30, 240]
    - Rellena genres nulo/ vacío con 'Unknown'
    - Filtra solo películas (titleType == 'movie') no adultas (isAdult == 0)
    - Genera 'genres_array' (split por coma)
    - Elimina duplicados por tconst
    """
    df2 = (
        df.select("tconst", "titleType", "isAdult", "runtimeMinutes", "genres")
          .withColumn("isAdult",        F.col("isAdult").cast(T.IntegerType()))
          .withColumn("isAdult",        F.coalesce(F.col("isAdult"), F.lit(0)))
          .withColumn("runtimeMinutes", F.col("runtimeMinutes").cast(T.DoubleType()))
          .withColumn("runtimeMinutes",
                      F.when(F.col("runtimeMinutes").isNull(), F.lit(None).cast(T.DoubleType()))
                       .otherwise(
                           F.when(F.col("runtimeMinutes") < CLIP_RUNTIME_LOW,  F.lit(CLIP_RUNTIME_LOW))
                            .when(F.col("runtimeMinutes") > CLIP_RUNTIME_HIGH, F.lit(CLIP_RUNTIME_HIGH))
                            .otherwise(F.col("runtimeMinutes"))
                       ))
          .withColumn("genres",
                      F.when(F.col("genres").isNull() | (F.col("genres") == "") , F.lit("Unknown"))
                       .otherwise(F.col("genres")))
          .filter((F.col("titleType") == F.lit("movie")) & (F.col("isAdult") == F.lit(0)))
          .dropDuplicates(["tconst"])
    )

    df2 = df2.withColumn("genres_array", F.split(F.col("genres"), ","))
    return df2

def preprocess_ratings(df: DataFrame) -> DataFrame:
    """
    Preprocesa 'ratings':
    - Selecciona columnas de interés
    - Castea tipos: averageRating a double, numVotes a int
    - Elimina registros sin tconst
    - Elimina duplicados por tconst
    """
    df2 = (
        df.select("tconst", "averageRating", "numVotes")
          .withColumn("averageRating", F.col("averageRating").cast(T.DoubleType()))
          .withColumn("numVotes",      F.col("numVotes").cast(T.IntegerType()))
          .dropna(subset=["tconst"])
          .dropDuplicates(["tconst"])
    )
    return df2