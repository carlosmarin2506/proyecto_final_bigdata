from pyspark.sql import Row, functions as F
from src.etl.preprocess import preprocess_basics

def test_preprocess_basics_filters_and_types(spark):
    #Datos de ejemplo (incluye no-movie y adultos para verificar filtrado)
    data = [
        Row(tconst="tt0000001", titleType="movie",     isAdult="0", runtimeMinutes="25",  genres="Action,Comedy"),
        Row(tconst="tt0000002", titleType="movie",     isAdult=None, runtimeMinutes="300", genres=None),
        Row(tconst="tt0000003", titleType="short",     isAdult="0", runtimeMinutes="50",  genres="Drama"),
        Row(tconst="tt0000004", titleType="movie",     isAdult="1", runtimeMinutes="100", genres="Comedy"),
        Row(tconst="tt0000005", titleType="movie",     isAdult="0", runtimeMinutes=None,  genres=""),
    ]
    df = spark.createDataFrame(data)

    out = preprocess_basics(df)

    #Deben quedar solo películas y no adultas: tt0000001, tt0000002, tt0000005
    ids = [r["tconst"] for r in out.select("tconst").collect()]
    assert set(ids) == {"tt0000001", "tt0000002", "tt0000005"}

    #Tipos
    row = {r["tconst"]: r for r in out.select("tconst", "isAdult", "runtimeMinutes", "genres").collect()}
    # tt0000001: runtime 25 = clip a 30.0
    assert row["tt0000001"]["runtimeMinutes"] == 30.0
    # tt0000002: runtime 300 = clip a 240.0; isAdult None = 0
    assert row["tt0000002"]["runtimeMinutes"] == 240.0
    assert row["tt0000002"]["isAdult"] == 0
    # tt0000005: runtime None = None; genres "" = "Unknown"
    assert row["tt0000005"]["runtimeMinutes"] is None
    assert row["tt0000005"]["genres"] == "Unknown"

def test_preprocess_basics_genres_array(spark):
    data = [
        Row(tconst="tt0000100", titleType="movie", isAdult="0", runtimeMinutes="100", genres="Action,Drama"),
        Row(tconst="tt0000101", titleType="movie", isAdult="0", runtimeMinutes="110", genres=None),
    ]
    df = spark.createDataFrame(data)
    out = preprocess_basics(df)

    recs = {r["tconst"]: r for r in out.select("tconst", "genres", "genres_array").collect()}
    assert recs["tt0000100"]["genres_array"] == ["Action", "Drama"]
    assert recs["tt0000101"]["genres"] == "Unknown"
    assert recs["tt0000101"]["genres_array"] == ["Unknown"]