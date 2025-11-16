from pyspark.sql import Row
from src.etl.preprocess import preprocess_ratings

def test_preprocess_ratings_types_and_dupes(spark):
    data = [
        Row(tconst="tt0000001", averageRating="7.5", numVotes="1500"),
        Row(tconst="tt0000001", averageRating="7.5", numVotes="1500"),  # duplicado
        Row(tconst="tt0000002", averageRating=None,  numVotes="10"),
        Row(tconst=None,        averageRating="6.0", numVotes="5"),     # sin tconst -> drop
    ]
    df = spark.createDataFrame(data)
    out = preprocess_ratings(df)

    #Evaluamos eliminacion de duplicados y filas sin tconst
    ids = [r["tconst"] for r in out.select("tconst").collect()]
    assert set(ids) == {"tt0000001", "tt0000002"}

    #Chequeamos los tipos de datos casteados
    row = {r["tconst"]: r for r in out.select("tconst", "averageRating", "numVotes").collect()}
    assert isinstance(row["tt0000001"]["averageRating"], float)
    assert isinstance(row["tt0000001"]["numVotes"], int)