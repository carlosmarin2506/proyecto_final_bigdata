import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
    
from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("imdb-preprocess-tests")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    yield spark
    spark.stop()