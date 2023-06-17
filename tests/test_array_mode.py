from pyspark.sql import SparkSession

from pyspark_functions import array_mode


def test_array_mode():
    spark = SparkSession.builder.getOrCreate()

    data = [
        dict(values=["a", "b", "b", "c", "c", "c"]),
        dict(values=["a", "a", "a", "b", "b", "c"]),
        dict(values=["a"]),
    ]

    df = spark.createDataFrame(data)
    df = df.select(array_mode(df.values))

    assert df.toPandas().to_dict(orient="records") == [
        {"array_mode(values)": "c"},
        {"array_mode(values)": "a"},
        {"array_mode(values)": "a"},
    ]
