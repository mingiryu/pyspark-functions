from pyspark.sql import SparkSession

from pyspark_functions import collect_set


def test_collect_set():
    spark = SparkSession.builder.getOrCreate()

    data = [
        dict(key=1, value=1),
        dict(key=2, value=2),
        dict(key=3, value=2),
        dict(key=3, value=3),
        dict(key=3, value=3),
        dict(key=3, value=3),
    ]

    df = spark.createDataFrame(data)
    df = df.groupBy(df.key).agg(collect_set(df.value))

    assert df.toPandas().to_dict(orient="records") == [
        {"key": 1, "collect_set(value)": [1]},
        {"key": 2, "collect_set(value)": [2]},
        {"key": 3, "collect_set(value)": [2, 3]},
    ]
