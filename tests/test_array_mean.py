from pyspark.sql import SparkSession

from pyspark_functions import array_mean


def test_array_mean():
    spark = SparkSession.builder.getOrCreate()

    data = [dict(values=[1, 2, 2, 3, 3, 3]), dict(values=[0])]

    df = spark.createDataFrame(data)
    df = df.select(array_mean(df.values))

    assert df.toPandas().to_dict(orient="records") == [
        {"array_mean(values)": 2.3333333333333335},
        {"array_mean(values)": 0.0},
    ]
