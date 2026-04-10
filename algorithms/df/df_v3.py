# La version qui sort, mais en dataframe la.
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_secondary_sorted_df(pairs_df: DataFrame) -> DataFrame:
    symmetric = pairs_df.select("src", "dst").unionByName(
        pairs_df.select(
            F.col("dst").alias("src"),
            F.col("src").alias("dst"),
        )
    )
    # sortWithInParttion = le tri qu'on fait dans la version pyspark.
    return symmetric.repartition(F.col("src")).sortWithinPartitions("src", "dst")


def iterate(pairs_df: DataFrame) -> DataFrame:
    sorted_df = build_secondary_sorted_df(pairs_df)
    ordered = Window.partitionBy("src").orderBy("dst")

    annotated = (
        sorted_df
        .withColumn("row_num", F.row_number().over(ordered))
        .withColumn("min_dst", F.first("dst").over(ordered))
        .withColumn("should_emit", F.col("min_dst") < F.col("src"))
        .filter(F.col("should_emit"))
    )

    self_rows = annotated.filter(F.col("row_num") == 1).select(
        F.col("src"),
        F.col("min_dst").alias("dst"),
        F.lit(False).alias("is_new"),
    )

    neighbour_rows = annotated.filter(F.col("row_num") > 1).select(
        F.col("dst").alias("src"),
        F.col("min_dst").alias("dst"),
        F.lit(True).alias("is_new"),
    )

    return self_rows.unionByName(neighbour_rows)
