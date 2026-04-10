# CCF-Iterate v1 in DataFrame form: materialise full neighbour lists via collect_list.

from pyspark.sql import DataFrame, functions as F


def iterate(pairs_df: DataFrame) -> DataFrame:
    # equivalent du flatmap, c'est la version la plus opti normalement
    symmetric = pairs_df.select("src", "dst").unionByName(
        pairs_df.select(
            F.col("dst").alias("src"),
            F.col("src").alias("dst"),
        )
    )
    # gros pb ici car ca va devoir tout materialiser ! Mais pas le choix pour cette version :/
    # grouped = (src | dst | min_dst (constante = voisinage))
    grouped = symmetric.groupBy("src").agg(
        F.collect_list("dst").alias("values")
    ).withColumn("min_dst", F.array_min(F.col("values")))
    # garder que les lignes avec min_dst < src
    propagating = grouped.filter(F.col("min_dst") < F.col("src"))

    # src -> son min_dst
    self_rows = propagating.select(
        F.col("src"),
        F.col("min_dst").alias("dst"),
        F.lit(False).alias("is_new"), # https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lit.html
    )

    neighbour_rows = (
        propagating
        .select(
            F.col("min_dst"),
            F.explode("values").alias("neighbour"), # min_dst -> [v1, v2, ...] transforme en [(min_dst, v1), (min_dst, v2)...].T
        )
        .filter(F.col("neighbour") != F.col("min_dst"))
        .select(
            F.col("neighbour").alias("src"),
            F.col("min_dst").alias("dst"),
            F.lit(True).alias("is_new"),
        )
    )

    return self_rows.unionByName(neighbour_rows)
