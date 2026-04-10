#  ça c'est ma try de spammer chatGPT et claude pour essayer de fixer un peu les performances guez des autres implémentations.
#  mais ça a pas marché sadly.
"""CCF-Iterate v2 in a Catalyst-friendly DataFrame form.

Unlike v1_df and v3_df, this variant is intentionally written to play well with
Spark SQL's optimizer:
- only built-in relational operators
- no Python UDF
- no collect_list / no window
- a min-per-key aggregate followed by a join the optimizer can turn into a
  BroadcastHashJoin when the propagating side stays small enough
"""

from pyspark.sql import DataFrame, functions as F

def iterate(pairs_df: DataFrame) -> DataFrame:
    # 1. Symmetrize the graph so every node sees all its neighbours.
    symmetric = pairs_df.select("src", "dst").unionByName(
        pairs_df.select(
            F.col("dst").alias("src"),
            F.col("src").alias("dst")
        )
    )

    # 2. Compute the minimum neighbour per node with a native SQL aggregate.
    # This is the closest DataFrame counterpart of reduceByKey(min).
    propagating = (
        symmetric.groupBy("src")
        .agg(F.min("dst").alias("min_dst"))
        .filter(F.col("min_dst") < F.col("src"))
    )

    # 3. Emit the self update (node -> minimum).
    self_rows = propagating.select(
        F.col("src"),
        F.col("min_dst").alias("dst"),
        F.lit(False).alias("is_new")
    )

    # 4. Propagate the minimum to neighbours with an optimizer-friendly join.
    # We explicitly hint broadcast on the propagating side because it is
    # typically much smaller than the symmetric edge relation.
    neighbour_rows = (
        symmetric.join(
            F.broadcast(propagating.select("src", "min_dst")),
            on="src",
            how="inner",
        )
        .filter(F.col("dst") != F.col("min_dst"))
        .select(
            F.col("dst").alias("src"),
            F.col("min_dst").alias("dst"),
            F.lit(True).alias("is_new"),
        )
    )

    # The outer loop handles deduplication while preserving the "is_new" flag.
    return self_rows.unionByName(neighbour_rows)
