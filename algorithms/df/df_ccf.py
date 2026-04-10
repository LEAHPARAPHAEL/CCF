"""DataFrame version of CCF: iterate + dedup until no new pair is emitted."""

import os
import sys
import time

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

CHECKPOINT_EVERY = 2


def run_ccf_df(spark: SparkSession, current_df: DataFrame, iterate_fn, max_iterations: int = 100) -> dict:
    """
    Optimized CCF Runner. 
    Note: current_df should be passed in as a DataFrame, not a local Python list!
    """
    prev_persisted_df = None

    total_time = 0.0 

    for iteration in range(1, max_iterations + 1):
        t0 = time.perf_counter() 

        deduped_with_flags = (
            iterate_fn(current_df)
            .groupBy("src", "dst")
            .agg(F.max(F.col("is_new").cast("int")).alias("is_new_int"))
            .persist(StorageLevel.DISK_ONLY) 
        )

        metrics = deduped_with_flags.agg(
            F.count(F.lit(1)).alias("pair_count"),
            F.coalesce(F.sum("is_new_int"), F.lit(0)).alias("new_pairs"),
        ).collect()[0]
        
        loop_time = time.perf_counter() - t0 
        total_time += loop_time

        #pair_count = int(metrics["pair_count"])
        new_pairs = int(metrics["new_pairs"])

        if prev_persisted_df is not None:
            prev_persisted_df.unpersist()

        if new_pairs == 0:
            current_df = deduped_with_flags.select("src", "dst")
            break

        if iteration % CHECKPOINT_EVERY == 0:
            current_df = deduped_with_flags.select("src", "dst").localCheckpoint(eager=True)
            
            deduped_with_flags.unpersist()
            prev_persisted_df = current_df 
        else:
            current_df = deduped_with_flags.select("src", "dst")
            prev_persisted_df = deduped_with_flags

    #component_map = _build_component_map_df(current_df)
    
    if prev_persisted_df is not None:
        prev_persisted_df.unpersist()
        
    #return component_map, total_time
    return current_df, total_time




'''
def run_ccf_df(spark: SparkSession, edge_list: list, iterate_fn, max_iterations: int = 100) -> dict:
    current_df = spark.createDataFrame(edge_list, ["src", "dst"]).select(
        F.col("src").cast("long").alias("src"),
        F.col("dst").cast("long").alias("dst"),
    )

    logger.info(f"CCF DataFrame start | {len(edge_list):,} edges | algo={iterate_fn.__module__}")

    for iteration in range(1, max_iterations + 1):
        t0 = time.perf_counter()

        iterated_df = iterate_fn(current_df).persist(StorageLevel.DISK_ONLY)
        deduped_with_flags = (
            iterated_df.groupBy("src", "dst")
            .agg(F.max(F.col("is_new").cast("int")).alias("is_new_int"))
            .persist(StorageLevel.DISK_ONLY)
        )
        metrics = deduped_with_flags.agg(
            F.count(F.lit(1)).alias("pair_count"),
            F.coalesce(F.sum("is_new_int"), F.lit(0)).alias("new_pairs"),
        ).collect()[0]
        pair_count = int(metrics["pair_count"])
        new_pairs = int(metrics["new_pairs"])
        deduped = deduped_with_flags.select("src", "dst").persist(StorageLevel.DISK_ONLY)
        deduped.count()

        logger.info(
            f"Iter {iteration} | pairs={pair_count} | new={new_pairs} | {time.perf_counter()-t0}s"
        )

        iterated_df.unpersist()
        deduped_with_flags.unpersist()

        if new_pairs == 0:
            logger.info(f"Converged after {iteration} iteration(s)")
            current_df = deduped
            break

        if iteration > 1:
            current_df.unpersist()
        if iteration % CHECKPOINT_EVERY == 0:
            current_df = deduped.localCheckpoint(eager=False).persist(StorageLevel.DISK_ONLY)
            current_df.count()
            deduped.unpersist()
        else:
            current_df = deduped
    else:
        logger.warning(f"Did not converge after {max_iterations} iterations")

    component_map = _build_component_map_df(current_df)
    current_df.unpersist()
    return component_map
'''

def _build_component_map_df(pairs_df: DataFrame) -> dict:
    roots_df = pairs_df.select(
        F.col("dst").alias("src"),
        F.col("dst").alias("dst"),
    ).dropDuplicates()

    component_df = (
        pairs_df.select("src", "dst")
        .unionByName(roots_df)
        .groupBy("src")
        .agg(F.min("dst").alias("component_id"))
    )

    return {
        int(row["src"]): int(row["component_id"])
        for row in component_df.collect()
    }
