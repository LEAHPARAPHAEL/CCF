import sys, os
import time

from pyspark.sql import SparkSession

from algorithms.rdd.rdd_dedup import dedup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from pyspark import RDD
from algorithms.rdd.rdd_v3 import build_secondary_sorted_rdd, emit_from_secondary_sorted_rdd
from pyspark import StorageLevel



def run_ccf_rdd(spark: SparkSession, current_rdd: RDD, iterate_fn, max_iterations: int = 100) -> RDD:

    sc = spark.sparkContext

    n_iters = 0
    total_time = 0.0 

    for iteration in range(1, max_iterations + 1):
        n_iters += 1
        t0 = time.perf_counter() 
        
        new_pair_acc = sc.accumulator(0)
        iterated = iterate_fn(current_rdd, new_pair_acc)
        deduped = dedup(iterated)
        deduped.cache()
        
        if iteration % 10 == 0:
            deduped.localCheckpoint()

        #pair_count = deduped.count()
        new_pairs = new_pair_acc.value
        
        loop_time = time.perf_counter() - t0 
        total_time += loop_time

        previous_rdd = current_rdd
        current_rdd = deduped

        if new_pairs == 0:
            break

        if iteration > 1:
            previous_rdd.unpersist()
            
    return current_rdd, total_time, n_iters



def run_ccf_v3_no_build(spark: SparkSession, current_rdd: RDD, max_iterations: int = 100) -> tuple[RDD, float]:

    sc = spark.sparkContext

    n_iters = 0
    
    total_compute_time = 0.0

    for iteration in range(1, max_iterations + 1):

        n_iters += 1

        sorted_rdd = build_secondary_sorted_rdd(current_rdd).persist(StorageLevel.MEMORY_AND_DISK)

        sorted_rdd.count()

        t0 = time.perf_counter()
        
        new_pair_acc = sc.accumulator(0)
        
        iterated = emit_from_secondary_sorted_rdd(sorted_rdd, new_pair_acc)
        deduped = dedup(iterated)
        
        deduped.cache()

        if iteration % 10 == 0:
            deduped.localCheckpoint()

        #pair_count = deduped.count()
        new_pairs = new_pair_acc.value
        
        loop_time = time.perf_counter() - t0
        total_compute_time += loop_time

        sorted_rdd.unpersist()
        
        previous_rdd = current_rdd
        current_rdd = deduped

        if new_pairs == 0:
            break

        if iteration > 1:
            previous_rdd.unpersist()
            
    return current_rdd, total_compute_time, n_iters

def _build_component_map(pairs: list) -> dict:
    component_map = {}
    for node, comp in pairs:
        if node not in component_map or comp < component_map[node]:
            component_map[node] = comp
    # TODO: jsp si c'est utile, je crois pas mais faut check pour être sûr.
    for node, comp in pairs:
        if comp not in component_map:
            component_map[comp] = comp
    return component_map
