import time
import math
import statistics
import concurrent.futures
import csv
import logging
import random
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark import StorageLevel
from algorithms.rdd.rdd_ccf import run_ccf_rdd, run_ccf_v3_no_build
from algorithms.rdd import rdd_v1, rdd_v3
from algorithms.df.df_ccf import run_ccf_df
from algorithms.df import df_v1, df_v3
import argparse
from utils.spark_builder import build_spark
import utils.generate_graphs as gg


import glob

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

class GraphDef:
    def __init__(self, name, nodes, edges, comp, filepath):
        self.name = name
        self.nodes = nodes
        self.edges = edges
        self.comp = comp
        self.filepath = filepath

def parse_metadata(filepath: str) -> GraphDef:
    meta = {"graph_type": "unknown", "n_nodes": 0, "n_edges": 0, "n_components": -1}

    if "web-Google" in filepath:
        return GraphDef("web_google", 875713, 5105039, 4336, filepath)

    with open(filepath, 'r') as f:
        for _ in range(10): 
            line = f.readline()
            if not line: break
            if line.startswith("#"):
                parts = line.strip("# \n").split(":")
                if len(parts) == 2:
                    key, val = parts[0].strip(), parts[1].strip()
                    if key in meta:
                        meta[key] = int(val) if val.isdigit() or val.lstrip('-').isdigit() else val

    return GraphDef(meta["graph_type"], meta["n_nodes"], meta["n_edges"], meta["n_components"], filepath)

def load_graph_to_spark(spark, filepath):
    df = spark.read.text(filepath) \
        .filter(~F.col("value").startswith("#")) \
        .select(
            F.split(F.trim(F.col("value")), "\\s+")[0].cast("long").alias("src"),
            F.split(F.trim(F.col("value")), "\\s+")[1].cast("long").alias("dst")
        ).cache()
    
    rdd = df.rdd.map(lambda row: (row.src, row.dst)).cache()
    return rdd, df


ALGORITHMS = {
    "rdd_v1": lambda spark, rdd, df: run_ccf_rdd(spark, rdd, rdd_v1.iterate),
    "rdd_v3": lambda spark, rdd, df: run_ccf_rdd(spark, rdd, rdd_v3.iterate),
    "df_v1": lambda spark, rdd, df: run_ccf_df(spark, df, df_v1.iterate),
    "df_v3": lambda spark, rdd, df: run_ccf_df(spark, df, df_v3.iterate),
    "rdd_v3_exec": lambda spark, rdd, df: run_ccf_v3_no_build(spark, rdd)
}

gg.main()


def execute_with_timeout(spark, job_group_id, func, *args, timeout_sec=100):
    
    def thread_worker():
        spark.sparkContext.setJobGroup(job_group_id, f"Benchmark: {job_group_id}", interruptOnCancel=True)
        try:
            return func(*args)
        finally:
            spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(thread_worker)
        try:
            return future.result(timeout=timeout_sec)
        except concurrent.futures.TimeoutError:
            logger.error(f"--> TIMEOUT: Job {job_group_id} exceeded {timeout_sec}s. Cancelling Spark jobs...")
            spark.sparkContext.cancelJobGroup(job_group_id)
            raise
        except Exception as e:
            logger.error(f"--> ERROR: Job {job_group_id} crashed. Cancelling Spark jobs... {e}")
            spark.sparkContext.cancelJobGroup(job_group_id)
            raise

def run_benchmark(args):
    spark = build_spark()

    sc = spark.sparkContext
    
    RUNS = 3
    TIMEOUT_SECONDS = 600
    CSV_FILE = args.output
    
    data_files = glob.glob("data/*.txt")
    graphs = [parse_metadata(f) for f in data_files]
    algos = ["rdd_v1", "rdd_v3", "rdd_v3_exec", "df_v1", "df_v3"] 

    headers = [
        "algo", "graph_type", "n_nodes", "n_edges", "n_components",
        "median_time_s", "mean_time_s", "stdev_time_s", 
        "successful_runs", "timeout_runs", "n_runs", "timed_out", "error"
    ]

    with open(CSV_FILE, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        logger.info(f"Writing results to {CSV_FILE}")

        for gDef in graphs:
            logger.info(f"Loading graph: {gDef.name} from {gDef.filepath}...")
            rdd, df = load_graph_to_spark(spark, gDef.filepath)
            rdd.count() 
            df.count()

            for algo_name in algos:
                times = []
                timeouts = 0
                errors = 0
                
                for run_id in range(1, RUNS + 1):
                    job_group = f"{gDef.name}-{algo_name}-run{run_id}"
                    
                    try:
                        func = ALGORITHMS[algo_name]
                        
                        _, wall_time = execute_with_timeout(
                            spark, 
                            job_group, 
                            func, 
                            spark, rdd, df, 
                            timeout_sec=TIMEOUT_SECONDS
                        )
                        
                        times.append(wall_time)
                        
                    except concurrent.futures.TimeoutError:
                        timeouts += 1
                    except Exception:
                        errors += 1

                success = len(times)
                is_timeout = 1 if timeouts > 0 else 0
                has_error = 1 if errors > 0 else 0
                
                if success > 0:
                    med = statistics.median(times)
                    mn = statistics.mean(times)
                    stdev = statistics.stdev(times) if success > 1 else 0.0
                else:
                    med, mn, stdev = 0.0, 0.0, 0.0

                row = [
                    algo_name, gDef.name, gDef.nodes, gDef.edges, gDef.comp,
                    f"{med:.6f}", f"{mn:.6f}", f"{stdev:.6f}", 
                    success, timeouts, RUNS, is_timeout, has_error
                ]
                writer.writerow(row)
                f.flush()
                logger.info(",".join(map(str, row)))

            rdd.unpersist()
            df.unpersist()

    logger.info("Benchmark complete. CSV file successfully saved.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Benchmark for our different CCF implementations")
    parser.add_argument("--output", "-o", type = str, default = "results/csv/benchmark_pyspark.csv",
                        help = "Path to the output file.")
    args = parser.parse_args()
    run_benchmark(args)