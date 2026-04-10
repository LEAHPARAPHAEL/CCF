# Project structure

The project is organized into 6 main modules :
- algorithms : contains the pyspark algorithms.
    - rdd : pyspark rdd algorithms.
    - df : pyspark dataframe algorithms.
- benchmark : code related to benchmarking the algorithms.
- data : directory containing the graphs as .txt files.
- results : directory containing the results of our experiments.
    - csv : outputs of the benchmarks.
    - plots : outputs of the utils/plot_and_generate_table method.
- scala : .txt file and zeppelin notebook containing the equivalent of the pyspark code in scala.
- utils : module containing diverse helper files.

# Setting up Spark

If, like some of us, your virtual environment doesn't automatically detect your environment variables like JAVA_HOME and SPARK_HOME, you will need to set them manually inside a .env file. Create a new .env file, and write the required environment variables to override. You can use the example given in .env.example.

# PySpark

To test our PySpark algorithms yourself, you can run the benchmark file :

    python -m benchmark.bench

If you want to customize the benchmark suite by adding graphs or changing the ones in the benchmark, you can do so by manually setting your own suite in the main method of the utils/generate_graphs.py file. By default, the output of this code will be a csv file located in results/csv/benchmark_pyspark.csv.

# Scala

To run our Scala code, you have two options :

1. Run the code directly from the spark shell : copy paste the content of the scala/scala.txt file in the spark shell directly.
2. Run the Zeppelin notebook ccf.zpln using a Zeppelin container.

# Recommended setup

Since it may be hard to deal with various operating systems, and different versions of python, spark and java, we propose an easy way to run both the PySpark and Scala versions.
Our recommended setup, and the one we used for our experiments, is the following : we use the Onyxia sspcloud datalab, with the jupyter pyspark service. We use a driver memory of 12GB, 120GB of RAM, 30 cores and initialize Spark with 64 shuffle partitions. You will be able to run the pyspark and scala benchmark from this terminal easily.



    

