# Project structure

The project is organized into 7 main modules :
- algorithms : contains the pyspark algorithms.
    - rdd : pyspark rdd algorithms.
    - df : pyspark dataframe algorithms.
- benchmark : code related to benchmarking the algorithms.
- configs : configurations files in which we specify the graphs that need to be generated for a given benchmark.
- data : directory containing the graphs as .txt files. 
- results : directory containing the results of our experiments.
    - csv : outputs of the benchmarks.
    - plots : outputs of the utils/plot_and_generate_table method.
- scala : .txt file and zeppelin notebook containing the equivalent of the pyspark code in scala.
- utils : module containing diverse helper files.

# Setting up Spark

If, like some of us, your virtual environment doesn't automatically detect your environment variables like JAVA_HOME and SPARK_HOME, you will need to set them manually inside a .env file. Create a new .env file at the root of the project, and write the required environment variables to override. You can use the example given in .env.example.

# Configurations files

In the configs/ directory, you can create a yaml configuration file, in which you specify all the characteristics of a benchmark. An example is given in configs/bench_example.yaml.
You need to specify the name of the directory where you want your graphs to be stored, which is created automatically inside the data directory. You also need to specify the output path for the csv benchmark results, which is going to be created automatically inside the results/csv directory. Finally, you can specify the list of algorithms to include in the benchmark suite, among {rdd_v1, rdd_v3, rdd_v3_exec, df_v1, df_v3}.

Then, you specify a list of graphs, grouped by graph type, by specifying a list of values for all required parameters. For example, a chain graph is only determined by its number of nodes, so the only parameter is n. 
For a multi-component graph, you need to specify the number of nodes n, the number of edges m, and the number of connected components k. 

Here is the list of required parameters for every graph type :
- chain : n
- star : n
- multi_comp : n, k, m
- scale_free : n, m0
- social_net : n, k, m_internal, m_external
- web_google : null
- fixed_diameter : n, m, d

For example, let's analyze the configuration file below. It creates and stores graphs inside the data/data_example directory, and outputs the results of the benchmark to the results/csv/benchmark_example.csv file. It considers all five algorithms for this benchmark. Then, moving to the graphs included in the suite, there are three chain graphs with 10, 100 and 1000 nodes, and three multi-components graphs, the first one with 100000 nodes, 500000 edges and 10 connected components, etc...

# Configuration file example : configs/bench_example.yaml

data path : data_example
output_path : benchmark_example.csv

algos: [
    rdd_v1,
    rdd_v3,
    rdd_v3_exec,
    df_v1,
    df_v3
]

graphs : [
    chain:
        n: [10, 100, 1000]

    multi_comp:
        n: [100000, 500000, 1000000]
        k: [10, 10, 10]
        m: [500000, 2500000, 5000000]
]


# PySpark

Once you have set up your configuration file configs/bench_example.yaml, you can run a PySpark benchmark suite using the following command :

    python -m benchmark.bench -c bench_example.yaml

# Scala

The Scala part of this project is not as customizable as the Python one. Before running the Scala code, make sure to generate the graphs you want to include in the benchmark suite in the directory data/benchmark_scala. To do this, run :

    python -m utils.generate_scala_graphs -c bench_scala.yaml

Where you write the desired graphs inside the configuration file as described above.
Then, to run our Scala code, you have three options :

1. Run the code directly from the spark shell : copy paste the content of the scala/ccf.txt file in the spark shell directly. This is probably the easiest and most reliable solution if you don't want to encounter environment variables issues.
2. Run the Zeppelin notebook ccf.zpln using a Zeppelin container.
3. Run the Jupyter notebook ccf.ipynb using a Scala kernel like spylon-kernel.

# Recommended setup

Since it may be hard to deal with various operating systems, and different versions of python, spark and java, we propose an easy way to run both the PySpark and Scala versions.
Our recommended setup, and the one used for our experiments, is the following : we use Onyxia sspcloud datalab, with the jupyter pyspark service. We use a driver memory of 12GB, 120GB of RAM, 30 cores and initialize Spark with 64 shuffle partitions. From JupyterLab, you can import the required files easily, and run the PySpark and Scala codes from the terminal. 



    

