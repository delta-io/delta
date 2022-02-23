#!/usr/bin/env python3

import argparse
from scripts.benchmarks import *

delta_version = "1.0.0"

# Benchmark name to their specifications. See the imported benchmarks.py for details of benchmark.

benchmarks = {
    "test":
        DeltaBenchmarkSpec(
            delta_version="1.0.0",
            benchmark_main_class="benchmark.TestBenchmark",
            main_class_args=["--test-param", "value"],
        ),

}


if __name__ == "__main__":
    """
    Run benchmark on a cluster using ssh.

    Example usage:

    ./run-benchmark.py -h <hostname> i <pem file> --benchmark test 

    """

    # Parse cmd line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--benchmark", "-b",
        required=True,
        help="Run the given benchmark. See this " +
             "python file for the list of predefined benchmark names and definitions.")
    parser.add_argument(
        "--cluster-hostname",
        required=True,
        help="Hostname or public IP of the EMR driver")
    parser.add_argument(
        "--ssh-id-file", "-i",
        required=True,
        help="SSH identity file")
    parser.add_argument(
        "--spark-conf",
        action="append",
        help="Run benchmark with given spark conf. Use separate --spark-conf for multiple confs.")
    parser.add_argument(
        "--resume-benchmark",
        help="Resume waiting for the given running benchmark.")
    parser.add_argument(
        "--use-local-delta-dir",
        help="Local path to delta repository which will be used for running the benchmark " +
             "instead of the version specified in the specification. Make sure that new delta" +
             " version is compatible with version in the spec.")

    args, passthru_args = parser.parse_known_args()

    if args.resume_benchmark is not None:
        Benchmark.wait_for_completion(
            args.cluster_hostname, args.ssh_id_file, args.resume_benchmark)
        exit(0)

    # Create and run the benchmark
    if args.benchmark in benchmarks:
        benchmark_spec = benchmarks[args.benchmark]
    else:
        raise Exception("Must provide one of the predefined benchmark names:\n" +
                        "\n".join(benchmarks.keys()) +
                        "\nSee this python file for more details.")
    benchmark_spec.append_spark_confs(args.spark_conf)
    benchmark_spec.append_main_class_args(passthru_args)
    print("------")
    print("Benchmark spec to run:\n" + str(vars(benchmark_spec)))
    print("------")

    benchmark = Benchmark(args.benchmark, benchmark_spec,
                          use_spark_shell=True, local_delta_dir=args.use_local_delta_dir)
    benchmark_dir = os.path.dirname(os.path.abspath(__file__))
    with WorkingDirectory(benchmark_dir):
        benchmark.run(args.cluster_hostname, args.ssh_id_file)
