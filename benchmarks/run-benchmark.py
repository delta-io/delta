#!/usr/bin/env python3

#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse

from scripts.benchmarks import *

delta_version = "2.0.0"

# Benchmark name to their specifications. See the imported benchmarks.py for details of benchmark.

benchmarks = {
    "test":
        DeltaBenchmarkSpec(
            delta_version=delta_version,
            benchmark_main_class="benchmark.TestBenchmark",
            main_class_args=["--test-param", "value"],
        ),

    # TPC-DS data load
    "tpcds-1gb-delta-load": DeltaTPCDSDataLoadSpec(delta_version=delta_version, scale_in_gb=1),
    "tpcds-3tb-delta-load": DeltaTPCDSDataLoadSpec(delta_version=delta_version, scale_in_gb=3000),
    "tpcds-1gb-parquet-load": ParquetTPCDSDataLoadSpec(scale_in_gb=1),
    "tpcds-3tb-parquet-load": ParquetTPCDSDataLoadSpec(scale_in_gb=3000),

    # TPC-DS benchmark
    "tpcds-1gb-delta": DeltaTPCDSBenchmarkSpec(delta_version=delta_version, scale_in_gb=1),
    "tpcds-3tb-delta": DeltaTPCDSBenchmarkSpec(delta_version=delta_version, scale_in_gb=3000),
    "tpcds-1gb-parquet": ParquetTPCDSBenchmarkSpec(scale_in_gb=1),
    "tpcds-3tb-parquet": ParquetTPCDSBenchmarkSpec(scale_in_gb=3000),

}

delta_log_store_classes = {
    "aws": "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
    "gcp": "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore",
}


# TODO: move to benchmarks.py and refactor how BenchmarkSpec is organized.
class KubernetesSpec:
    def __init__(self, benchmark_id, cluster_endpoint, docker_image):
        self.benchmark_id = benchmark_id
        self.cluster_endpoint = cluster_endpoint
        self.docker_image = docker_image

    @staticmethod
    def extra_maven_packages():
        return [
            "org.apache.hadoop:hadoop-aws:3.3.1",
            # In order to provide fine-grained permissions, IAM roles for service accounts feature is used:
            # https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html
            # S3A connector has to be configured to use WebIdentityTokenCredentialsProvider, which is
            # supported by aws-java-sdk as of version 1.11.704. We use 1.11.901 since it is compatible
            # com.amazonaws:aws-java-sdk-bundle:1.11.901 which is a hadoop-aws:3.3.1 dependency.
            "com.amazonaws:aws-java-sdk-core:1.11.901",
            "com.amazonaws:aws-java-sdk-sts:1.11.901",
            "com.amazonaws:aws-java-sdk-s3:1.11.901"
        ]

    def extra_spark_config(self):
        return [
            "spark.sql.catalogImplementation=hive",
            f"spark.master=k8s://{self.cluster_endpoint}",
            "spark.submit.deployMode=client",
            "spark.driver.bindAddress=0.0.0.0",
            "spark.driver.port=38003",
            "spark.driver.blockManager.port=38004",
            "spark.driver.host=$SPARK_DRIVER_IP",
            "spark.driver.extraJavaOptions=\"-Divy.cache.dir=/tmp/.ivy/cache/ -Divy.home=/tmp/.ivy/\"",
            "spark.kubernetes.driver.pod.name=benchmarks-edge-node",
            "spark.kubernetes.node.selector.role=spark",
            "spark.executor.memory=6g",
            # A fraction of CPU available is always reserved for kubernetes services in each pod, so we do not request full CPU.
            "spark.kubernetes.executor.request.cores=0.95",
            f"spark.kubernetes.container.image={self.docker_image}",
            "spark.kubernetes.namespace=benchmarks",
            "spark.kubernetes.authenticate.driver.serviceAccountName=benchmarks-sa",
            "spark.kubernetes.authenticate.executor.serviceAccountName=benchmarks-sa",
            "spark.kubernetes.authenticate.submission.caCertFile=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
            "spark.kubernetes.authenticate.submission.oauthTokenFile=/var/run/secrets/kubernetes.io/serviceaccount/token",
            "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
        ]


def is_kubernetes_benchmark(args):
    return args.k8s_cluster_endpoint is not None


def get_command_runner(args):
    if is_kubernetes_benchmark(args):
        return KubectlCommandRunner(args.k8s_cluster_endpoint, "benchmarks", "benchmarks-edge-node")
    else:
        return SshCommandRunner(args.cluster_hostname, args.ssh_id_file, args.ssh_user)


def parse_args():
    # Parse cmd line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--benchmark", "-b",
        required=True,
        help="Run the given benchmark. See this " +
             "python file for the list of predefined benchmark names and definitions.")
    parser.add_argument(
        "--resume-benchmark",
        help="Resume waiting for the given running benchmark.")
    parser.add_argument(
        "--spark-conf",
        action="append",
        help="Run benchmark with given spark conf. Use separate --spark-conf for multiple confs.")
    parser.add_argument(
        "--use-local-delta-dir",
        help="Local path to delta repository which will be used for running the benchmark " +
             "instead of the version specified in the specification. Make sure that new delta" +
             " version is compatible with version in the spec.")
    parser.add_argument(
        "--cloud-provider",
        choices=delta_log_store_classes.keys(),
        help="Cloud where the benchmark will be executed.")

    yarn_cluster_group = parser.add_argument_group('yarn-cluster')
    yarn_cluster_group.add_argument(
        "--cluster-hostname",
        help="Hostname or public IP of the cluster driver")
    yarn_cluster_group.add_argument(
        "--ssh-id-file", "-i",
        help="SSH identity file")
    yarn_cluster_group.add_argument(
        "--ssh-user",
        default="hadoop",
        help="The user which is used to communicate with the master via SSH.")

    kubernetes_cluster_group = parser.add_argument_group('kubernetes-cluster')
    kubernetes_cluster_group.add_argument(
        "--k8s-cluster-endpoint",
        help="Kubernetes cluster endpoint URL.")
    kubernetes_cluster_group.add_argument(
        "--docker-image",
        help="Spark docker image URI.")

    parsed_args, parsed_passthru_args = parser.parse_known_args()
    return parsed_args, parsed_passthru_args


def run_single_benchmark(benchmark_name, benchmark_spec, other_args):
    benchmark_id = BenchmarkIdGenerator.get(benchmark_name)
    benchmark_spec.append_spark_confs(other_args.spark_conf)
    benchmark_spec.append_spark_conf(delta_log_store_classes.get(other_args.cloud_provider))
    benchmark_spec.append_main_class_args(passthru_args)

    if is_kubernetes_benchmark(args):
        k8s_spec = KubernetesSpec(benchmark_id, args.k8s_cluster_endpoint, args.docker_image)
        benchmark_spec.append_spark_confs(k8s_spec.extra_spark_config())
        benchmark_spec.append_maven_artifacts(k8s_spec.extra_maven_packages())

    print("------")
    print("Benchmark spec to run:\n" + str(vars(benchmark_spec)))
    print("------")

    benchmark = Benchmark(benchmark_id,
                          benchmark_spec,
                          use_spark_shell=True,
                          local_delta_dir=other_args.use_local_delta_dir,
                          command_runner=get_command_runner(args))
    benchmark_dir = os.path.dirname(os.path.abspath(__file__))
    with WorkingDirectory(benchmark_dir):
        benchmark.run()


if __name__ == "__main__":
    """
    Run benchmark on a cluster using ssh.

    Example usage:

      ./run-benchmark.py --benchmark test --cluster-hostname <hostname> -i <pem file> --ssh-user <ssh user> --cloud-provider <cloud provider> 
    or
      ./run-benchmark.py --benchmark test --k8s-cluster-endpoint <url> --docker-image <image-name> --cloud-provider <cloud provider>
    """
    args, passthru_args = parse_args()
    if args.resume_benchmark is not None:
        cmd_executor = get_command_runner(args)
        benchmark = Benchmark(args.resume_benchmark, None, None, cmd_executor, None)
        benchmark.wait_for_completion()
        exit(0)

    benchmark_names = args.benchmark.split(",")
    for benchmark_name in benchmark_names:
        # Create and run the benchmark
        if benchmark_name in benchmarks:
            run_single_benchmark(benchmark_name, benchmarks[benchmark_name], args)
        else:
            raise Exception("Could not find benchmark spec for '" + benchmark_name + "'." +
                            "Must provide one of the predefined benchmark names:\n" +
                            "\n".join(benchmarks.keys()) +
                            "\nSee this python file for more details.")
