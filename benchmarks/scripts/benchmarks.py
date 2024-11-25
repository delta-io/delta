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

from scripts.utils import *
from datetime import datetime
import time


class BenchmarkSpec:
    """
    Specifications of a benchmark.

    :param format_name: Spark format name
    :param maven_artifacts: Maven artifact name in x:y:z format
    :param spark_confs: list of spark conf strings in key=value format
    :param benchmark_main_class: Name of main Scala class from the JAR to run
    :param main_class_args: command line args for the main class
    """
    def __init__(
            self, format_name, maven_artifacts, spark_confs,
            benchmark_main_class, main_class_args, extra_spark_shell_args=None, **kwargs):
        if main_class_args is None:
            main_class_args = []
        if extra_spark_shell_args is None:
            extra_spark_shell_args = []
        self.format_name = format_name
        self.maven_artifacts = maven_artifacts
        self.spark_confs = spark_confs
        self.benchmark_main_class = benchmark_main_class
        self.benchmark_main_class_args = main_class_args
        self.extra_spark_shell_args = extra_spark_shell_args

    def append_spark_conf(self, new_conf):
        if isinstance(new_conf, str):
            self.spark_confs.append(new_conf)

    def append_spark_confs(self, new_confs):
        if new_confs is not None and isinstance(new_confs, list):
            self.spark_confs.extend(new_confs)

    def append_main_class_args(self, new_args):
        if new_args is not None and isinstance(new_args, list):
            self.benchmark_main_class_args.extend(new_args)

    def get_sparksubmit_cmd(self, benchmark_jar_path):
        spark_conf_str = ""
        for conf in self.spark_confs:
            print(f"conf={conf}")
            spark_conf_str += f"""--conf "{conf}" """
        main_class_args = ' '.join(self.benchmark_main_class_args)
        spark_shell_args_str = ' '.join(self.extra_spark_shell_args)
        spark_submit_cmd = (
            f"spark-submit {spark_shell_args_str} " +
            (f"--packages {self.maven_artifacts} " if self.maven_artifacts else "") +
            f"{spark_conf_str} --class {self.benchmark_main_class} " +
            f"{benchmark_jar_path} {main_class_args}"
        )
        print(spark_submit_cmd)
        return spark_submit_cmd

    def get_sparkshell_cmd(self, benchmark_jar_path, benchmark_init_file_path):
        spark_conf_str = ""
        for conf in self.spark_confs:
            print(f"conf={conf}")
            spark_conf_str += f"""--conf "{conf}" """
        spark_shell_args_str = ' '.join(self.extra_spark_shell_args)
        spark_shell_cmd = (
                f"spark-shell {spark_shell_args_str} " +
                (f"--packages {self.maven_artifacts} " if self.maven_artifacts else "") +
                f"{spark_conf_str} --jars {benchmark_jar_path} -I {benchmark_init_file_path}"
        )
        print(spark_shell_cmd)
        return spark_shell_cmd


class TPCDSDataLoadSpec(BenchmarkSpec):
    """
    Specifications of TPC-DS data load process.
    Always mixin in this first before the base benchmark class.
    """
    def __init__(self, scale_in_gb, exclude_nulls=True, **kwargs):
        # forward all keyword args to next constructor
        super().__init__(benchmark_main_class="benchmark.TPCDSDataLoad", **kwargs)
        self.benchmark_main_class_args.extend([
            "--format", self.format_name,
            "--scale-in-gb", str(scale_in_gb),
            "--exclude-nulls", str(exclude_nulls),
        ])
        # To access the public TPCDS parquet files on S3
        self.spark_confs.extend(["spark.hadoop.fs.s3.useRequesterPaysHeader=true"])


class TPCDSBenchmarkSpec(BenchmarkSpec):
    """
    Specifications of TPC-DS benchmark.
    """
    def __init__(self, scale_in_gb, **kwargs):
        # forward all keyword args to next constructor
        super().__init__(benchmark_main_class="benchmark.TPCDSBenchmark", **kwargs)
        # after init of super class, use the format to add main class args
        self.benchmark_main_class_args.extend([
            "--format", self.format_name,
            "--scale-in-gb", str(scale_in_gb)
        ])


class MergeDataLoadSpec(BenchmarkSpec):
    """
    Specifications of Merge data load process.
    Always mixin in this first before the base benchmark class.
    """
    def __init__(self, scale_in_gb, exclude_nulls=True, **kwargs):
        # forward all keyword args to next constructor
        super().__init__(benchmark_main_class="benchmark.MergeDataLoad", **kwargs)
        self.benchmark_main_class_args.extend([
            "--scale-in-gb", str(scale_in_gb),
        ])
        # To access the public TPCDS parquet files on S3
        self.spark_confs.extend(["spark.hadoop.fs.s3.useRequesterPaysHeader=true"])


class MergeBenchmarkSpec(BenchmarkSpec):
    """
    Specifications of Merge benchmark.
    """
    def __init__(self, scale_in_gb, **kwargs):
        # forward all keyword args to next constructor
        super().__init__(benchmark_main_class="benchmark.MergeBenchmark", **kwargs)
        # after init of super class, use the format to add main class args
        self.benchmark_main_class_args.extend([
            "--scale-in-gb", str(scale_in_gb)
        ])




# ============== Delta benchmark specifications ==============


class DeltaBenchmarkSpec(BenchmarkSpec):
    """
    Specification of a benchmark using the Delta format.
    """
    def __init__(self, delta_version, benchmark_main_class, main_class_args=None, scala_version="2.12", **kwargs):
        delta_spark_confs = [
            "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ]
        self.scala_version = scala_version

        if "spark_confs" in kwargs and isinstance(kwargs["spark_confs"], list):
            kwargs["spark_confs"].extend(delta_spark_confs)
        else:
            kwargs["spark_confs"] = delta_spark_confs

        super().__init__(
            format_name="delta",
            maven_artifacts=self.delta_maven_artifacts(delta_version, self.scala_version),
            benchmark_main_class=benchmark_main_class,
            main_class_args=main_class_args,
            **kwargs
        )

    def update_delta_version(self, new_delta_version):
        self.maven_artifacts = \
            DeltaBenchmarkSpec.delta_maven_artifacts(new_delta_version, self.scala_version)

    @staticmethod
    def delta_maven_artifacts(delta_version, scala_version):
        return f"io.delta:delta-core_{scala_version}:{delta_version},io.delta:delta-contribs_{scala_version}:{delta_version},io.delta:delta-hive_{scala_version}:0.2.0"


class DeltaTPCDSDataLoadSpec(TPCDSDataLoadSpec, DeltaBenchmarkSpec):
    def __init__(self, delta_version, scale_in_gb=1):
        super().__init__(delta_version=delta_version, scale_in_gb=scale_in_gb)


class DeltaTPCDSBenchmarkSpec(TPCDSBenchmarkSpec, DeltaBenchmarkSpec):
    def __init__(self, delta_version, scale_in_gb=1):
        super().__init__(delta_version=delta_version, scale_in_gb=scale_in_gb)


class DeltaMergeDataLoadSpec(MergeDataLoadSpec, DeltaBenchmarkSpec):
    def __init__(self, delta_version, scale_in_gb=1):
        super().__init__(delta_version=delta_version, scale_in_gb=scale_in_gb)


class DeltaMergeBenchmarkSpec(MergeBenchmarkSpec, DeltaBenchmarkSpec):
    def __init__(self, delta_version, scale_in_gb=1):
        super().__init__(delta_version=delta_version, scale_in_gb=scale_in_gb)



# ============== Parquet benchmark specifications ==============


class ParquetBenchmarkSpec(BenchmarkSpec):
    """
    Specification of a benchmark using the Parquet format.
    """
    def __init__(self, benchmark_main_class, main_class_args=None, **kwargs):
        super().__init__(
            format_name="parquet",
            maven_artifacts=None,
            spark_confs=[],
            benchmark_main_class=benchmark_main_class,
            main_class_args=main_class_args,
            **kwargs
        )

class ParquetTPCDSDataLoadSpec(TPCDSDataLoadSpec, ParquetBenchmarkSpec):
    def __init__(self, scale_in_gb=1):
        super().__init__(scale_in_gb=scale_in_gb)


class ParquetTPCDSBenchmarkSpec(TPCDSBenchmarkSpec, ParquetBenchmarkSpec):
    def __init__(self, scale_in_gb=1):
        super().__init__(scale_in_gb=scale_in_gb)


# ============== General benchmark execution ==============


class Benchmark:
    """
    Represents a benchmark that can be run on a remote Spark cluster
    :param benchmark_name: A name to be used for uniquely identifying this benchmark.
                           Added to file names generated by this benchmark.
    :param benchmark_spec: Specification of the benchmark. See BenchmarkSpec.
    """
    def __init__(self, benchmark_name, benchmark_spec, use_spark_shell, local_delta_dir=None):
        now = datetime.now()
        self.benchmark_id = now.strftime("%Y%m%d-%H%M%S") + "-" + benchmark_name
        self.benchmark_spec = benchmark_spec

        # Add benchmark id as a spark conf so that it get transferred automatically to scala code
        self.benchmark_spec.append_spark_confs([f"spark.benchmarkId={self.benchmark_id}"])
        self.output_file = Benchmark.output_file(self.benchmark_id)
        self.json_report_file = Benchmark.json_report_file(self.benchmark_id)
        self.completed_file = Benchmark.completed_file(self.benchmark_id)
        self.use_spark_shell = use_spark_shell
        self.local_delta_dir = local_delta_dir

    def run(self, cluster_hostname, ssh_id_file, ssh_user):
        if self.local_delta_dir and isinstance(self.benchmark_spec, DeltaBenchmarkSpec):
            # Upload new Delta jar to cluster and update spec to use the jar's version
            delta_version_to_use = \
                self.upload_delta_jars_to_cluster_and_get_version(cluster_hostname, ssh_id_file, ssh_user)
            self.benchmark_spec.update_delta_version(delta_version_to_use)

        jar_path_in_cluster = self.upload_jar_to_cluster(cluster_hostname, ssh_id_file, ssh_user)
        self.install_dependencies_via_ssh(cluster_hostname, ssh_id_file, ssh_user)
        self.start_benchmark_via_ssh(cluster_hostname, ssh_id_file, jar_path_in_cluster, ssh_user)
        Benchmark.wait_for_completion(cluster_hostname, ssh_id_file, self.benchmark_id, ssh_user)

    def spark_submit_script_content(self, jar_path):
        return f"""
#!/bin/bash
jps | grep "Spark" | cut -f 1 -d ' ' |  xargs kill -9
set -e
{self.benchmark_spec.get_sparksubmit_cmd(jar_path)} 2>&1 | tee {self.output_file}
""".strip()

    def spark_shell_script_content(self, jar_path):
        shell_init_file_name = f"{self.benchmark_id}_shell_init.scala"
        benchmark_cmd_line_params_str = \
            ', '.join(f'"{w}"' for w in self.benchmark_spec.benchmark_main_class_args)
        call_main_with_args = \
            f"{self.benchmark_spec.benchmark_main_class}.main(Array[String]({benchmark_cmd_line_params_str}))"
        shell_init_file_content = \
            "try { %s } catch { case t => println(t); println(\"FAILED\"); System.exit(1) } ; System.exit(0)" % call_main_with_args
        shell_cmd = self.benchmark_spec.get_sparkshell_cmd(jar_path, shell_init_file_name)
        return f"""
#!/bin/bash
jps | grep "Spark" | cut -f 1 -d ' ' |  xargs kill -9
echo '{shell_init_file_content}' > {shell_init_file_name}
{shell_cmd} 2>&1 | tee {self.output_file}
touch {self.completed_file}
""".strip()

    def upload_jar_to_cluster(self, cluster_hostname, ssh_id_file, ssh_user, delta_version_to_use=None):
        # Compile JAR
        # Note: Deleting existing JARs instead of sbt clean is faster
        if os.path.exists("target"):
            run_cmd("""find target -name "*.jar" -type f -delete""", stream_output=True)
        run_cmd("build/sbt assembly", stream_output=True)
        (_, out, _) = run_cmd("find target -name *.jar")
        print(">>> Benchmark JAR compiled\n")

        # Upload JAR
        jar_local_path = out.decode("utf-8").strip()
        jar_remote_path = f"{self.benchmark_id}-benchmarks.jar"
        scp_cmd = \
            f"scp -C -i {ssh_id_file} {jar_local_path} {ssh_user}@{cluster_hostname}:{jar_remote_path}"
        print(scp_cmd)
        run_cmd(scp_cmd, stream_output=True)
        print(">>> Benchmark JAR uploaded to cluster\n")
        return f"~/{jar_remote_path}"

    def install_dependencies_via_ssh(self, cluster_hostname, ssh_id_file, ssh_user):
        script_file_name = f"{self.benchmark_id}-install-deps.sh"
        script_file_text = """
#!/bin/bash
package='screen'
if [ -x "$(command -v yum)" ]; then
    if rpm -q $package; then
        echo "$package has already been installed"
    else
        sudo yum -y install $package
    fi
elif [ -x "$(command -v apt)" ]; then
    if dpkg -s $package; then
        echo "$package has already been installed"
    else
        sudo apt install $package
    fi
else
    echo "Failed to install packages: Package manager not found. You must manually install: $package">&2; exit 1;
fi


        """.strip()
        self.copy_script_via_ssh(cluster_hostname, ssh_id_file, ssh_user, script_file_name, script_file_text)
        print(">>> Install dependencies script generated and uploaded\n")

        job_cmd = (
                f"ssh -i {ssh_id_file} {ssh_user}@{cluster_hostname} " +
                f"bash {script_file_name}"
        )
        print(job_cmd)
        run_cmd(job_cmd, stream_output=True)
        print(">>> Dependencies have been installed\n")

    def start_benchmark_via_ssh(self, cluster_hostname, ssh_id_file, jar_path, ssh_user):
        # Generate and upload the script to run the benchmark
        script_file_name = f"{self.benchmark_id}-cmd.sh"
        if self.use_spark_shell:
            script_file_text = self.spark_shell_script_content(jar_path)
        else:
            script_file_text = self.spark_submit_script_content(jar_path)

        self.copy_script_via_ssh(cluster_hostname, ssh_id_file, ssh_user, script_file_name, script_file_text)
        print(">>> Benchmark script generated and uploaded\n")

        # Start the script
        job_cmd = (
            f"ssh -i {ssh_id_file} {ssh_user}@{cluster_hostname} " +
            f"screen -d -m bash {script_file_name}"
        )
        print(job_cmd)
        run_cmd(job_cmd, stream_output=True)

        # Print the screen where it is running
        run_cmd(f"ssh -i {ssh_id_file} {ssh_user}@{cluster_hostname}" +
                f""" "screen -ls ; sleep 2; echo Files for this benchmark: ; ls {self.benchmark_id}*" """,
                stream_output=True, throw_on_error=False)
        print(f">>> Benchmark id {self.benchmark_id} started in a screen. Stdout piped into {self.output_file}. "
              f"Final report will be generated on completion in {self.json_report_file}.\n")

    @staticmethod
    def copy_script_via_ssh(cluster_hostname, ssh_id_file, ssh_user, script_file_name, script_file_text):
        try:
            script_file = open(script_file_name, "w")
            script_file.write(script_file_text)
            script_file.close()

            scp_cmd = (
                    f"scp -i {ssh_id_file} {script_file_name}" +
                    f" {ssh_user}@{cluster_hostname}:{script_file_name}"
            )
            print(scp_cmd)
            run_cmd(scp_cmd, stream_output=True)
            run_cmd_over_ssh(f"chmod +x {script_file_name}", cluster_hostname, ssh_id_file, ssh_user,
                             throw_on_error=False)
        finally:
            if os.path.exists(script_file_name):
                os.remove(script_file_name)

    @staticmethod
    def output_file(benchmark_id):
        return f"{benchmark_id}-out.txt"

    @staticmethod
    def json_report_file(benchmark_id):
        return f"{benchmark_id}-report.json"

    @staticmethod
    def csv_report_file(benchmark_id):
        return f"{benchmark_id}-report.csv"

    @staticmethod
    def completed_file(benchmark_id):
        return f"{benchmark_id}-completed.txt"

    @staticmethod
    def wait_for_completion(cluster_hostname, ssh_id_file, benchmark_id, ssh_user, copy_report=True):
        completed = False
        succeeded = False
        output_file = Benchmark.output_file(benchmark_id)
        completed_file = Benchmark.completed_file(benchmark_id)
        json_report_file = Benchmark.json_report_file(benchmark_id)
        csv_report_file = Benchmark.csv_report_file(benchmark_id)

        print(f"\nWaiting for completion of benchmark id {benchmark_id}")
        while not completed:
            # Print the size of the output file to show progress
            (_, out, _) = run_cmd_over_ssh(f"stat -c '%n:   [%y]   [%s bytes]' {output_file}",
                                           cluster_hostname, ssh_id_file, ssh_user,
                                           throw_on_error=False)
            out = out.decode("utf-8").strip()
            print(out)
            if "No such file" in out:
                print(">>> Benchmark failed to start")
                return

            # Check for the existence of the completed file
            (_, out, _) = run_cmd_over_ssh(f"ls {completed_file}", cluster_hostname, ssh_id_file, ssh_user,
                                           throw_on_error=False)
            if completed_file in out.decode("utf-8"):
                completed = True
            else:
                time.sleep(60)

        # Check the last few lines of output files to identify success
        (_, out, _) = run_cmd_over_ssh(f"tail {output_file}", cluster_hostname, ssh_id_file, ssh_user,
                                       throw_on_error=False)
        if "SUCCESS" in out.decode("utf-8"):
            succeeded = True
            print(">>> Benchmark completed with success\n")
        else:
            print(">>> Benchmark completed with failure\n")

        # Download reports
        if copy_report:
            Benchmark.download_file(output_file, cluster_hostname, ssh_id_file, ssh_user)
            if succeeded:
                report_files = [json_report_file, csv_report_file]
                for report_file in report_files:
                    Benchmark.download_file(report_file, cluster_hostname, ssh_id_file, ssh_user)
            print(">>> Downloaded reports to local directory")


    @staticmethod
    def download_file(file, cluster_hostname, ssh_id_file, ssh_user):
        run_cmd(f"scp -C -i {ssh_id_file} " +
                f"{ssh_user}@{cluster_hostname}:{file} {file}",
                stream_output=True)

    def upload_delta_jars_to_cluster_and_get_version(self, cluster_hostname, ssh_id_file, ssh_user):
        if not self.local_delta_dir:
            raise Exception("Path to delta repo not specified")
        delta_repo_dir = os.path.abspath(self.local_delta_dir)

        with WorkingDirectory(delta_repo_dir):
            # Compile Delta JARs by publishing to local maven cache
            print(f"Compiling Delta to local dir {delta_repo_dir}")
            local_maven_delta_dir = os.path.expanduser("~/.ivy2/local/io.delta/")
            if os.path.exists(local_maven_delta_dir):
                run_cmd(f"rm -rf {local_maven_delta_dir}", stream_output=True)
                print(f"Cleared local maven cache at {local_maven_delta_dir}")
            run_cmd("build/sbt publishLocal", stream_output=False, throw_on_error=True)

            # Get the new version
            (_, out, _) = run_cmd("""build/sbt "show version" """)
            version = out.decode("utf-8").strip().rsplit("\n", 1)[-1].rsplit(" ", 1)[-1].strip()
            if not version:
                raise Exception(f"Could not find the version from the sbt output:\n--\n{out}\n-")

            # Upload JARs to cluster's local maven cache
            remote_maven_dir = ".ivy2/local/"  # must have "/" at the end
            run_cmd_over_ssh(
                f"rm -rf {remote_maven_dir}/* .ivy2/cache/io.delta .ivy2/jars/io.delta*",
                cluster_hostname, ssh_id_file, ssh_user, stream_output=True, throw_on_error=False)
            run_cmd_over_ssh(f"mkdir -p {remote_maven_dir}", cluster_hostname,
                             ssh_id_file, ssh_user, stream_output=True)
            scp_cmd = f"""scp -r -C -i {ssh_id_file} {local_maven_delta_dir.rstrip("/")} """ +\
                      f"{ssh_user}@{cluster_hostname}:{remote_maven_dir}"
            print(scp_cmd)
            run_cmd(scp_cmd, stream_output=True)
            print(f">>> Delta {version} JAR uploaded to cluster\n")
            return version
