ThisBuild / scalaVersion := "2.12.18"

enablePlugins(JmhPlugin)

name := "flink-benchmarks"

version := "0.1.0"
val flinkVersion = "2.0.1"
val hadoopVersion = "3.4.2"

libraryDependencies ++= Seq(
  "org.openjdk.jmh" % "jmh-core" % "1.37",
  "org.openjdk.jmh" % "jmh-generator-annprocess" % "1.37",
  "org.apache.flink" % "flink-core" % flinkVersion,
  "org.apache.flink" % "flink-table-common" % flinkVersion,
  "org.apache.flink" % "flink-streaming-java" % flinkVersion,
  "org.apache.flink" % "flink-table-api-java-bridge" % flinkVersion,
  "org.apache.flink" % "flink-clients" % flinkVersion,
  "org.apache.flink" % "flink-table-api-java-bridge" % flinkVersion,
  "org.apache.flink" % "flink-table-planner-loader" % flinkVersion,
  "org.apache.flink" % "flink-table-runtime" % flinkVersion,
  "io.unitycatalog" % "unitycatalog-client" % "0.3.1",
  "org.apache.httpcomponents" % "httpclient" % "4.5.14",
  "dev.failsafe" % "failsafe" % "3.2.0",
  "com.github.ben-manes.caffeine" % "caffeine" % "3.1.8",
  "org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.5",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.13.5",
  "org.apache.parquet" % "parquet-hadoop" % "1.12.3",
  "com.google.guava" % "guava" % "32.1.3-jre",
  "io.delta" % "delta-kernel-api" % "4.1.0-SNAPSHOT",
  "io.delta" % "delta-kernel-defaults" % "4.1.0-SNAPSHOT",
  "io.delta" % "delta-flink" % "4.1.0-SNAPSHOT",
)

unmanagedSourceDirectories in Compile += baseDirectory.value.getParentFile / "src" / "main" / "java"