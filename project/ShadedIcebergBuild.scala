/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._
import sbtassembly.*

/**
 * Exclusion rules for not bringing in conflicting dependencies via Iceberg Jar
 */

object ShadedIcebergBuild {
  val icebergExclusionRules = List.apply(
    ExclusionRule("com.github.ben-manes.caffeine"),
    ExclusionRule("io.netty"),
    ExclusionRule("org.apache.httpcomponents.client5"),
    ExclusionRule("org.apache.httpcomponents.core5"),
    ExclusionRule("io.airlift"),
    ExclusionRule("org.apache.commons"),
    ExclusionRule("commons-io"),
    ExclusionRule("commons-compress"),
    ExclusionRule("commons-lang3"),
    ExclusionRule("commons-codec"),
    ExclusionRule("com.fasterxml.jackson.core"),
    ExclusionRule("com.fasterxml.jackson.databind"),
  )

  val hadoopClientExclusionRules = List.apply(
    ExclusionRule("org.apache.avro"),
    ExclusionRule("org.slf4j"),
    ExclusionRule("commons-beanutils"),
    ExclusionRule("org.datanucleus"),
    ExclusionRule("io.netty")
  )

  val hiveMetastoreExclusionRules = List.apply(
    ExclusionRule("org.apache.avro"),
    ExclusionRule("org.slf4j"),
    ExclusionRule("org.pentaho"),
    ExclusionRule("org.apache.hbase"),
    ExclusionRule("org.apache.logging.log4j"),
    ExclusionRule("co.cask.tephra"),
    ExclusionRule("com.google.code.findbugs"),
    ExclusionRule("org.eclipse.jetty.aggregate"),
    ExclusionRule("org.eclipse.jetty.orbit"),
    ExclusionRule("org.apache.parquet"),
    ExclusionRule("com.tdunning"),
    ExclusionRule("javax.transaction"),
    ExclusionRule("com.zaxxer"),
    ExclusionRule("org.apache.ant"),
    ExclusionRule("javax.servlet"),
    ExclusionRule("javax.jdo"),
    ExclusionRule("commons-beanutils"),
    ExclusionRule("org.datanucleus")
  )

  /**
   * Replace those files with our customized version
   * Here's an overview:
   *  PartitionSpec: sets checkConflicts to false to honor field ID assigned by Delta
   *  HiveCatalog, HiveTableOperations: allow metadataUpdates to overwrite schema and partition spec
   *  RESTFileScanTaskParser: fixes NoSuchElementException on empty delete-file-references arrays
   */
  def updateMergeStrategy(prev: String => MergeStrategy): String => MergeStrategy = {
    case PathList("shadedForDelta", "org", "apache", "iceberg", s)
      if s.matches("TableMetadata(\\$.*)?\\.class") =>
      MergeStrategy.first
    case PathList("shadedForDelta", "org", "apache", "iceberg", s)
      if s.matches("MetadataUpdate(\\$.*)?\\.class") =>
      MergeStrategy.first
    case PathList("shadedForDelta", "org", "apache", "iceberg", "PartitionSpec$Builder.class") =>
      MergeStrategy.first
    case PathList("shadedForDelta", "org", "apache", "iceberg", "PartitionSpec.class") =>
      MergeStrategy.first
    case PathList("shadedForDelta", "org", "apache", "iceberg", "rest", "RESTFileScanTaskParser.class") =>
      MergeStrategy.first
    case PathList("shadedForDelta", "org", "apache", "iceberg", "hive", "HiveCatalog.class") =>
      MergeStrategy.first
    case PathList("shadedForDelta", "org", "apache", "iceberg", "hive", "HiveCatalog$1.class") =>
      MergeStrategy.first
    case PathList(
    "shadedForDelta",
    "org",
    "apache",
    "iceberg",
    "hive",
    "HiveCatalog$ViewAwareTableBuilder.class"
    ) =>
      MergeStrategy.first
    case PathList(
    "shadedForDelta",
    "org",
    "apache",
    "iceberg",
    "hive",
    "HiveCatalog$TableAwareViewBuilder.class"
    ) =>
      MergeStrategy.first
    case PathList(
    "shadedForDelta",
    "org",
    "apache",
    "iceberg",
    "hive",
    "HiveTableOperations.class"
    ) =>
      MergeStrategy.first
    case PathList(
    "shadedForDelta",
    "org",
    "apache",
    "iceberg",
    "hive",
    "HiveTableOperations$1.class"
    ) =>
      MergeStrategy.first
    case PathList("org", "slf4j", xs @ _*) =>
      // SLF4J is provided by Spark runtime, exclude from assembly
      MergeStrategy.discard
    case PathList("org", "jspecify", "annotations", xs @ _*) =>
      MergeStrategy.discard
    case x => prev(x)
  }
}
