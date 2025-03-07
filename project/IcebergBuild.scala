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
object IcebergBuild {
  val icebergExclusionRules = List.apply(
    ExclusionRule("com.github.ben-manes.caffeine"),
    ExclusionRule("io.netty")
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
}
