/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.tables

import org.scalatest.{BeforeAndAfterAll, Suite}

import org.apache.spark.sql.connect.SparkSession

/**
 * An util class to start a local Delta Connect server in a different process for local E2E tests.
 * Pre-running the tests, the Delta Connect artifact needs to be built using e.g. `build/sbt
 * assembly`. It is designed to start the server once but shared by all tests. It is equivalent to
 * use the following command to start the connect server via command line:
 *
 * {{{
 * bin/spark-shell \
 * --jars `ls spark-connect/server/target/**/delta-connect-server-assembly*SNAPSHOT.jar | paste -sd ',' -` \
 * --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin
 * --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
 * --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
 * }}}
 *
 * Set system property `delta.test.home` or env variable `DELTA_HOME` if the test is not executed
 * from the Delta project top folder.
 */
trait RemoteSparkSession extends BeforeAndAfterAll { self: Suite =>

  // TODO: Instead of hard-coding the server port, assign port number the same way as in Spark Connect.
  private val serverPort = 15003
  var spark: SparkSession = _

  private val buildLocation = System.getProperty("delta.test.home")
  private val javaHome = System.getProperty("java.home")

  private val resources = s"$buildLocation/spark-connect/client/target/scala-2.13/resource_managed/test"

  private lazy val server = {
    // We start SparkSubmit directly. This saves us from downloading an entire Spark distribution
    // for a single test. The parameters used here are the ones that would have been used to start
    // spark-submit.
    val command = Seq.newBuilder[String]
    command += s"$javaHome/bin/java"
    command += "-cp" += resources + "/jars/*"
    command += "-Xmx1g"
    command += "-XX:+IgnoreUnrecognizedVMOptions"
    command += "--add-modules=jdk.incubator.vector"
    command += "--add-opens=java.base/java.lang=ALL-UNNAMED"
    command += "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
    command += "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
    command += "--add-opens=java.base/java.io=ALL-UNNAMED"
    command += "--add-opens=java.base/java.net=ALL-UNNAMED"
    command += "--add-opens=java.base/java.nio=ALL-UNNAMED"
    command += "--add-opens=java.base/java.util=ALL-UNNAMED"
    command += "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
    command += "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
    command += "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
    command += "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    command += "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED"
    command += "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
    command += "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    command += "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    command += "-Djdk.reflect.useDirectMethodHandle=false"
    command += "-Dio.netty.tryReflectionSetAccessible=true"
    command += "-Dderby.connection.requireAuthentication=false"
    command += "org.apache.spark.deploy.SparkSubmit"
    command += "--class" += "io.delta.tables.SimpleDeltaConnectService"
    command += "--conf" += s"spark.connect.grpc.binding.port=$serverPort"
    command += "--conf" += "spark.connect.extensions.relation.classes=" +
      "org.apache.spark.sql.connect.delta.DeltaRelationPlugin"
    command += "--conf" += "spark.connect.extensions.command.classes=" +
      "org.apache.spark.sql.connect.delta.DeltaCommandPlugin"
    command += s"$resources/jars/unused-1.0.0.jar"

    val builder = new ProcessBuilder(command.result(): _*)
    builder.environment().put("SPARK_HOME", resources)
    builder.redirectError(ProcessBuilder.Redirect.INHERIT)
    builder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
    builder.start()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    server
    spark = SparkSession.builder().remote(s"sc://localhost:$serverPort").create()
  }

  override def afterAll(): Unit = {
    server.destroy()
    super.afterAll()
  }
}
