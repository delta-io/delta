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

import org.apache.spark.sql.SparkSession

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
  // TODO: Instead of hard-coding the path, use the findJar function from
  // Apache Spark's IntegrationTestUtils.scala.
  private val deltaConnectJar = s"$buildLocation/" +
    "spark-connect/server/target/scala-2.13/delta-connect-server-assembly-3.3.0-SNAPSHOT.jar"

  private val resources = s"$buildLocation/spark-connect/client/target/scala-2.13/resource_managed/test"
  private val sparkConnectJar = s"$resources/spark-connect.jar"
  private val sparkSubmit = s"$resources/spark/spark-4.0.0-preview1-bin-hadoop3/sbin/start-connect-server.sh"

  private lazy val server = {
    val command = Seq.newBuilder[String]
    command += sparkSubmit
    command += "--driver-class-path" += s"$sparkConnectJar:$deltaConnectJar"
    command += "--jars" += sparkConnectJar
    command += "--class" += "io.delta.tables.SimpleDeltaConnectService"
    command += "--conf" += s"spark.connect.grpc.binding.port=$serverPort"
    command += "--conf" += "spark.connect.extensions.relation.classes=" +
      "org.apache.spark.sql.connect.delta.DeltaRelationPlugin"
    command += "--conf" += "spark.connect.extensions.command.classes=" +
      "org.apache.spark.sql.connect.delta.DeltaCommandPlugin"
    command += deltaConnectJar

    val builder = new ProcessBuilder(command.result(): _*)
    builder.redirectError(ProcessBuilder.Redirect.INHERIT)
    builder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
    builder.start()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    server
    // TODO: Instead of sleeping for a fixed time, which is a bit brittle,
    // we should repeatedly check when the server is ready.
    Thread.sleep(10000)
    spark = SparkSession.builder().remote(s"sc://localhost:$serverPort").build()
  }

  override def afterAll(): Unit = {
    server.destroy()
    super.afterAll()
  }
}
