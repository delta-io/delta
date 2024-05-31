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

trait RemoteSparkSession extends BeforeAndAfterAll { self: Suite =>

  private val serverPort = 15003
  var spark: SparkSession = _

  private val buildLocation = System.getProperty("delta.test.home")
  private val deltaConnectJar = s"$buildLocation/" +
    s"spark-connect/server/target/scala-2.13/delta-connect-server-assembly-3.2.1-SNAPSHOT.jar"

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
    Thread.sleep(1000)
    spark = SparkSession.builder().remote(s"sc://localhost:$serverPort").build()
    Thread.sleep(1000)
  }

  override def afterAll(): Unit = {
    server.destroy()
    super.afterAll()
  }
}
