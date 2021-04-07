/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta

import java.io.File

import org.apache.spark.sql.delta.actions.{Action, FileAction, SingleAction}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

abstract class EvolvabilitySuiteBase
    extends QueryTest with SharedSparkSession {
  import testImplicits._

  protected def testEvolvability(tablePath: String): Unit = {
    // Check we can load everything from a log checkpoint
    val deltaLog = DeltaLog.forTable(spark, new Path(tablePath))
    val path = deltaLog.dataPath.toString
    checkDatasetUnorderly(
      spark.read.format("delta").load(path).select("id", "value").as[(Int, String)],
      4 -> "d", 5 -> "e", 6 -> "f")
    assert(deltaLog.snapshot.metadata.schema === StructType.fromDDL("id INT, value STRING"))
    assert(deltaLog.snapshot.metadata.partitionSchema === StructType.fromDDL("id INT"))

    // Check we can load CheckpointMetaData
    assert(deltaLog.lastCheckpoint === Some(CheckpointMetaData(3, 6L, None)))

    // Check we can parse all `Action`s in delta files. It doesn't check correctness.
    deltaLog.getChanges(0L).toList.map(_._2.toList)
  }
}


// scalastyle:off
/***
 * A tool to generate data for evolvability tests. Here are the steps to generate data.
 *
 * 1. Update `EvolvabilitySuite.generateData` if there are new [[Action]] types.
 * 2. Change the following command with the right path and run it. Note: the working directory is "[delta_project_root]".
 *
 * scalastyle:off
 * ```
 * build/sbt "test:runMain org.apache.spark.sql.delta.EvolvabilitySuite src/test/resources/delta/delta-0.1.0"
 * ```
 */
// scalastyle:on
object EvolvabilitySuiteBase {

  def generateData(
      spark: SparkSession,
      path: String,
      tblProps: Map[DeltaConfig[_], String] = Map.empty): Unit = {
    import spark.implicits._
    implicit val s = spark.sqlContext

    Seq(1, 2, 3).toDF().write.format("delta").save(path)
    if (tblProps.nonEmpty) {
      val tblPropsStr = tblProps.map { case (k, v) => s"'${k.key}' = '$v'" }.mkString(", ")
      spark.sql(s"CREATE TABLE test USING DELTA LOCATION '$path'")
      spark.sql(s"ALTER TABLE test SET TBLPROPERTIES($tblPropsStr)")
    }
    Seq(1, 2, 3).toDF().write.format("delta").mode("append").save(path)
    Seq(1, 2, 3).toDF().write.format("delta").mode("overwrite").save(path)

    val checkpoint = Utils.createTempDir().toString
    val data = MemoryStream[Int]
    data.addData(1, 2, 3)
    val stream = data.toDF()
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpoint)
      .start(path)
    stream.processAllAvailable()
    stream.stop()

    DeltaLog.forTable(spark, path).checkpoint()
  }

  /** Validate the generated data contains all [[Action]] types */
  def validateData(spark: SparkSession, path: String): Unit = {
    import org.apache.spark.sql.delta.util.FileNames._
    import scala.reflect.runtime.{universe => ru}
    import spark.implicits._

    val mirror = ru.runtimeMirror(this.getClass.getClassLoader)

    val tpe = ru.typeOf[Action]
    val clazz = tpe.typeSymbol.asClass
    assert(clazz.isSealed, s"${classOf[Action]} must be sealed")

    val deltaLog = DeltaLog.forTable(spark, new Path(path))
    val deltas = 0L to deltaLog.snapshot.version
    val deltaFiles = deltas.map(deltaFile(deltaLog.logPath, _)).map(_.toString)
    val actionsTypesInLog =
      spark.read.schema(Action.logSchema).json(deltaFiles: _*)
        .as[SingleAction]
        .collect()
        .map(_.unwrap.getClass.asInstanceOf[Class[_]])
        .toSet

    val allActionTypes =
      clazz.knownDirectSubclasses
        .flatMap {
          case t if t == ru.typeOf[FileAction].typeSymbol => t.asClass.knownDirectSubclasses
          case t => Set(t)
        }
        .map(t => mirror.runtimeClass(t.asClass))

    val missingTypes = allActionTypes -- actionsTypesInLog
    val unknownTypes = actionsTypesInLog -- allActionTypes
    assert(
      missingTypes.isEmpty,
      s"missing types: $missingTypes. " +
        "Please update EvolveabilitySuite.generateData to include them in the log.")
    assert(
      unknownTypes.isEmpty,
      s"unknown types: $unknownTypes. " +
        s"Please make sure they inherit ${classOf[Action]} or ${classOf[FileAction]} directly.")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val path = new File(args(0))
    if (path.exists()) {
      // Don't delete automatically in case the user types a wrong path.
      // scalastyle:off throwerror
      throw new AssertionError(s"${path.getCanonicalPath} exists. Please delete it and retry.")
      // scalastyle:on throwerror
    }
    generateData(spark, path.toString)
    validateData(spark, path.toString)
  }
}
