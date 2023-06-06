/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.internal

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import io.delta.standalone.{DeltaLog, Operation}
import io.delta.standalone.actions.{AddFile => AddFileJ}
import io.delta.standalone.expressions.{And, EqualTo, LessThan, Literal}
import io.delta.standalone.types.{IntegerType, StructField, StructType}

import io.delta.standalone.internal.actions.{Action, AddFile, Metadata}
import io.delta.standalone.internal.sources.StandaloneHadoopConf
import io.delta.standalone.internal.util.{ConversionUtils, FileNames}
import io.delta.standalone.internal.util.TestUtils._

class DeltaScanSuite extends FunSuite {

  private val op = new Operation(Operation.Name.WRITE)

  private val schema = new StructType(Array(
    new StructField("col1", new IntegerType(), true),
    new StructField("col2", new IntegerType(), true),
    new StructField("col3", new IntegerType(), true),
    new StructField("col4", new IntegerType(), true)
  ))

  private val partitionSchema = new StructType(Array(
    new StructField("col1", new IntegerType(), true),
    new StructField("col2", new IntegerType(), true)
  ))

  val metadata = Metadata(
    partitionColumns = partitionSchema.getFieldNames, schemaString = schema.toJson)

  private val files = (1 to 10).map { i =>
    val partitionValues = Map("col1" -> (i % 3).toString, "col2" -> (i % 2).toString)
    AddFile(i.toString, partitionValues, 1L, 1L, dataChange = true)
  }

  private val externalFileSystems = Seq("s3://", "wasbs://", "adls://")

  private val externalFiles = (1 to 10).map { i =>
    val partitionValues = Map("col1" -> (i % 3).toString, "col2" -> (i % 2).toString)
    val schema = externalFileSystems(i % 3)
    AddFile(s"${schema}path/to/$i.parquet", partitionValues, 1L, 1L, dataChange = true)
  }

  private val filesDataChangeFalse = files.map(_.copy(dataChange = false))

  private val metadataConjunct = new EqualTo(schema.column("col1"), Literal.of(0))
  private val dataConjunct = new EqualTo(schema.column("col3"), Literal.of(5))

  def withLog(
               actions: Seq[Action],
               configuration: Configuration = new Configuration()
             )(test: DeltaLog => Unit): Unit = {
    withTempDir { dir =>
      val log = DeltaLog.forTable(configuration, dir.getCanonicalPath)
      log.startTransaction().commit(metadata :: Nil, op, "engineInfo")
      log.startTransaction().commit(actions, op, "engineInfo")

      test(log)
    }
  }

  test("properly splits metadata (pushed) and data (residual) predicates") {
    withLog(files) { log =>
      val mixedConjunct = new LessThan(schema.column("col2"), schema.column("col4"))
      val filter = new And(new And(metadataConjunct, dataConjunct), mixedConjunct)
      val scan = log.update().scan(filter)
      assert(scan.getPushedPredicate.get == metadataConjunct)
      assert(scan.getResidualPredicate.get == new And(dataConjunct, mixedConjunct))
    }
  }

  test("filtered scan with a metadata (pushed) conjunct should return matched files") {
    withLog(files) { log =>
      val filter = new And(metadataConjunct, dataConjunct)
      val scan = log.update().scan(filter)

      assert(scan.getFiles.asScala.toSeq.map(ConversionUtils.convertAddFileJ) ==
        filesDataChangeFalse.filter(_.partitionValues("col1").toInt == 0))

      assert(scan.getPushedPredicate.get == metadataConjunct)
      assert(scan.getResidualPredicate.get == dataConjunct)
    }
  }

  test("filtered scan with only data (residual) predicate should return all files") {
    withLog(files) { log =>
      val filter = dataConjunct
      val scan = log.update().scan(filter)

      assert(scan.getFiles.asScala.toSeq.map(ConversionUtils.convertAddFileJ) ==
        filesDataChangeFalse)
      assert(!scan.getPushedPredicate.isPresent)
      assert(scan.getResidualPredicate.get == filter)
    }
  }

  test("filtered scan with files stored in external file systems") {
    val configuration = new Configuration()
    configuration.setBoolean(StandaloneHadoopConf.RELATIVE_PATH_IGNORE, true)
    withLog(externalFiles, configuration) { log =>
      val filter = dataConjunct
      val scan = log.update().scan(filter)
      val scannedFiles = scan.getFiles.asScala.map(_.getPath).toSet
      val expectedFiles = externalFiles.map(_.path).toSet
      assert(scannedFiles == expectedFiles,
        "paths should not have been made qualified")
    }
  }

  /**
   * This tests the following DeltaScan MemoryOptimized functionalities:
   * - skipping AddFiles that don't match the given filter
   * - returning AddFiles that do match the given filter
   * - skipping AddFiles that were later removed
   * - returning only the latest AddFile that was added across different commits
   * - returning the first AddFile that was written in the same commit .json
   */
  test("correct reverse replay") {
    val filter = new And(
      new EqualTo(partitionSchema.column("col1"), Literal.of(0)),
      new EqualTo(partitionSchema.column("col2"), Literal.of(0))
    )

    val addA_1 = AddFile("a", Map("col1" -> "0", "col2" -> "0"), 1L, 10L, dataChange = true)
    val addA_2 = AddFile("a", Map("col1" -> "0", "col2" -> "0"), 1L, 20L, dataChange = true)
    val addB_4 = AddFile("b", Map("col1" -> "0", "col2" -> "1"), 1L, 40L, dataChange = true) // FAIL
    val addC_7 = AddFile("c", Map("col1" -> "0", "col2" -> "0"), 1L, 70L, dataChange = true)
    val addD_8 = AddFile("d", Map("col1" -> "0", "col2" -> "0"), 1L, 80L, dataChange = true)
    val removeD_9 = addD_8.removeWithTimestamp(90L)
    val addE_13 = AddFile("e", Map("col1" -> "0", "col2" -> "0"), 1L, 10L, dataChange = true)
    val addF_16_0 = AddFile("f", Map("col1" -> "0", "col2" -> "0"), 1L, 130L, dataChange = true)
    val addF_16_1 = AddFile("f", Map("col1" -> "0", "col2" -> "0"), 1L, 131L, dataChange = true)

    withTempDir { dir =>
      val log = DeltaLogImpl.forTable(new Configuration(), dir.getCanonicalPath)

      def commit(actions: Seq[Action]): Unit =
        log.startTransaction().commit(actions, op, "engineInfo")

      commit(metadata :: Nil) // v0
      commit(addA_1 :: Nil) // IGNORED - replaced later by addA_2
      commit(addA_2 :: Nil) // RETURNED - passes filter
      commit(Nil) // v3
      commit(addB_4 :: Nil) // IGNORED - fails filter
      commit(Nil) // v5
      commit(Nil) // v6
      commit(addC_7 :: Nil) // RETURNED
      commit(addD_8 :: Nil) // IGNORED - deleted later
      commit(removeD_9 :: Nil)
      commit(Nil) // v10
      commit(Nil) // v11
      commit(Nil) // v12 - will be overwritten to be an empty file
      commit(addE_13 :: Nil) // RETURNED
      commit(Nil) // v14 - will be overwritten to be an empty file
      commit(Nil) // v15 - will be overwritten to be an empty file
      commit(addF_16_0 :: addF_16_1 :: Nil) // addF_16_0 RETURNED, addF_16_1 IGNORED
      commit(Nil) // v17 - will be overwritten to be an empty file

      Seq(12, 14, 15, 17).foreach { i =>
        val path = FileNames.deltaFile(log.logPath, i)
        log.store.write(path, Iterator().asJava, true, log.hadoopConf)
      }

      val expectedSet = Set(addA_2, addC_7, addE_13, addF_16_0)
        .map(_.copy(dataChange = false))
        .map(ConversionUtils.convertAddFile)

      val set = new scala.collection.mutable.HashSet[AddFileJ]()
      val scan = log.update().scan(filter)
      val iter = scan.getFiles

      while (iter.hasNext) {
        iter.hasNext // let's use another hasNext call to make sure it is idempotent

        set += iter.next()
      }

      assert(set == expectedSet)

      iter.close()
    }
  }
}
