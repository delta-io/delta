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

package org.apache.spark.sql.delta.stats

import scala.collection.mutable

import org.apache.spark.sql.delta.JoinedProjection
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

/**
 * A [[WriteTaskStats]] that contains a map from file name to the json representation
 * of the collected statistics.
 */
case class DeltaFileStatistics(stats: Map[String, String]) extends WriteTaskStats

/**
 * [[WriteTaskStatsTracker]] that collects the statistics defined by [[StatisticsCollection]]
 * for files that are being written into a delta table.
 *
 * @param dataCols Resolved data (i.e. non-partitionBy) columns of the dataframe to be written.
 * @param statsColExpr Resolved expression for computing all the statistics that we want to gather.
 * @param rootPath The Reservoir's root path.
 * @param hadoopConf Hadoop Config for being able to instantiate a [[FileSystem]].
 */
class DeltaStatisticsTracker(
    dataCols: Seq[Attribute],
    statsColExpr: Expression,
    rootPath: Path,
    hadoopConf: Configuration) extends WriteTaskStatsTracker {

  // Still used by Photon.
  private var currentFilePath: String = _

  private[this] val submittedFiles = mutable.HashMap[String, InternalRow]()

  /** Were the statistic computed by DeltaStatisticsTracker or by Photon? */
  private var statsComputedByPhoton: Boolean = false

  // For example, when strings are involved, statsColExpr might look like
  // struct(
  //   count("*") as "numRecords"
  //   struct(
  //     substring(min(col), 0, stringPrefix))
  //   ) as "minValues",
  //   struct(
  //     udf(max(col))
  //   ) as "maxValues"
  // ) as "stats"

  // [[DeclarativeAggregate]] is the API to the Catalyst machinery for initializing and updating
  // the result of an aggregate function. We will be using it here the same way it's used during
  // query execution.

  // Given the example above, aggregates would hold: Seq(count, min, max)
  private val aggregates: Seq[DeclarativeAggregate] = statsColExpr.collect {
    case ae: AggregateExpression if ae.aggregateFunction.isInstanceOf[DeclarativeAggregate] =>
      ae.aggregateFunction.asInstanceOf[DeclarativeAggregate]
  }

  // The fields of aggBuffer - see below
  private val aggBufferAttrs: Seq[Attribute] = aggregates.flatMap(_.aggBufferAttributes)

  // This projection initializes aggBuffer with the neutral values for the agg fcns e.g. 0 for sum
  private val initializeStats: MutableProjection = GenerateMutableProjection.generate(
    expressions = aggregates.flatMap(_.initialValues),
    inputSchema = Seq.empty,
    useSubexprElimination = false
  )

  // This projection combines the intermediate results stored by aggBuffer with the values of the
  // currently processed row and updates aggBuffer in place.
  private val updateStats: MutableProjection = GenerateMutableProjection.generate(
    expressions = JoinedProjection.bind(
      aggBufferAttrs,
      dataCols,
      aggregates.flatMap(_.updateExpressions)),
    inputSchema = Nil,
    useSubexprElimination = true
  )

  // This executes the whole statsColExpr in order to compute the final stats value for the file.
  // In order to evaluate it, we have to replace its aggregate functions with the corresponding
  // aggregates' evaluateExpressions that basically just return the results stored in aggBuffer.
  private val resultExpr: Expression = statsColExpr.transform {
    case ae: AggregateExpression if ae.aggregateFunction.isInstanceOf[DeclarativeAggregate] =>
      ae.aggregateFunction.asInstanceOf[DeclarativeAggregate].evaluateExpression
  }

  // See resultExpr above
  private val getStats: Projection = UnsafeProjection.create(
    exprs = Seq(resultExpr),
    inputSchema = aggBufferAttrs
  )

  // This serves as input to updateStats, with aggBuffer always on the left, while the right side
  // is every time replaced with the row currently being processed - see updateStats and newRow.
  private val extendedRow: GenericInternalRow = new GenericInternalRow(2)

  // file path to corresponding stats encoded as json
  private val results = new collection.mutable.HashMap[String, String]

  // called once per file, executes the getStats projection
  override def closeFile(filePath: String): Unit = {
    // We assume file names are unique
    val fileName = new Path(filePath).getName
    if (statsComputedByPhoton) {
      assert(results.contains(fileName), s"No Photon stats provided for file: $filePath")
    } else {
      assert(!results.contains(fileName), s"Stats already recorded for file: $filePath")
      // this is statsColExpr's output (json string)
      val jsonStats = getStats(submittedFiles(filePath)).getString(0)
      results += ((fileName, jsonStats))
      submittedFiles.remove(filePath)
    }
  }

  override def newPartition(partitionValues: InternalRow): Unit = { }

  override def newFile(newFilePath: String): Unit = {
    currentFilePath = newFilePath
    submittedFiles.getOrElseUpdate(newFilePath, {
      // `buffer` is a row that will start off by holding the initial values for the agg expressions
      // (see the initializeStats: Projection), will then be updated in place every time a new row
      // is processed (see updateStats: Projection), and will finally serve as an input for
      // computing the per-file result of statsColExpr (see getStats: Projection)
      val buffer = new SpecificInternalRow(aggBufferAttrs.map(_.dataType))
      if (!statsComputedByPhoton) {
        initializeStats.target(buffer).apply(EmptyRow)
      } else {
        buffer
      }
    })
  }

  override def newRow(filePath: String, currentRow: InternalRow): Unit = {
    assert(!statsComputedByPhoton, "Using newRow() while the stats should be provided by Photon")
    val aggBuffer = submittedFiles(filePath)
    extendedRow.update(0, aggBuffer)
    extendedRow.update(1, currentRow)
    updateStats.target(aggBuffer).apply(extendedRow)
  }

  override def getFinalStats(taskCommitTime: Long): DeltaFileStatistics = {
    submittedFiles.keys.foreach(closeFile)
    submittedFiles.clear()
    DeltaFileStatistics(results.toMap)
  }

  /**
   * Flattens the struct nested columns and for each column returns its data type and an unique
   * custom name created by appending the inner field column name to the given `customExprId`
   * (i.e. an accumulator for so far created custom name, which initially is equal
   * to the column `exprId`).
   */
  def flattenStructCols(field: StructType, customExprId: String): Array[(String, DataType)] = {
    field.fields.flatMap { f =>
      f.dataType match {
        case structField: StructType =>
          flattenStructCols(structField, customExprId + "." + f.name)
        case _ =>
          Array((customExprId + "." + f.name, f.dataType))
      }
    }
  }

  /**
   * Creates a unique custom ID for `structField` in `statsColExpr` that matches the format of the
   * names created through `flattenStructCols()`. This allows us to match the nested stats schema
   * used by Spark with flattened schema produced by Photon. All nested children of `structField`
   * are of `GetStructField` type, while the last child will be an `AttributeReference`
   * which has access to the `exprId`.
   */
  private def createCustomExprId(structField: GetStructField): String = {
    val structName = s".${structField.name.getOrElse(
      if (structField.resolved) structField.childSchema(structField.ordinal).name
      else s"_${structField.ordinal}")}"
    val childName = structField.child match {
      case c: AttributeReference => c.exprId.toString + "." + c.name
      case c: GetStructField => createCustomExprId(c)
    }
    childName + structName
  }

  /**
   * Rewritten version of `statsColExpr` that uses the aggregates computed by Photon, instead of
   * computing them using Spark. Aggregates are replaced by bound reference into the InternalRow
   * returned by Photon. See `NativeDataWriter.DeltaStatistics` to see how the indexes for each
   * aggregate into the row are computed.
   */
  private lazy val photonStatsColExpr: Projection = {
    val dataColsSeq: AttributeSeq = dataCols

    val flattenedCols = mutable.ArrayBuffer[(String, DataType)]()
    for (col <- dataCols) {
      col.dataType match {
        case structCol: StructType =>
          flattenedCols ++= flattenStructCols(structCol, col.exprId.toString + "." + col.name)
        case _ =>
          flattenedCols.append((col.exprId.toString, col.dataType))
      }
    }

    val minMaxIndex = mutable.Map[String, Int]()
    val nullCntIndex = mutable.Map[String, Int]()
    var idx: Int = 1
    for ((customExprId, dt) <- flattenedCols) {
      if (dt == StringType || dt == DateType ||
          dt == TimestampType || dt.isInstanceOf[NumericType]) {
        minMaxIndex(customExprId) = idx
        idx += 2
      }
      nullCntIndex(customExprId) = idx
      idx += 1
    }

    val photonResultExpr = statsColExpr.transform {
      case ae : AggregateExpression =>
        val photonStatsColumnIndex = ae.aggregateFunction match {
          case Count(Seq(Literal(1, IntegerType))) =>
            // Number of records.
            0

          case Min(a: AttributeReference) if contains(dataColsSeq, a.exprId) =>
            // Minimum value.
            minMaxIndex(a.exprId.toString)

          case Max(a: AttributeReference) if contains(dataColsSeq, a.exprId) =>
            // Maximum value.
            minMaxIndex(a.exprId.toString) + 1

          case Min(a: GetStructField) =>
            // Minimum value.
            minMaxIndex(createCustomExprId(a))

          case Max(a: GetStructField) =>
            // Maximum value.
            minMaxIndex(createCustomExprId(a)) + 1

          case Sum(CaseWhen(
                Seq((IsNull(a: GetStructField), Literal(1, IntegerType))),
                Some(Literal(0, IntegerType))), _) =>
            // Number of null values.
            nullCntIndex(createCustomExprId(a))

          case Sum(CaseWhen(
                Seq((IsNull(a: AttributeReference), Literal(1, IntegerType))),
                Some(Literal(0, IntegerType))), _)
              if contains(dataColsSeq, a.exprId) =>
            // Number of null values.
            nullCntIndex(a.exprId.toString)

          case _ =>
            throw new IllegalArgumentException(
              s"Photon did not compute the aggregate ${ae.aggregateFunction}.")
        }
        BoundReference(photonStatsColumnIndex, ae.dataType, ae.nullable)
    }
    UnsafeProjection.create(photonResultExpr)
  }



  private def contains(attrSeq: AttributeSeq, exprId: ExprId): Boolean = {
    attrSeq.indexOf(exprId) >= 0
  }
}

/**
 * Serializable factory class that holds together all required parameters for being able to
 * instantiate a [[DeltaStatisticsTracker]] on an executor.
 *
 * @param hadoopConf The Hadoop configuration object to use on an executor.
 * @param path Root Reservoir path
 * @param dataCols Resolved data (i.e. non-partitionBy) columns of the dataframe to be written.
 */
class DeltaJobStatisticsTracker(
    @transient private val hadoopConf: Configuration,
    @transient val path: Path,
    val dataCols: Seq[Attribute],
    val statsColExpr: Expression) extends WriteJobStatsTracker {

  var recordedStats: Map[String, String] = _

  private val srlHadoopConf = new SerializableConfiguration(hadoopConf)
  private val rootUri = path.getFileSystem(hadoopConf).makeQualified(path).toUri()

  override def newTaskInstance(): WriteTaskStatsTracker = {
    val rootPath = new Path(rootUri)
    val hadoopConf = srlHadoopConf.value
    new DeltaStatisticsTracker(dataCols, statsColExpr, rootPath, hadoopConf)
  }

  override def processStats(stats: Seq[WriteTaskStats], jobCommitTime: Long): Unit = {
    recordedStats = stats.map(_.asInstanceOf[DeltaFileStatistics]).flatMap(_.stats).toMap
  }
}
