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

package io.delta.hive

import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.HiveMetaHook
import org.apache.hadoop.hive.metastore.api.MetaException
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.index.IndexSearchCondition
import org.apache.hadoop.hive.ql.io.IOConstants
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.AbstractSerDe
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.typeinfo.{StructTypeInfo, TypeInfo, TypeInfoFactory, TypeInfoUtils}
import org.apache.hadoop.mapred.{InputFormat, JobConf, OutputFormat}
import org.slf4j.LoggerFactory

class DeltaStorageHandler extends DefaultStorageHandler with HiveMetaHook
  with HiveStoragePredicateHandler {

  import DeltaStorageHandler._

  private val LOG = LoggerFactory.getLogger(classOf[DeltaStorageHandler])

  override def getInputFormatClass: Class[_ <: InputFormat[_, _]] = classOf[DeltaInputFormat]

  /**
   * Returns a special [[OutputFormat]] to prevent from writing to a Delta table in Hive before we
   * support it. We have to give Hive some class when creating a table, hence we have to implement
   * an [[OutputFormat]] which throws an exception when Hive is using it.
   */
  override def getOutputFormatClass: Class[_ <: OutputFormat[_, _]] = classOf[DeltaOutputFormat]

  override def getSerDeClass(): Class[_ <: AbstractSerDe] = classOf[ParquetHiveSerDe]

  override def configureInputJobProperties(
      tableDesc: TableDesc,
      jobProperties: java.util.Map[String, String]): Unit = {
    super.configureInputJobProperties(tableDesc, jobProperties)
    val tableProps = tableDesc.getProperties()
    val columnNames =
      DataWritableReadSupport.getColumnNames(tableProps.getProperty(IOConstants.COLUMNS))
    val columnTypes =
      DataWritableReadSupport.getColumnTypes(tableProps.getProperty(IOConstants.COLUMNS_TYPES))
    val hiveSchema = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes)
      .asInstanceOf[StructTypeInfo]
    val rootPath = tableProps.getProperty(META_TABLE_LOCATION)
    val snapshot = DeltaHelper.loadDeltaLatestSnapshot(getConf, new Path(rootPath))
    DeltaHelper.checkTableSchema(snapshot.getMetadata.getSchema, hiveSchema)
    jobProperties.put(DELTA_TABLE_PATH, rootPath)
    jobProperties.put(DELTA_TABLE_SCHEMA, hiveSchema.toString)
  }

  override def decomposePredicate(
      jobConf: JobConf,
      deserializer: Deserializer,
      predicate: ExprNodeDesc): DecomposedPredicate = {
    // Get the delta root path
    val deltaRootPath = jobConf.get(META_TABLE_LOCATION)
    // Get the partitionColumns of Delta
    val partitionColumns = DeltaHelper.getPartitionCols(jobConf, new Path(deltaRootPath))
    if (LOG.isInfoEnabled) {
      LOG.info("delta partitionColumns is " + partitionColumns.mkString(", "))
    }
    val analyzer = newIndexPredicateAnalyzer(partitionColumns)

    val conditions = new java.util.ArrayList[IndexSearchCondition]()
    var pushedPredicate: ExprNodeGenericFuncDesc = null
    var residualPredicate =
      analyzer.analyzePredicate(predicate, conditions).asInstanceOf[ExprNodeGenericFuncDesc]
    for (searchConditions <- decompose(conditions).values) {
      // still push back the pushedPredicate to residualPredicate
      residualPredicate =
        extractResidualCondition(analyzer, searchConditions, residualPredicate)
      pushedPredicate =
        extractStorageHandlerCondition(analyzer, searchConditions, pushedPredicate)
    }

    if (LOG.isInfoEnabled) {
      LOG.info("pushedPredicate:" +
        (if (pushedPredicate == null) "null" else pushedPredicate.getExprString()) +
        ",residualPredicate" + residualPredicate)
    }
    val decomposedPredicate = new DecomposedPredicate()
    decomposedPredicate.pushedPredicate = pushedPredicate
    decomposedPredicate.residualPredicate = residualPredicate
    decomposedPredicate
  }

  private def newIndexPredicateAnalyzer(partitionColumns: Seq[String]): IndexPredicateAnalyzer = {
    val analyzer = new IndexPredicateAnalyzer()
    for (col <- partitionColumns) {
      // Supported filter exprs on partition column to be pushed down to delta
      analyzer.addComparisonOp(col, SUPPORTED_PUSH_DOWN_UDFS: _*)
    }
    analyzer
  }

  private def decompose(searchConditions: JArrayList[IndexSearchCondition]):
    Map[String, JArrayList[IndexSearchCondition]] = {
    val result = mutable.Map[String, java.util.ArrayList[IndexSearchCondition]]()
    for (condition <- searchConditions.asScala) {
      val conditions = result.getOrElseUpdate(
        condition.getColumnDesc().getColumn(),
        new JArrayList[IndexSearchCondition]())
      conditions.add(condition)
    }
    result.toMap
  }

  private def extractResidualCondition(
    analyzer: IndexPredicateAnalyzer,
    searchConditions: java.util.ArrayList[IndexSearchCondition],
    inputExpr: ExprNodeGenericFuncDesc): ExprNodeGenericFuncDesc = {
    if (inputExpr == null) {
      analyzer.translateOriginalConditions(searchConditions)
    } else {
      val children = new JArrayList[ExprNodeDesc]
      children.add(analyzer.translateOriginalConditions(searchConditions))
      children.add(inputExpr)
      new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        FunctionRegistry.getGenericUDFForAnd(), children)
    }
  }

  private def extractStorageHandlerCondition(
    analyzer: IndexPredicateAnalyzer,
    searchConditions: java.util.ArrayList[IndexSearchCondition],
    inputExpr: ExprNodeGenericFuncDesc): ExprNodeGenericFuncDesc = {
    if (inputExpr == null) {
      analyzer.translateSearchConditions(searchConditions)
    } else {
      val children = new JArrayList[ExprNodeDesc]
      children.add(analyzer.translateSearchConditions(searchConditions))
      children.add(inputExpr)
      new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        FunctionRegistry.getGenericUDFForAnd(), children)
    }
  }

  override def getMetaHook: HiveMetaHook = this

  override def preCreateTable(tbl: Table): Unit = {
    if (!MetaStoreUtils.isExternalTable(tbl)) {
      throw new UnsupportedOperationException(
        s"The type of table ${tbl.getDbName}:${tbl.getTableName} is ${tbl.getTableType}." +
          "Only external Delta tables can be read in Hive right now")
    }

    if (tbl.getPartitionKeysSize > 0) {
      throw new MetaException(
        s"Found partition columns " +
          s"(${tbl.getPartitionKeys.asScala.map(_.getName).mkString(",")}) in table " +
          s"${tbl.getDbName}:${tbl.getTableName}. The partition columns in a Delta table " +
          s"will be read from its own metadata and should not be set manually.")    }

    val deltaRootString = tbl.getSd.getLocation
    if (deltaRootString == null || deltaRootString.trim.isEmpty) {
      throw new MetaException("table location should be set when creating a Delta table")
    }

    val snapshot = DeltaHelper.loadDeltaLatestSnapshot(getConf, new Path(deltaRootString))

    // Extract the table schema in Hive to compare it with the latest table schema in Delta logs,
    // and fail the query if it was changed.
    val cols = tbl.getSd.getCols
    val columnNames = new JArrayList[String](cols.size)
    val columnTypes = new JArrayList[TypeInfo](cols.size)
    cols.asScala.foreach { col =>
      columnNames.add(col.getName)
      columnTypes.add(TypeInfoUtils.getTypeInfoFromTypeString(col.getType))
    }
    val hiveSchema = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes)
      .asInstanceOf[StructTypeInfo]
    DeltaHelper.checkTableSchema(snapshot.getMetadata.getSchema, hiveSchema)
    tbl.getParameters.put("spark.sql.sources.provider", "DELTA")
    tbl.getSd.getSerdeInfo.getParameters.put("path", deltaRootString)
  }

  override def rollbackCreateTable(table: Table): Unit = {
    // We don't change the Delta table on the file system. Nothing to do
  }

  override def commitCreateTable(table: Table): Unit = {
    // Nothing to do
  }

  override def preDropTable(table: Table): Unit = {
    // Nothing to do
  }

  override def rollbackDropTable(table: Table): Unit = {
    // Nothing to do
  }

  override def commitDropTable(table: Table, b: Boolean): Unit = {
    // Nothing to do
  }
}

object DeltaStorageHandler {
  /**
   * The Delta table path passing into `JobConf` so that `DeltaLog` can be accessed everywhere.
   */
  val DELTA_TABLE_PATH = "delta.table.path"

  /**
   * The Hive table schema passing into `JobConf` so that `DeltaLog` can be accessed everywhere.
   */
  val DELTA_TABLE_SCHEMA = "delta.table.schema"

  val SUPPORTED_PUSH_DOWN_UDFS = Array(
    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual",
    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan",
    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan",
    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan",
    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan",
    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual",
    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualNS",
    "org.apache.hadoop.hive.ql.udf.UDFLike",
    "org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn"
  )
}
