package io.delta.hive

import com.google.common.base.Joiner
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.HiveMetaHook
import org.apache.hadoop.hive.metastore.MetaStoreUtils
import org.apache.hadoop.hive.metastore.api.MetaException
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.index.IndexSearchCondition
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.AbstractSerDe
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.util.StringUtils
import org.apache.spark.sql.delta.DeltaHelper
import org.apache.spark.sql.delta.DeltaPushFilter
import org.slf4j.LoggerFactory

class DeltaStorageHandler extends DefaultStorageHandler with HiveMetaHook with HiveStoragePredicateHandler {

  import DeltaStorageHandler._

  private val LOG = LoggerFactory.getLogger(classOf[DeltaStorageHandler])


  override def getInputFormatClass: Class[_ <: InputFormat[_, _]] = classOf[DeltaInputFormat]

  override def getSerDeClass(): Class[_ <: AbstractSerDe] = classOf[ParquetHiveSerDe]

  override def configureInputJobProperties(tableDesc: TableDesc, jobProperties: java.util.Map[String, String]): Unit = {
    super.configureInputJobProperties(tableDesc, jobProperties)
    jobProperties.put(DELTA_TABLE_PATH, tableDesc.getProperties().getProperty(DELTA_TABLE_PATH))
    jobProperties.put(DELTA_PARTITION_COLS_NAMES, tableDesc.getProperties().getProperty(DELTA_PARTITION_COLS_NAMES))
    jobProperties.put(DELTA_PARTITION_COLS_TYPES, tableDesc.getProperties().getProperty(DELTA_PARTITION_COLS_TYPES))
  }

  override def decomposePredicate(jobConf: JobConf, deserializer: Deserializer, predicate: ExprNodeDesc): DecomposedPredicate = {
    // Get the delta root path
    val deltaRootPath = jobConf.get(DELTA_TABLE_PATH)
    // Get the partitionColumns of Delta
    val partitionColumns = DeltaHelper.getPartitionCols(new Path(deltaRootPath))
    LOG.info("delta partitionColumns is " + partitionColumns.mkString(", "))

    val analyzer = newIndexPredicateAnalyzer(partitionColumns)

    val conditions = new java.util.ArrayList[IndexSearchCondition]()
    var pushedPredicate: ExprNodeGenericFuncDesc = null
    var residualPredicate = analyzer.analyzePredicate(predicate, conditions).asInstanceOf[ExprNodeGenericFuncDesc]
    for (searchConditions <- decompose(conditions).values) {
      // still push back the pushedPredicate to residualPredicate
      residualPredicate =
        extractResidualCondition(analyzer, searchConditions, residualPredicate)
      pushedPredicate =
        extractStorageHandlerCondition(analyzer, searchConditions, pushedPredicate)
    }

    LOG.info("pushedPredicate:" + (if (pushedPredicate == null) "null" else pushedPredicate.getExprString())
      + ",residualPredicate" + residualPredicate)

    val decomposedPredicate = new DecomposedPredicate()
    decomposedPredicate.pushedPredicate = pushedPredicate
    decomposedPredicate.residualPredicate = residualPredicate
    decomposedPredicate
  }

  private def newIndexPredicateAnalyzer(partitionColumns: Seq[String]): IndexPredicateAnalyzer = {
    val analyzer = new IndexPredicateAnalyzer()
    for (col <- partitionColumns) {
      // Supported filter exprs on partition column to be pushed down to delta
      analyzer.addComparisonOp(col, DeltaPushFilter.supportedPushDownUDFs: _*)
    }
    analyzer
  }

  private def decompose(searchConditions: java.util.ArrayList[IndexSearchCondition]): Map[String, java.util.ArrayList[IndexSearchCondition]] = {
    val result = mutable.Map[String, java.util.ArrayList[IndexSearchCondition]]()
    for (condition <- searchConditions.asScala) {
      val conditions = result.getOrElseUpdate(condition.getColumnDesc().getColumn(), new java.util.ArrayList[IndexSearchCondition]())
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
      val children = new java.util.ArrayList[ExprNodeDesc]
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
      val children = new java.util.ArrayList[ExprNodeDesc]
      children.add(analyzer.translateSearchConditions(searchConditions))
      children.add(inputExpr)
      new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        FunctionRegistry.getGenericUDFForAnd(), children)
    }
  }

  override def getMetaHook: HiveMetaHook = this

  override def preCreateTable(tbl: Table): Unit = {
    val isExternal = MetaStoreUtils.isExternalTable(tbl)
    if (!isExternal) {
      throw new MetaException("HiveOnDelta should be an external table.")
    } else if (tbl.getPartitionKeysSize() > 0) {
      throw new MetaException("HiveOnDelta does not support to create a partition hive table")
    }

    val deltaRootString = tbl.getSd().getLocation()
    if (deltaRootString == null || deltaRootString.trim().length() == 0) {
      throw new MetaException("table location should be set when creating table")
    } else {
      val deltaPath = new Path(deltaRootString)
      val fs = deltaPath.getFileSystem(getConf())
      if (!fs.exists(deltaPath)) {
        throw new MetaException("delta.table.path(" + deltaRootString + ") does not exist...")
      } else {
        val partitionProps = DeltaHelper.checkHiveColsInDelta(deltaPath, tbl.getSd().getCols())
        tbl.getSd().getSerdeInfo().getParameters().putAll(partitionProps.asJava)
        tbl.getSd().getSerdeInfo().getParameters().put(DELTA_TABLE_PATH, deltaRootString)
        LOG.info("write partition cols/types to table properties " +
          partitionProps.map(kv => s"${kv._1}=${kv._2}").mkString(", "))
      }
    }
  }

  override def rollbackCreateTable(table: Table): Unit = {
    // TODO What should we do?
  }

  override def commitCreateTable(table: Table): Unit = {
    // TODO What should we do?
  }

  override def preDropTable(table: Table): Unit = {
    // TODO What should we do?
  }

  override def rollbackDropTable(table: Table): Unit = {
    // TODO What should we do?
  }

  override def commitDropTable(table: Table, b: Boolean): Unit = {
    // TODO What should we do?
  }
}

object DeltaStorageHandler {
  val DELTA_TABLE_PATH = "delta.table.path"
  val DELTA_PARTITION_COLS_NAMES = "delta.partition.columns"
  val DELTA_PARTITION_COLS_TYPES = "delta.partition.columns.types"
}
