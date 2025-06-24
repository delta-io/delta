package io.delta.dsv2.read

import io.delta.dsv2.utils.{ExpressionUtils, SchemaUtils}
import io.delta.kernel.{Table => KernelTable}
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.kernel.expressions.{And => KernelAnd}
import io.delta.kernel.internal.ScanImpl
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.StructType

class DeltaScanBuilder(kernelTable: KernelTable, tableEngine: KernelEngine)
    extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownV2Filters {
  import DeltaScanBuilder._

  private val readSnapshot = kernelTable.getLatestSnapshot(tableEngine)
  private var scanBuilder = readSnapshot.getScanBuilder()
  private var sparkSchema =
    SchemaUtils.convertKernelSchemaToSparkSchema(readSnapshot.getSchema())
  private var pushedSparkPredicates = Array.empty[Predicate]

  logger.info(
    s"Constructed DeltaScanBuilder for ${kernelTable.getPath(tableEngine)} at read " +
      s"version ${readSnapshot.getVersion()}")

  /**
   * Data sources can implement this interface to push down required columns to the data source
   * and only read these columns during scan to reduce the size of the data to be read.
   *
   * Applies column pruning w.r.t. the given requiredSchema.
   */
  override def pruneColumns(requiredSchema: StructType): Unit = {
    // TODO: verify that requiredSchema is a subset of the table schema
    logger.info(s"Pruning columns for required schema: $requiredSchema")

    sparkSchema = requiredSchema
    scanBuilder = scanBuilder.withReadSchema(
      SchemaUtils.convertSparkSchemaToKernelSchema(sparkSchema))
  }

  /**
   * Data sources can implement this interface to push down V2 Predicate to the data source and
   * reduce the size of the data to be read.
   *
   * Pushes down predicates, and returns predicates that need to be evaluated after scanning. Rows
   * should be returned from the data source if and only if all of the predicates match. That is,
   * predicates must be interpreted as ANDed together.
   */
  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    logger.info(s"pushPredicates(): predicates=${predicates.mkString("Array(", ", ", ")")}")
    predicates.foreach(p => logger.info(s"predicate $p, name: ${p.name()}"))

    val sparkToKernelPredicates =
      predicates.map(p => p -> ExpressionUtils.convertStoKPredicate(p)).collect {
        case (sparkPredicate, Some(kernelPredicate)) => sparkPredicate -> kernelPredicate
      }

    logger.info(s"input predicates size: ${predicates.size}")
    logger.info(s"converted predicates size: ${sparkToKernelPredicates.size}")

    if (predicates.length != sparkToKernelPredicates.length) {
      logger.warn("Some predicates could not be converted to kernel predicates")
    }

    logger.info(s"sparkToKernelPredicatesMap: ${sparkToKernelPredicates.mkString(", ")}")

    val kernelAndOpt = sparkToKernelPredicates
      .map(_._2)
      .reduceOption((left, right) => new KernelAnd(left, right))

    logger.info(s"Pushing down predicates: $kernelAndOpt")

    if (kernelAndOpt.nonEmpty) {
      scanBuilder = scanBuilder.withFilter(kernelAndOpt.get)
      val scan = scanBuilder.build()
      val kernelPushedOpt = scan.asInstanceOf[ScanImpl].getPartitionsFilters()
      val kernelRemainingOpt = scan.getRemainingFilter

      logger.info(s"kernelPushedOpt: $kernelPushedOpt")
      logger.info(s"kernelRemainingOpt: $kernelRemainingOpt")

      if (kernelPushedOpt.isPresent) {
        logger.info("kernelPushedOpt is non-empty")

        ExpressionUtils.convertKtoSPredicate(kernelPushedOpt.get()).foreach { pushed =>
          pushedSparkPredicates = Array(pushed)
        }
      }

      if (kernelRemainingOpt.isPresent) {
        logger.info("kernelRemainingOpt is non-empty")

        val sparkRemainingOpt = ExpressionUtils.convertKtoSPredicate(kernelRemainingOpt.get())
        logger.info(s"sparkRemainingOpt: ${sparkRemainingOpt.toArray.mkString(", ")}")

        sparkRemainingOpt.toArray
      } else {
        Array.empty
      }
    } else {
      logger.info("No pushable predicates found")
      predicates
    }
  }

  /**
   * Returns the predicates that are pushed to the data source via [[pushPredicates]].
   *
   * There are 3 kinds of predicates:
   *   - pushable predicates which don't need to be evaluated again after scanning.
   *   - pushable predicates which still need to be evaluated after scanning, e.g. parquet row
   *     group predicate.
   *   - non-pushable predicates.
   *
   * Both case 1 and 2 should be considered as pushed predicates and should be returned by this
   * method.
   *
   * It's possible that there is no predicates in the query and [[pushPredicates]] is never
   * called, empty array should be returned for this case.
   */
  override def pushedPredicates(): Array[Predicate] = {
    logger.info(s"pushedPredicates(): ${pushedSparkPredicates.mkString("Array(", ", ", ")")}")
    pushedSparkPredicates
  }

  override def build(): Scan = {
    new DeltaScan(scanBuilder.build(), tableEngine, sparkSchema)
  }
}

object DeltaScanBuilder {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}
