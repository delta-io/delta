package org.apache.spark.sql.delta

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression, Literal}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.util.PartitionUtils

object DeltaHelper {

  def resolvePathFilters(snapshot: Snapshot, partitionFragments: Seq[String]): Seq[Expression] = {
    val metadata = snapshot.metadata
    val partitionFilters = partitionFragments.map { fragment =>
      val partitions = try {
        PartitionUtils.parsePathFragmentAsSeq(fragment)
      } catch {
        case _: ArrayIndexOutOfBoundsException =>
          throw DeltaErrors.partitionPathParseException(fragment)
      }

      val badColumns = partitions.map(_._1).filterNot(metadata.partitionColumns.contains)
      if (badColumns.nonEmpty) {
        throw DeltaErrors.partitionPathInvolvesNonPartitionColumnException(badColumns, fragment)
      }

      partitions.map { case (key, value) =>
        EqualTo(UnresolvedAttribute(key), Literal(value))
      }.reduce(And)
    }

    import org.apache.spark.sql.delta.actions.SingleAction._
    val files = DeltaLog.filterFileList(
      metadata.partitionColumns, snapshot.allFiles.toDF(), partitionFilters).as[AddFile].collect()
    if (files.length == 0) {
      throw DeltaErrors.pathNotExistsException(partitionFragments.mkString(","))
    }
    partitionFilters
  }
}
