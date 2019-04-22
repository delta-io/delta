package org.apache.spark.sql.delta.sources

import java.util.Locale

object DeltaSourceUtils {
  val NAME = "tahoe"
  val ALT_NAME = "delta"

  // Batch relations don't pass partitioning columns to `CreatableRelationProvider`s, therefore
  // as a hack, we pass in the partitioning columns among the options.
  val PARTITIONING_COLUMNS_KEY = "__partition_columns"

  def isDeltaDataSourceName(name: String): Boolean = {
    name.toLowerCase(Locale.ROOT) == NAME || name.toLowerCase(Locale.ROOT) == ALT_NAME
  }

  /** Check whether this table is a Delta table based on information from the Catalog. */
  def isDeltaTable(provider: Option[String]): Boolean = {
    provider match {
      case Some(p) => isDeltaDataSourceName(p)
      case None => false
    }
  }
}
