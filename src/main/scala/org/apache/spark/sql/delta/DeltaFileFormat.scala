package org.apache.spark.sql.delta

/*
 * Copyright (C) 2019 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

trait DeltaFileFormat {
  /** Return the underlying Spark `FileFormat` of the Delta table. */
  def fileFormat: FileFormat = new ParquetFileFormat()
}
