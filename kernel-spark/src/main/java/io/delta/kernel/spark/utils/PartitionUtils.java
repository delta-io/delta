/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.spark.utils;

import java.lang.reflect.Method;
import java.util.List;
import org.apache.spark.sql.execution.datasources.PartitionedFile;

/** Utility class for partition-related operations shared across Delta Kernel Spark components. */
public class PartitionUtils {

  /**
   * Calculate the maximum split bytes for file partitioning, considering total bytes and file
   * count. This is used for optimal file splitting in both batch and streaming scenarios. Uses
   * reflection to access Spark classes to avoid compile-time dependencies.
   *
   * @param sparkSession the Spark session (accessed via reflection)
   * @param totalBytes total bytes across all files
   * @param fileCount number of files
   * @return maximum split bytes for partitioning
   */
  public static long calculateMaxSplitBytes(Object sparkSession, long totalBytes, int fileCount) {
    try {
      // Use reflection to access SQLConf.get()
      Class<?> sqlConfClass = Class.forName("org.apache.spark.sql.internal.SQLConf");
      Method getMethod = sqlConfClass.getMethod("get");
      Object sqlConf = getMethod.invoke(null);

      // Get configuration values using reflection
      Method filesMaxPartitionBytesMethod = sqlConfClass.getMethod("filesMaxPartitionBytes");
      long defaultMaxSplitBytes = (Long) filesMaxPartitionBytesMethod.invoke(sqlConf);

      Method filesOpenCostInBytesMethod = sqlConfClass.getMethod("filesOpenCostInBytes");
      long openCostInBytes = (Long) filesOpenCostInBytesMethod.invoke(sqlConf);

      Method filesMinPartitionNumMethod = sqlConfClass.getMethod("filesMinPartitionNum");
      Object minPartitionNumOption = filesMinPartitionNumMethod.invoke(sqlConf);

      int minPartitionNum;
      // Check if Option is defined using reflection
      Class<?> optionClass = minPartitionNumOption.getClass();
      Method isDefinedMethod = optionClass.getMethod("isDefined");
      boolean isDefined = (Boolean) isDefinedMethod.invoke(minPartitionNumOption);

      if (isDefined) {
        Method getOptionMethod = optionClass.getMethod("get");
        Object value = getOptionMethod.invoke(minPartitionNumOption);
        minPartitionNum = ((Number) value).intValue();
      } else {
        // Use sparkSession.leafNodeDefaultParallelism() via reflection
        Method leafNodeDefaultParallelismMethod =
            sparkSession.getClass().getMethod("leafNodeDefaultParallelism");
        minPartitionNum = (Integer) leafNodeDefaultParallelismMethod.invoke(sparkSession);
      }

      if (minPartitionNum <= 0) {
        minPartitionNum = 1;
      }

      long calculatedTotalBytes = totalBytes + (long) fileCount * openCostInBytes;
      long bytesPerCore = calculatedTotalBytes / minPartitionNum;

      return Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore));

    } catch (Exception e) {
      // Fallback to simple calculation
      return calculateMaxSplitBytesWithFallback();
    }
  }

  /**
   * Calculate the maximum split bytes for file partitioning using file list. Convenience method
   * that calculates total bytes from the file list.
   *
   * @param sparkSession the Spark session (accessed via reflection)
   * @param partitionedFiles list of partitioned files
   * @return maximum split bytes for partitioning
   */
  public static long calculateMaxSplitBytes(
      Object sparkSession, List<PartitionedFile> partitionedFiles) {
    long totalBytes = partitionedFiles.stream().mapToLong(file -> file.length()).sum();
    return calculateMaxSplitBytes(sparkSession, totalBytes, partitionedFiles.size());
  }

  /**
   * Calculate the maximum split bytes with fallback for streaming scenarios where exact file info
   * may not be available.
   *
   * @return maximum split bytes for partitioning (fallback version)
   */
  public static long calculateMaxSplitBytesWithFallback() {
    // For kernel-only compilation, use a reasonable default
    // At runtime when Spark classes are available, this could be enhanced
    return 128 * 1024 * 1024L; // 128MB default
  }
}
