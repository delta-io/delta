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
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import scala.collection.JavaConverters;

/**
 * Runtime utilities for accessing Spark functionality that may not be available at compile time in
 * kernel-only environments.
 */
public class SparkRuntimeUtils {

  /**
   * Get FilePartitions using reflection to avoid compile-time SparkSession dependency. This allows
   * the code to compile in kernel-only mode but still work at runtime.
   */
  public static InputPartition[] getFilePartitions(
      List<PartitionedFile> partitionedFiles, long maxSplitBytes) {
    try {
      // Use reflection to get SparkSession.active()
      Class<?> sparkSessionClass = Class.forName("org.apache.spark.SparkSession");
      Method activeMethod = sparkSessionClass.getMethod("active");
      Object sparkSession = activeMethod.invoke(null);

      // Use reflection to call FilePartition$.MODULE$.getFilePartitions()
      Class<?> filePartitionClass =
          Class.forName("org.apache.spark.sql.execution.datasources.FilePartition$");
      Object filePartitionModule = filePartitionClass.getField("MODULE$").get(null);
      Method getFilePartitionsMethod =
          filePartitionClass.getMethod(
              "getFilePartitions", Object.class, scala.collection.Seq.class, long.class);

      // Convert List<PartitionedFile> to Scala Seq
      scala.collection.Seq<PartitionedFile> scalaSeq =
          JavaConverters.asScalaBuffer(partitionedFiles).toSeq();

      // Call getFilePartitions
      Object filePartitionsSeq =
          getFilePartitionsMethod.invoke(
              filePartitionModule, sparkSession, scalaSeq, maxSplitBytes);

      // Convert back to Java array
      scala.collection.Seq<?> resultSeq = (scala.collection.Seq<?>) filePartitionsSeq;
      List<?> resultList = JavaConverters.seqAsJavaList(resultSeq);

      return resultList.toArray(new InputPartition[0]);

    } catch (Exception e) {
      // Fallback: create simple partitions (one per file)
      return createSimplePartitions(partitionedFiles);
    }
  }

  /** Get SparkSession using reflection to avoid compile-time dependency. */
  public static Object getActiveSparkSession() {
    try {
      Class<?> sparkSessionClass = Class.forName("org.apache.spark.SparkSession");
      Method activeMethod = sparkSessionClass.getMethod("active");
      return activeMethod.invoke(null);
    } catch (Exception e) {
      throw new RuntimeException("SparkSession not available at runtime", e);
    }
  }

  /** Fallback method to create simple partitions when Spark classes are not available. */
  private static InputPartition[] createSimplePartitions(List<PartitionedFile> partitionedFiles) {
    return partitionedFiles.stream()
        .map(file -> new SimpleInputPartition(file))
        .toArray(InputPartition[]::new);
  }

  /** Simple InputPartition implementation for fallback scenarios. */
  private static class SimpleInputPartition implements InputPartition {
    private final PartitionedFile file;

    public SimpleInputPartition(PartitionedFile file) {
      this.file = file;
    }

    public PartitionedFile getFile() {
      return file;
    }

    @Override
    public String[] preferredLocations() {
      return new String[0];
    }
  }
}
