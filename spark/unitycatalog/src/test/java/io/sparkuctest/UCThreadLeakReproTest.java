/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.sparkuctest;

import org.junit.jupiter.api.Test;

/**
 * Reproduces the AWS SDK v2 thread leak from CASE# 00843024.
 *
 * <p>Creates a Delta table with thousands of parquet files, then reads them to trigger credential
 * vending. With real S3, each vend leaks 5 {@code sdk-ScheduledExecutor} threads from an unclosed
 * AWS client.
 *
 * <p>Run against real S3 to observe the leak:
 *
 * <pre>
 *   UC_REMOTE=true UC_URI=... UC_TOKEN=... UC_CATALOG_NAME=... \
 *   UC_SCHEMA_NAME=... UC_BASE_TABLE_LOCATION=s3://... \
 *   sbt "sparkUnityCatalog/testOnly *UCThreadLeakReproTest"
 * </pre>
 */
public class UCThreadLeakReproTest extends UCDeltaTableIntegrationBaseTest {

  @Test
  public void testThreadLeakWithManyFiles() throws Exception {
    withNewTable(
        "thread_leak_repro",
        "id INT, payload STRING",
        TableType.EXTERNAL,
        tableName -> {
          // 1 row per parquet file -> 2000 files.
          sql("SET spark.sql.files.maxRecordsPerFile = 1");
          spark().range(2000).createOrReplaceTempView("source");
          sql(
              "INSERT INTO %s SELECT CAST(id AS INT), CONCAT('payload-', id) FROM source",
              tableName);

          long before = countSdkThreads();
          System.out.println("sdk-ScheduledExecutor threads before reads: " + before);

          for (int i = 0; i < 10; i++) {
            sql("SELECT count(*) FROM %s", tableName);
          }

          long after = countSdkThreads();
          long leaked = after - before;
          System.out.println("sdk-ScheduledExecutor threads after reads:  " + after);
          System.out.println("Leaked sdk-ScheduledExecutor threads:       " + leaked);

          if (leaked > 10) {
            System.out.println(
                "THREAD LEAK DETECTED: "
                    + leaked
                    + " threads — each 5 = one unclosed AWS SDK v2 client.");
          }

          sql("SET spark.sql.files.maxRecordsPerFile = 0");
        });
  }

  private long countSdkThreads() {
    Thread[] threads = new Thread[Thread.activeCount() * 2];
    int n = Thread.enumerate(threads);
    long count = 0;
    for (int i = 0; i < n; i++) {
      if (threads[i] != null && threads[i].getName().startsWith("sdk-ScheduledExecutor")) {
        count++;
      }
    }
    return count;
  }
}
