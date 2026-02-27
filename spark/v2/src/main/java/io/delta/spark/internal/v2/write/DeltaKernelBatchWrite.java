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
package io.delta.spark.internal.v2.write;

import io.delta.kernel.Operation;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

/**
 * DSv2 BatchWrite for append-only writes using the Delta Kernel Transaction API. Creates the
 * transaction on the driver, passes serialized transaction state to executor tasks via {@link
 * DeltaKernelDataWriterFactory}, and commits on the driver when all tasks succeed.
 */
public class DeltaKernelBatchWrite extends AbstractDeltaKernelBatchWrite {

  public DeltaKernelBatchWrite(
      String tablePath,
      Configuration hadoopConf,
      io.delta.kernel.Snapshot initialSnapshot,
      StructType writeSchema,
      String queryId,
      Map<String, String> options,
      List<String> partitionColumnNames) {
    super(tablePath, hadoopConf, initialSnapshot, Operation.WRITE, options, partitionColumnNames);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    commitActions(collectAddActions(messages));
  }
}
