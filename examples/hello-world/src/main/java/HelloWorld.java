/*
 * Copyright (2021) The Delta Lake Project Authors.
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
package example;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructType;

/**
 * Simple test class for delta-standalone JAR verification.
 * - Creates a delta table with partition columns.
 * - Commits {@code AddFile}s individually.
 * - Reads table metadata and verifies all {@code AddFile}s were committed as expected.
 *
 * To run with Maven:
 * - cd connectors/examples/hello-world
 * - mvn package exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass=example.HelloWorld
 *
 * To run with SBT:
 * - cd connectors/examples
 * - build/sbt "helloWorld/runMain example.HelloWorld"
 */
public class HelloWorld {
    public static void main(String[] args) throws IOException {
        File tmpDir = Files.createTempDirectory("my_table").toFile();
        String tmpDirPath = tmpDir.getAbsolutePath();

        try {
            final String engineInfo = "local";

            DeltaLog log = DeltaLog.forTable(new Configuration(), tmpDirPath);

            StructType schema = new StructType()
                    .add("foo", new IntegerType())
                    .add("bar", new IntegerType())
                    .add("zip", new StringType());

            List<String> partitionColumns = Arrays.asList("foo", "bar");

            Metadata metadata = Metadata.builder()
                    .schema(schema)
                    .partitionColumns(partitionColumns)
                    .build();

            Operation op = new Operation(Operation.Name.WRITE);

            for (int i = 0; i < 15; i++) {
                OptimisticTransaction txn = log.startTransaction();
                if (i == 0) {
                    txn.updateMetadata(metadata);
                }

                Map<String, String> partitionValues = new HashMap<>();
                partitionValues.put("foo", Integer.toString(i % 3));
                partitionValues.put("bar", Integer.toString(i % 2));

                long now = System.currentTimeMillis();

                AddFile addFile = AddFile.builder(Integer.toString(i), partitionValues, 100L, now, true)
                        .tags(Collections.singletonMap("someTagKey", "someTagVal"))
                        .build();

                txn.commit(Collections.singletonList(addFile), op, engineInfo);
            }

            DeltaLog log2 = DeltaLog.forTable(new Configuration(), tmpDirPath);
            Set<Integer> pathVals = log2.update()
                    .getAllFiles()
                    .stream()
                    .map(addFile -> Integer.parseInt(addFile.getPath()))
                    .collect(Collectors.toSet());

            for (int i = 0; i < 15; i++) {
                if (!pathVals.contains(i)) throw new RuntimeException();
            }

        } finally {
            FileUtils.deleteDirectory(tmpDir);
        }
    }
}
