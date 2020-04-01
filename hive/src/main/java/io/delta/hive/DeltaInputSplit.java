/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

/**
 * A special {@link FileSplit} that holds the corresponding partition information of the file.
 *
 * This file is written in Java because we need to call two different constructors of
 * {@link FileSplit} but Scala doesn't support it.
 */
public class DeltaInputSplit extends FileSplit {

    private PartitionColumnInfo[] partitionColumns;

    protected DeltaInputSplit() {
        super();
        partitionColumns = new PartitionColumnInfo[0];
    }

    public DeltaInputSplit(Path file, long start, long length, String[] hosts, PartitionColumnInfo[] partitionColumns) {
        super(file, start, length, hosts);
        this.partitionColumns = partitionColumns;
    }

    public DeltaInputSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts, PartitionColumnInfo[] partitionColumns) {
        super(file, start, length, hosts, inMemoryHosts);
        this.partitionColumns = partitionColumns;
    }

    public PartitionColumnInfo[] getPartitionColumns() {
        return partitionColumns;
    }

    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(partitionColumns.length);
        for (PartitionColumnInfo partitionColumn : partitionColumns) {
            partitionColumn.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int size = in.readInt();
        partitionColumns = new PartitionColumnInfo[size];
        for (int i = 0; i < size; i++) {
            PartitionColumnInfo partitionColumn = new PartitionColumnInfo();
            partitionColumn.readFields(in);
            partitionColumns[i] = partitionColumn;
        }
    }
}
