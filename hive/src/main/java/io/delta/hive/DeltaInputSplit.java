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
