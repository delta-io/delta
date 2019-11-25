package io.delta.hive;

import com.google.common.base.Joiner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import scala.collection.JavaConverters;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.sql.delta.DeltaHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaRecordReaderWrapper extends ParquetRecordReaderWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaRecordReaderWrapper.class);

    private Writable[] partitionWritable = null;

    public DeltaRecordReaderWrapper(ParquetInputFormat<ArrayWritable> newInputFormat, InputSplit oldSplit, JobConf oldJobConf, Reporter reporter) throws IOException, InterruptedException {
        super(newInputFormat, oldSplit, oldJobConf, reporter);

        if (!(oldSplit instanceof FileSplit)) {
            throw new IllegalArgumentException("Unknown split type: " + oldSplit);
        } else {
            final String columnNameProperty = oldJobConf.get(DeltaStorageHandler.DELTA_PARTITION_COLS_NAMES);
            final String columnTypeProperty = oldJobConf.get(DeltaStorageHandler.DELTA_PARTITION_COLS_TYPES);
            LOG.info("Delta partition cols: " + columnNameProperty + " with types: " + columnTypeProperty);

            if (columnNameProperty == null || columnNameProperty.trim().length() == 0
                    || columnTypeProperty == null || columnTypeProperty.trim().length() == 0) {
                LOG.info("No partition info is provided...");
            } else {
                // generate partition writale values which will be appended after data values from parquet
                final List<String> columnNames;
                final List<String> columnTypes;
                if (columnNameProperty.length() == 0) {
                    columnNames = new ArrayList<String>();
                } else {
                    columnNames = Arrays.asList(columnNameProperty.split(","));
                }
                if (columnTypeProperty.length() == 0) {
                    columnTypes = new ArrayList<String>();
                } else {
                    columnTypes = Arrays.asList(columnTypeProperty.split(":"));
                }

                Path filePath = ((FileSplit)oldSplit).getPath();

                Map<String, String> parsedPartitions = JavaConverters.mapAsJavaMap(
                        DeltaHelper.parsePathPartition(filePath, JavaConverters.asScalaBufferConverter(columnNames).asScala().toSeq()));

                partitionWritable = new Writable[columnNames.size()];
                // inspect partition values
                for(int i=0; i < columnNames.size(); i++) {
                    ObjectInspector oi = PrimitiveObjectInspectorFactory
                            .getPrimitiveWritableObjectInspector(TypeInfoFactory
                                    .getPrimitiveTypeInfo(columnTypes.get(i)));

                    partitionWritable[i] = (Writable)ObjectInspectorConverters.getConverter(
                            PrimitiveObjectInspectorFactory.javaStringObjectInspector, oi).convert(parsedPartitions.get(columnNames.get(i)));
                }
                LOG.info("Parsed partition values from " + filePath.toString() + " list: " + Joiner.on(",").withKeyValueSeparator("=").join(parsedPartitions)
                    + ", partitionWritable length:" + partitionWritable.length);
            }
        }
    }

    @Override
    public boolean next(NullWritable key, ArrayWritable value) throws IOException {
        boolean hasNext = super.next(key, value);
        if (partitionWritable != null && partitionWritable.length != 0) {
            // append partition values to data values
            for (int i=0; i < partitionWritable.length; i++) {
                value.get()[value.get().length - partitionWritable.length + i] = partitionWritable[i];
            }
        }
        return hasNext;
    }

    @Override
    public ArrayWritable createValue() {
        ArrayWritable value = super.createValue();
        if (partitionWritable != null && partitionWritable.length != 0) {
            for (int i=0; i < partitionWritable.length; i++) {
                value.get()[value.get().length - partitionWritable.length + i] = partitionWritable[i];
            }
        }
        return value;
    }
}
