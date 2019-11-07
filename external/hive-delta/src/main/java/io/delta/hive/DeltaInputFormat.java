package io.delta.hive;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.sql.delta.DeltaHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaInputFormat extends FileInputFormat<NullWritable, ArrayWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(MapredParquetInputFormat.class);
    private final ParquetInputFormat<ArrayWritable> realInput;

    public DeltaInputFormat() {
        this(new ParquetInputFormat<ArrayWritable>(DataWritableReadSupport.class));
    }

    protected DeltaInputFormat(ParquetInputFormat<ArrayWritable> inputFormat) {
        this.realInput = inputFormat;
    }

    @Override
    public RecordReader<NullWritable, ArrayWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        try {
            if (Utilities.getUseVectorizedInputFileFormat(job)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Using vectorized record reader");
                }
                throw new IOException("Currently not support Delta VectorizedReader");
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Using row-mode record reader");
                }
                return new DeltaRecordReaderWrapper(this.realInput, split, job, reporter);
            }

        } catch (InterruptedException var5) {
            throw new RuntimeException("Cannot create a RecordReaderWrapper", var5);
        }
    }

    @Override
    protected FileStatus[] listStatus(JobConf job) throws IOException {
        Path deltaRootPath = new Path(job.get(DeltaStorageHandler.DELTA_TABLE_PATH));
        TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[]{deltaRootPath}, job);

        List<FileStatus> filteredDeltaFiles = DeltaHelper.listDeltaFiles(deltaRootPath, job);
        return filteredDeltaFiles.toArray(new FileStatus[filteredDeltaFiles.size()]);
    }
}
