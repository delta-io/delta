package io.delta.flink.source;

import java.util.Arrays;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;

public class SourceExamples {

    /**
     * Delta Flink Source for bounded mode, that should read all columns from Delta's table row.
     */
    public void builderBoundedAllColumns() {
        Configuration hadoopConf = new Configuration();

        DeltaSource<RowData> source = DeltaSource.forBoundedRowData(
                new Path("s3://some/path"),
                hadoopConf
            )
            .build();
    }

    /**
     * Delta Flink Source for bounded mode, that should read only columns defined by user.
     */
    public void builderBoundedUserSelectedColumns() {
        Configuration hadoopConf = new Configuration();

        DeltaSource<RowData> source = DeltaSource.forBoundedRowData(
                new Path("s3://some/path"),
                hadoopConf
            )
            .columnNames(Arrays.asList("col1", "col2"))
            .build();
    }

    /**
     * Delta Flink Source for continuous mode, that should read all columns from Delta's table row.
     */
    public void builderContinuousAllColumns() {
        Configuration hadoopConf = new Configuration();

        DeltaSource<RowData> source = DeltaSource.forContinuousRowData(
                new Path("s3://some/path"),
                hadoopConf
            )
            .build();
    }

    /**
     * Delta Flink Source for bounded mode, using extra, public options.
     */
    public void builderBoundedPublicOption() {
        Configuration hadoopConf = new Configuration();

        DeltaSource<RowData> source = DeltaSource.forBoundedRowData(
                new Path("s3://some/path"),
                hadoopConf
            )
            .versionAsOf(10)
            .build();
    }

    /**
     * Delta Flink Source for continuous mode, using extra, public options.
     */
    public void builderContinuousPublicOption() {
        Configuration hadoopConf = new Configuration();

        DeltaSource<RowData> source = DeltaSource.forContinuousRowData(
                new Path("s3://some/path"),
                hadoopConf
            )
            .updateCheckIntervalMillis(1000)
            .startingVersion(10)
            .build();
    }
}
