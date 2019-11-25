package io.delta.hive;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class HiveInputFormat extends org.apache.hadoop.hive.ql.io.HiveInputFormat {
    @Override
    protected void pushProjectionsAndFilters(JobConf jobConf, Class inputFormatClass, Path splitPath, boolean nonNative) {
        if (inputFormatClass == DeltaInputFormat.class ) {
            super.pushProjectionsAndFilters(jobConf, inputFormatClass, splitPath, false);
        } else {
            super.pushProjectionsAndFilters(jobConf, inputFormatClass, splitPath, nonNative);
        }
    }
}
