package io.delta.flink;

import org.apache.hadoop.conf.Configuration;

public class DeltaTestUtils {

    ///////////////////////////////////////////////////////////////////////////
    // hadoop conf test utils
    ///////////////////////////////////////////////////////////////////////////

    public static Configuration getHadoopConf() {
        Configuration conf = new Configuration();
        conf.set("parquet.compression", "SNAPPY");
        return conf;
    }

}
