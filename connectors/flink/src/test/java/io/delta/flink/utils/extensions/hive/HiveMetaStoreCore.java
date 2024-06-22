package io.delta.flink.utils.extensions.hive;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

/**
 * This class abstract out and hides Hive server and metastore client from end user making this
 * class as a main entry point for managing life cycle of Hive metastore.
 * <p>
 *
 * @implNote This class is based on from https://github.com/ExpediaGroup/beeju/blob/beeju-5.0
 * .0/src/main/java/com/hotels/beeju/core/HiveMetaStoreCore.java
 * and "trimmed" to our needs. We could not use entire beeju library as sbt dependency due to
 * dependency conflicts with Flink on Calcite, Parquet and many others. See https://github
 * .com/ExpediaGroup/beeju/issues/54 for details. As a result we added only org .apache.hive
 * hive-exec and hive-metastore dependencies, and we used beeju's Junit5 extension classes.
 */
public class HiveMetaStoreCore {

    private final HiveServerContext hiveServerContext;

    private HiveMetaStoreClient client;

    public HiveMetaStoreCore(HiveServerContext hiveServerContext) {
        this.hiveServerContext = hiveServerContext;
    }

    public void initialise() throws InterruptedException, ExecutionException {
        HiveConf hiveConf = new HiveConf(hiveServerContext.conf(), HiveMetaStoreClient.class);
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
        try {
            client = singleThreadExecutor.submit(new CallableHiveClient(hiveConf)).get();
        } finally {
            singleThreadExecutor.shutdown();
        }
    }

    public void shutdown() {
        if (client != null) {
            client.close();
        }
    }

    /**
     * @return the {@link HiveMetaStoreClient} backed by an HSQLDB in-memory database.
     */
    public HiveMetaStoreClient client() {
        return client;
    }

    public static class CallableHiveClient implements Callable<HiveMetaStoreClient> {

        private final HiveConf hiveConf;

        CallableHiveClient(HiveConf hiveConf) {
            this.hiveConf = hiveConf;
        }

        @Override
        public HiveMetaStoreClient call() throws Exception {
            return new HiveMetaStoreClient(hiveConf);
        }
    }
}
