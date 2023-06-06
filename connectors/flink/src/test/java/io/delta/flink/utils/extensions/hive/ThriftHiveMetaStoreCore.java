package io.delta.flink.utils.extensions.hive;

import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge23;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @implNote This class is based on https://github.com/ExpediaGroup/beeju/blob/beeju-5.0
 * .0/src/main/java/com/hotels/beeju/core/ThriftHiveMetaStoreCore.java
 * and "trimmed" to our needs. We could not use entire beeju library as sbt dependency due to
 * Dependency conflicts with Flink on Calcite, Parquet and many others. See https://github
 * .com/ExpediaGroup/beeju/issues/54 for details. As a result we added only org .apache.hive
 * hive-exec and hive-metastore dependencies, and we used beeju's Junit5 extension classes.
 */
public class ThriftHiveMetaStoreCore {

    private static final Logger LOG = LoggerFactory.getLogger(ThriftHiveMetaStoreCore.class);

    private final ExecutorService thriftServer;

    private final HiveServerContext hiveServerContext;

    private int thriftPort = -1;

    public ThriftHiveMetaStoreCore(HiveServerContext hiveServerContext) {
        this.hiveServerContext = hiveServerContext;
        thriftServer = Executors.newSingleThreadExecutor();
    }

    public void initialise() throws Exception {
        final Lock startLock = new ReentrantLock();
        final Condition startCondition = startLock.newCondition();
        final AtomicBoolean startedServing = new AtomicBoolean();

        int socketPort = Math.max(thriftPort, 0);

        try (ServerSocket socket = new ServerSocket(socketPort)) {
            thriftPort = socket.getLocalPort();
        }

        hiveServerContext.setHiveVar(HiveConf.ConfVars.METASTOREURIS, getThriftConnectionUri());

        final HiveConf hiveConf = new HiveConf(hiveServerContext.conf(), HiveMetaStoreClient.class);

        thriftServer.execute(() -> {
            try {
                HadoopThriftAuthBridge bridge = HadoopThriftAuthBridge23.getBridge();
                HiveMetaStore.startMetaStore(
                    thriftPort,
                    bridge,
                    hiveConf,
                    startLock,
                    startCondition,
                    startedServing
                );
            } catch (Throwable e) {
                LOG.error("Unable to start a Thrift server for Hive Metastore", e);
            }
        });
        int i = 0;
        while (i++ < 3) {
            startLock.lock();
            try {
                if (startCondition.await(1, TimeUnit.MINUTES)) {
                    break;
                }
            } finally {
                startLock.unlock();
            }
            if (i == 3) {
                throw new RuntimeException(
                    "Maximum number of tries reached whilst waiting for Thrift server to be ready");
            }
        }
    }

    public void shutdown() {
        thriftServer.shutdown();
    }

    /**
     * @return The Thrift connection string for the Metastore service.
     */
    public String getThriftConnectionUri() {
        return "thrift://localhost:" + thriftPort;
    }

    /**
     * @return The port used for the Thrift Metastore service.
     */
    public int getThriftPort() {
        return thriftPort;
    }

    /**
     * @param thriftPort The Port to use for the Thrift Hive metastore, if not set then a port
     *                   number will automatically be allocated.
     */
    public void setThriftPort(int thriftPort) {
        if (thriftPort < 0) {
            throw new IllegalArgumentException("Thrift port must be >=0, not " + thriftPort);
        }
        this.thriftPort = thriftPort;
    }
}

