package io.delta.flink.utils.extensions;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import io.delta.flink.utils.extensions.hive.ThriftHiveMetaStoreJUnitExtension;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Junit test extension that setup Delta Catalog with 'Hive' metastore.
 */
public class HiveCatalogExtension extends BaseCatalogExtension {

    private static final Logger LOG = LoggerFactory.getLogger(HiveCatalogExtension.class);

    protected final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private final ThriftHiveMetaStoreJUnitExtension hiveMetaStoreJUnitExtension =
        new ThriftHiveMetaStoreJUnitExtension("testDb");

    private String hadoopConfDir;

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        hiveMetaStoreJUnitExtension.beforeEach(context);
        TEMPORARY_FOLDER.create();
        prepareHiveConf();
        LOG.info("Hive Stub configuration folder " + this.hadoopConfDir);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        hiveMetaStoreJUnitExtension.afterEach(context);
        TEMPORARY_FOLDER.delete();

        boolean delete = new File(hadoopConfDir + "/core-site.xml").delete();
        if (!delete) {
            LOG.info(
                "Hive catalog config file was nto deleted: " + hadoopConfDir + "/core-site.xml");
        }
    }

    @Override
    public void setupDeltaCatalog(TableEnvironment tableEnv) {
        String catalogSQL = String.format(""
            + "CREATE CATALOG myDeltaHiveCatalog"
            + " WITH ("
            + "'type' = 'delta-catalog',"
            + "'catalog-type' = 'hive',"
            + "'hadoop-conf-dir' = '%s'"
            + ");", this.hadoopConfDir);

        String useDeltaCatalog = "USE CATALOG myDeltaHiveCatalog;";

        tableEnv.executeSql(catalogSQL);
        tableEnv.executeSql(useDeltaCatalog);
    }

    private void prepareHiveConf() {
        try {
            String thriftConnectionUri = hiveMetaStoreJUnitExtension.getThriftConnectionUri();

            String path = "src/test/resources/hive/core-site.xml";
            File file = new File(path);
            List<String> strings = Files.readAllLines(file.toPath(), Charset.defaultCharset());
            strings.replaceAll(s -> s.replace("CHANGE_THIS", thriftConnectionUri));

            File hiveSite = TEMPORARY_FOLDER.newFile("core-site.xml");
            Path write = Files.write(hiveSite.toPath(), strings, StandardOpenOption.WRITE);
            this.hadoopConfDir = write.getParent().toUri().getPath();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
