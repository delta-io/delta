/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.iceberg.unityCatalog;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;

import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
/**
 * UnityCatalog manages delta table with iceberg metadata conversion in unity catalog
 * Only newTableOps needs implementation as it is used to commit to Iceberg. All other methods are
 * not required (eg, listTables, dropTable, renameTable) because the tables are managed by
 * unity catalog outside of iceberg context.
 */
public class UnityCatalog extends BaseMetastoreCatalog implements Closeable, Configurable<Object> {
    private static final String DEFAULT_FILE_IO_IMPL = "org.apache.iceberg.hadoop.HadoopFileIO";

    // Injectable factory for testing purposes.
    static class FileIOFactory {
        public FileIO newFileIO(String impl, Map<String, String> properties, Object hadoopConf) {
            return CatalogUtil.loadFileIO(impl, properties, hadoopConf);
        }
    }

    private Object conf;

    private Map<String, String> catalogProperties;

    private FileIOFactory fileIOFactory;

    private CloseableGroup closeableGroup;

    private List<MetadataUpdate> metadataUpdates;

    // If set, all table option in this catalog will be built based on the snapshot baseMetadataLocation points to.
    private final Optional<String> baseMetadataLocation;
    // When true, Iceberg catalog update and Iceberg metadata written would be atomic
    // Otherwise, Iceberg metadata will be generated without updating the corresponding catalog
    private final boolean commitToUC;
    // When true, preserve Delta related table properties
    // Otherwise, remove delta table properties and name mapping to avoid leaking
    // implementation details (like for managed iceberg tables)
    private final boolean shouldPreserveDeltaProperties;

    /**
     * Creates a new UnityCatalog instance.
     *
     * @param metadataUpdates metadata update to be committed in the table operation
     * @param baseMetadataLocation Optional file path specifying the base Iceberg metadata location.
     *                            If present, new Iceberg table operations will be based on
     *                            metadata at this location. Should only be set when commitToUC
     *                            is false and table is present.
     * @param commitToUC Flag indicating whether changes should be committed to Unity Catalog.
     *                   When true, modifications are persisted to Unity Catalog;
     *                   when false, iceberg metadata file will be written but not tracked
     *                   by Unity Catalog
     */
    public UnityCatalog(List<MetadataUpdate> metadataUpdates,
                        Optional<String> baseMetadataLocation,
                        boolean commitToUC,
                        boolean shouldPreserveDeltaProperties) {
        this.metadataUpdates = metadataUpdates;
        this.baseMetadataLocation = baseMetadataLocation;
        this.commitToUC = commitToUC;
        this.shouldPreserveDeltaProperties = shouldPreserveDeltaProperties;
    }

    public UnityCatalog() {
        this.baseMetadataLocation = Optional.empty();
        this.commitToUC = true;
        this.shouldPreserveDeltaProperties = true;
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
        FileIO fileIO = fileIOFactory.newFileIO(DEFAULT_FILE_IO_IMPL, catalogProperties, conf);
        closeableGroup.addCloseable(fileIO);
        return new UnityCatalogTableOperations(
                fileIO, tableIdentifier, metadataUpdates, commitToUC, shouldPreserveDeltaProperties, baseMetadataLocation);
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
        this.catalogProperties = properties;
        this.fileIOFactory = new FileIOFactory();
        this.closeableGroup = new CloseableGroup();
    }

    @Override
    public void close() throws IOException {
        if (closeableGroup != null) {
            closeableGroup.close();
        }
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        throw new UnsupportedOperationException(
                "UnityCatalog does not currently support defaultWarehouseLocation");
    }

    @Override
    public void setConf(Object conf) {
        this.conf = conf;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        throw new UnsupportedOperationException("UnityCatalog does not currently support listTables");
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        throw new UnsupportedOperationException("UnityCatalog does not currently support dropTable");
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
        throw new UnsupportedOperationException("UnityCatalog does not currently support renameTable");
    }

    // If the given metadataLocation is invalid, we also see it as the table doesn't exist
    @Override
    public boolean tableExists(TableIdentifier identifier) {
        try {
            loadTable(identifier);
            return true;
        } catch (NoSuchTableException | NotFoundException e) {
            return false;
        }
    }
}
