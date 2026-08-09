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

    public UnityCatalog(
        List<MetadataUpdate> metadataUpdates
        , Optional<String> baseMetadataLocation
    ) {
        this.metadataUpdates = metadataUpdates;
        this.baseMetadataLocation = baseMetadataLocation;
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
        FileIO fileIO = fileIOFactory.newFileIO(DEFAULT_FILE_IO_IMPL, catalogProperties, conf);
        closeableGroup.addCloseable(fileIO);
        return new UnityCatalogTableOperations(
                fileIO
                , tableIdentifier
                , metadataUpdates
                , baseMetadataLocation
        );
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
