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

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.*;

import com.databricks.sql.managedcatalog.DeltaUniformIceberg;
import org.apache.spark.sql.delta.UniformConcurrentModificationException;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;

/**
 * UnityCatalogTableOperations supports load and commit to Iceberg metadata through unity catalog
 */
public class UnityCatalogTableOperations extends BaseMetastoreTableOperations {

    private static final Logger LOG = LoggerFactory.getLogger(UnityCatalogTableOperations.class);

    /**
     * Property to be set in translated Iceberg metadata files.
     * Indicates the delta commit version # that it corresponds to.
     */
    public static final String DELTA_VERSION_PROPERTY = "delta-version";
    public static final String DELTA_TIMESTAMP_PROPERTY = "delta-timestamp";
    public static final String BASE_DELTA_VERSION_PROPERTY = "base-delta-version";
    public static final String DELTA_HIGH_WATER_MARK_PROPERTY = "delta-high-water-mark";

    public static final String DELTA_SQL_CONF_BYPASS_SEQUENCE_NUMBER_CHECK =
            "spark.databricks.delta.uniform.bypassSnapshotSequenceNumberCheck";
    public static final String DELTA_SQL_CONF_BYPASS_FORMAT_VERSION_DOWNGRAGDE_CHECK =
            "spark.databricks.delta.uniform.bypassFormatVersionDowngradeCheck";

    private final FileIO fileIO;
    private final TableIdentifier tableIdentifier;
    private CatalogTable currentSparkCatalogTable;
    private final SessionCatalog catalog;
    private final Optional<String> baseMetadataLocation;
    // When true, Iceberg catalog update and Iceberg metadata written would be atomic
    // Otherwise, Iceberg metadata will be generated without updating the corresponding catalog
    private final boolean commitToUC;
    // When true, preserve Delta related table properties
    // Otherwise, remove delta table properties and name mapping to avoid leaking
    // implementation details (like for managed iceberg tables)
    private final boolean shouldPreserveDeltaProperties;

    // Pre-assigned snapshot ID to be used for the next snapshot creation.
    // When set, this value will override the automatically generated snapshot ID
    // during the next commit operation. This allows for deterministic snapshot ID
    // generation for DBI based on delta version.
    private Optional<Long> preAssignedNextSnapshotId;
    private boolean preAssignedNextSnapshotIdConsumed;

    private List<MetadataUpdate> metadataUpdates;
    private Optional<Tuple2<String, TableMetadata>> lastWrittenTableMetadataWithLocation;

    /**
     * Creates a new UnityCatalogTableOperations instance for managing table operations.
     *
     * @param catalog The SessionCatalog backed by Unity Catalog
     * @param fileIO FileIO instance used for reading and writing iceberg metadata file
     * @param tableIdentifier Identifier for the table
     * @param metadataUpdates metadata update to be committed in the table operation
     * @param commitToUC Flag indicating whether changes should be committed to Unity Catalog.
     *                   When true, modifications are persisted to Unity Catalog;
     *                   when false, iceberg metadata file will be written but not tracked
     *                   by Unity Catalog
     * @param baseMetadataLocation Optional file path specifying the base Iceberg metadata location.
     *                            If present, new Iceberg table operations will be based on
     *                            metadata at this location. Should only be set when commitToUC
     *                            is false and table is present.
     */
    public UnityCatalogTableOperations(
            SessionCatalog catalog,
            FileIO fileIO,
            TableIdentifier tableIdentifier,
            List<MetadataUpdate> metadataUpdates,
            boolean commitToUC,
            boolean shouldPreserveDeltaProperties,
            Optional<String> baseMetadataLocation) {
        this.fileIO = fileIO;
        this.tableIdentifier = tableIdentifier;
        this.catalog = catalog;
        this.metadataUpdates = metadataUpdates;
        this.commitToUC = commitToUC;
        this.shouldPreserveDeltaProperties = shouldPreserveDeltaProperties;
        this.baseMetadataLocation = baseMetadataLocation;
        this.lastWrittenTableMetadataWithLocation = Optional.empty();
        this.preAssignedNextSnapshotId = Optional.empty();
        this.preAssignedNextSnapshotIdConsumed = false;
    }

    @Override
    public FileIO io() {
        return fileIO;
    }

    @Override
    protected String tableName() {
        return tableIdentifier.toString();
    }

    @Override
    public void doCommit(TableMetadata base, TableMetadata metadata) {
        TableMetadata.Builder builder = TableMetadata.buildFrom(metadata);

        // hasAddPartitionSpec indicates if the current commit attempt have added a new
        // partition spec through metadata update.
        boolean hasAddPartitionSpec = false;
        boolean initialPartitionSpecExistsAndIsPartitioned = false;
        for (PartitionSpec spec : metadata.specs()) {
            if (spec.specId() == 0 && spec.isPartitioned()) {
                initialPartitionSpecExistsAndIsPartitioned = true;
            }
        }
        Long deltaVersion = Long.parseLong(
                metadata.properties().get(DELTA_VERSION_PROPERTY));
        String baseDeltaVersionStr =
                metadata.properties().get(BASE_DELTA_VERSION_PROPERTY);

        String deltaHighWaterMarkStr =
                metadata.properties().get(DELTA_HIGH_WATER_MARK_PROPERTY);

        Schema lastAddedSchema = metadata.schema();
        for (MetadataUpdate update : metadataUpdates) {
            // iceberg-core reassigns field id in its schema when firstly creates table metadata;
            // we should always use the schema (with field ids assigned by delta) from MetadataUpdate
            // because the parquet data files are already written with field ids assigned by Delta.
            if (update instanceof MetadataUpdate.AddSchema) {
                MetadataUpdate.AddSchema addSchema = (MetadataUpdate.AddSchema) update;
                // lastColumnId must be monotonically increasing.
                builder.setCurrentSchema(
                  addSchema.schema(), Math.max(metadata.lastColumnId(), addSchema.lastColumnId()));
                lastAddedSchema = addSchema.schema();
            } else if (update instanceof MetadataUpdate.AddPartitionSpec) {
                // Use the partition spec from MetadataUpdate because the partition spec contains
                // the correct field ids assigned by Delta
                PartitionSpec specToAdd = ((MetadataUpdate.AddPartitionSpec) update).spec().bind(lastAddedSchema);
                if (!specToAdd.compatibleWith(metadata.spec())) {
                    builder.setDefaultPartitionSpec(specToAdd);
                    hasAddPartitionSpec = true;
                }
            } else {
                update.applyTo(builder);
            }
        }

        // Remove the initial partitioned partition spec (id=0) if new partition spec is added
        // because the initial partition spec has field ids assigned by iceberg-core
        // which may mismatch with field id assigned by Delta
        if (hasAddPartitionSpec && initialPartitionSpecExistsAndIsPartitioned) {
            MetadataUpdate.RemovePartitionSpecs removeSpecs = new MetadataUpdate.RemovePartitionSpecs(Set.of(0));
            removeSpecs.applyTo(builder);
        }

        // Remove delta table properties and name mapping for managed iceberg tables to avoid leaking
        // implementation details
        if (!shouldPreserveDeltaProperties) {
            Set<String> toRemove = new HashSet<>(Arrays.asList(DELTA_VERSION_PROPERTY,
                    BASE_DELTA_VERSION_PROPERTY, DELTA_TIMESTAMP_PROPERTY,
                    DELTA_HIGH_WATER_MARK_PROPERTY,
                    TableProperties.DEFAULT_NAME_MAPPING));
            builder.removeProperties(toRemove);
        }

        metadata = builder.build();

        if (base != current()) {
            throw new CommitFailedException("Cannot commit changes based on stale table metadata");
        }
        if (base == metadata) {
            LOG.info("Nothing to commit.");
            return;
        }


        final int newVersion = currentVersion() + 1;
        String newMetadataLocation = writeNewMetadata(metadata, newVersion);
        if (!commitToUC) {
            // Only cache currentTableMetadata and currentMetadataLocation on PREPARE_WITHOUT_COMMIT mode.
            // COMMIT mode should always look at Unity Catalog.
            lastWrittenTableMetadataWithLocation = Optional.of(Tuple2.apply(newMetadataLocation, metadata));
            return;
        }
    }

    @Override
    public TableMetadata refresh() {
        // during Delta to iceberg conversion, the table should always exist in catalog and the only
        // case with NoSuchTableException is the very first metadata conversion where iceberg
        // metadata and metadata location table prop does not exist yet.
        try {
            return super.refresh();
        } catch (NoSuchTableException e) {
            return null;
        }
    }

    @Override
    public void doRefresh() {
        LOG.debug("Getting metadata location for table {}", tableIdentifier);
        String location = loadTableMetadataLocation();
        Preconditions.checkState(
                location != null && !location.isEmpty(),
                "Got null or empty location %s for table %s",
                location,
                tableIdentifier);
        refreshFromMetadataLocation(location);
    }


    /**
     * Returns both the location of the Iceberg metadata file and its corresponding table metadata.
     * This information is only present when {@link commitToUC} is set to false.
     *
     * @return An Optional containing a tuple of:
     *         - String: The path where the metadata file was written
     *         - TableMetadata: The corresponding Iceberg table metadata
     *         Returns empty if no metadata has been written or if commitToUC is true
     */
    public Optional<Tuple2<String, TableMetadata>> getLastWrittenTableMetadataWithLocation() {
        return lastWrittenTableMetadataWithLocation;
    }

    private String loadTableMetadataLocation() {
        if (baseMetadataLocation.isPresent()) {
            return baseMetadataLocation.get();
        }
    }

}
