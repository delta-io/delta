package io.delta.flink.internal.table;

import java.util.Collections;
import java.util.List;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.hadoop.conf.Configuration;

/**
 * A proxy class that redirects calls to Delta Catalog or decorated catalog depending on table type.
 */
public class CatalogProxy extends BaseCatalog {

    private final DeltaCatalog deltaCatalog;

    public CatalogProxy(
            String catalogName,
            String defaultDatabase,
            Catalog decoratedCatalog,
            Configuration hadoopConfiguration) {
        super(catalogName, defaultDatabase, decoratedCatalog);

        this.deltaCatalog = new DeltaCatalog(catalogName, decoratedCatalog, hadoopConfiguration);
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException {
        DeltaCatalogBaseTable catalogTable = getCatalogTableUnchecked(tablePath);
        if (catalogTable.isDeltaTable()) {
            return this.deltaCatalog.getTable(catalogTable);
        } else {
            return catalogTable.getCatalogTable();
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {

        DeltaCatalogBaseTable catalogTable = getCatalogTable(tablePath);
        if (catalogTable.isDeltaTable()) {
            return this.deltaCatalog.tableExists(catalogTable);
        } else {
            return this.decoratedCatalog.tableExists(tablePath);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
        throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

        DeltaCatalogBaseTable catalogTable = new DeltaCatalogBaseTable(tablePath, table);
        if (catalogTable.isDeltaTable()) {
            this.deltaCatalog.createTable(catalogTable, ignoreIfExists);
        } else {
            this.decoratedCatalog.createTable(tablePath, table, ignoreIfExists);
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
        throws TableNotExistException, CatalogException {

        DeltaCatalogBaseTable catalogTable = getCatalogTable(tablePath);
        if (catalogTable.isDeltaTable()) {
            this.deltaCatalog.dropTable(catalogTable, ignoreIfNotExists);
        } else {
            this.decoratedCatalog.dropTable(tablePath, ignoreIfNotExists);
        }
    }

    @Override
    public void alterTable(
            ObjectPath tablePath,
            CatalogBaseTable newTable,
            boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

        DeltaCatalogBaseTable newCatalogTable = new DeltaCatalogBaseTable(tablePath, newTable);
        if (newCatalogTable.isDeltaTable()) {
            this.deltaCatalog.alterTable(newCatalogTable);
        } else {
            this.decoratedCatalog.alterTable(tablePath, newTable, ignoreIfNotExists);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
        throws TableNotExistException, TableNotPartitionedException, CatalogException {

        DeltaCatalogBaseTable catalogTable = getCatalogTable(tablePath);
        if (catalogTable.isDeltaTable()) {
            // Delta standalone Metadata does not provide information about partition value.
            // This information is needed to build CatalogPartitionSpec
            // However, to make SELECT queries with partition column filter to work, we cannot throw
            // an exception here, since this method will be called by flink-table planner.
            return Collections.emptyList();
        } else {
            return this.decoratedCatalog.listPartitions(tablePath);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec)
        throws CatalogException, TableNotPartitionedException, TableNotExistException,
        PartitionSpecInvalidException {

        DeltaCatalogBaseTable catalogTable = getCatalogTable(tablePath);
        if (catalogTable.isDeltaTable()) {
            // Delta standalone Metadata does not provide information about partition value.
            // This information is needed to build CatalogPartitionSpec
            throw new CatalogException(
                "Delta table connector does not support partition listing.");
        } else {
            return this.decoratedCatalog.listPartitions(tablePath, partitionSpec);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath,
            List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {

        DeltaCatalogBaseTable catalogTable = getCatalogTable(tablePath);
        if (catalogTable.isDeltaTable()) {
            // Delta standalone Metadata does not provide information about partition value.
            // This information is needed to build CatalogPartitionSpec.

            // When implementing SupportsPartitionPushDown on DeltaDynamicTableSource, both
            // SupportsPartitionPushDown::listPartitions() and this method here should return
            // empty optional/empty list. The plan for Delta connector is to trick the planner
            // into thinking the table is unpartitioned, which will force it to treat partition
            // columns as data columns. This allows us to not list all the partitions in the
            // table (on which we would apply this filter). Then we will get a data filter that
            // we can apply to the scan we use to start reading from the delta log.
            throw new CatalogException(
                "Delta table connector does not support partition listing by filter.");
        } else {
            return this.decoratedCatalog.listPartitionsByFilter(tablePath, filters);
        }
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
        throws PartitionNotExistException, CatalogException {

        DeltaCatalogBaseTable catalogTable = getCatalogTable(tablePath);
        if (catalogTable.isDeltaTable()) {
            // Delta standalone Metadata does not provide information about partition value.
            // This information is needed to build CatalogPartitionSpec
            throw new CatalogException(
                "Delta table connector does not support partition listing.");
        } else {
            return this.decoratedCatalog.getPartition(tablePath, partitionSpec);
        }
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
        throws CatalogException {

        DeltaCatalogBaseTable catalogTable = getCatalogTable(tablePath);
        if (catalogTable.isDeltaTable()) {
            // Delta standalone Metadata does not provide information about partition value.
            // This information is needed to build CatalogPartitionSpec
            throw new CatalogException(
                "Delta table connector does not support partition listing.");
        } else {
            return this.decoratedCatalog.partitionExists(tablePath, partitionSpec);
        }
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists) throws TableNotExistException, TableNotPartitionedException,
        PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {

        DeltaCatalogBaseTable catalogTable = getCatalogTable(tablePath);
        if (catalogTable.isDeltaTable()) {
            throw new CatalogException(
                "Delta table connector does not support partition creation.");
        } else {
            this.decoratedCatalog.createPartition(
                tablePath,
                partitionSpec,
                partition,
                ignoreIfExists
            );
        }
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

        DeltaCatalogBaseTable catalogTable = getCatalogTable(tablePath);
        if (catalogTable.isDeltaTable()) {
            throw new CatalogException(
                "Delta table connector does not support partition drop operation.");
        } else {
            this.decoratedCatalog.dropPartition(
                tablePath,
                partitionSpec,
                ignoreIfNotExists
            );
        }
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

        DeltaCatalogBaseTable catalogTable = getCatalogTable(tablePath);
        if (catalogTable.isDeltaTable()) {
            throw new CatalogException(
                "Delta table connector does not support alter partition operation.");
        } else {
            this.decoratedCatalog.alterPartition(
                tablePath,
                partitionSpec,
                newPartition,
                ignoreIfNotExists
            );
        }
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
        throws TableNotExistException, CatalogException {

        if (getCatalogTable(tablePath).isDeltaTable()) {
            // Table statistic call is used by flink-table-planner module to get Table schema, so
            // we cannot throw from this method.
            return CatalogTableStatistics.UNKNOWN;
        } else {
            return this.decoratedCatalog.getTableStatistics(tablePath);
        }
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
        throws TableNotExistException, CatalogException {

        if (getCatalogTable(tablePath).isDeltaTable()) {
            // Table statistic call is used by flink-table-planner module to get Table schema, so
            // we cannot throw from this method.
            return CatalogColumnStatistics.UNKNOWN;
        } else {
            return this.decoratedCatalog.getTableColumnStatistics(tablePath);
        }
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec)
        throws PartitionNotExistException, CatalogException {

        if (getCatalogTable(tablePath).isDeltaTable()) {
            throw new CatalogException(
                "Delta table connector does not support partition statistics.");
        } else {
            return this.decoratedCatalog.getPartitionStatistics(tablePath, partitionSpec);
        }
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec)
        throws PartitionNotExistException, CatalogException {

        if (getCatalogTable(tablePath).isDeltaTable()) {
            throw new CatalogException(
                "Delta table connector does not support partition column statistics.");
        } else {
            return this.decoratedCatalog.getPartitionColumnStatistics(tablePath, partitionSpec);
        }
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath,
            CatalogTableStatistics tableStatistics,
            boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

        if (getCatalogTable(tablePath).isDeltaTable()) {
            throw new CatalogException(
                "Delta table connector does not support alter table statistics.");
        } else {
            this.decoratedCatalog.alterTableStatistics(
                tablePath,
                tableStatistics,
                ignoreIfNotExists
            );
        }
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException, TablePartitionedException {

        if (getCatalogTable(tablePath).isDeltaTable()) {
            throw new CatalogException(
                "Delta table connector does not support alter table column statistics.");
        } else {
            this.decoratedCatalog.alterTableColumnStatistics(
                tablePath,
                columnStatistics,
                ignoreIfNotExists
            );
        }
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

        if (getCatalogTable(tablePath).isDeltaTable()) {
            throw new CatalogException(
                "Delta table connector does not support alter partition statistics.");
        } else {
            this.decoratedCatalog.alterPartitionStatistics(
                tablePath,
                partitionSpec,
                partitionStatistics,
                ignoreIfNotExists
            );
        }
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

        if (getCatalogTable(tablePath).isDeltaTable()) {
            throw new CatalogException(
                "Delta table connector does not support alter partition column statistics.");
        } else {
            this.decoratedCatalog.alterPartitionColumnStatistics(
                tablePath,
                partitionSpec,
                columnStatistics,
                ignoreIfNotExists
            );
        }
    }

    private DeltaCatalogBaseTable getCatalogTable(ObjectPath tablePath) {
        try {
            return getCatalogTableUnchecked(tablePath);
        } catch (TableNotExistException e) {
            throw new CatalogException(e);
        }
    }

    /**
     * In some cases like {@link Catalog#getTable(ObjectPath)} Flink runtime expects
     * TableNotExistException. In those cases we cannot throw checked exception because it could
     * break some table planner logic.
     */
    private DeltaCatalogBaseTable getCatalogTableUnchecked(ObjectPath tablePath)
        throws TableNotExistException {
        CatalogBaseTable table = this.decoratedCatalog.getTable(tablePath);
        return new DeltaCatalogBaseTable(tablePath, table);
    }
}
