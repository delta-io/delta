package io.delta.dsv2.catalog;

import io.delta.dsv2.table.DeltaCcv2Table;
import io.delta.kernel.ResolvedTable;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.unity.InMemoryUCClient;
import io.delta.unity.UCCatalogManagedClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A test implementation of TableCatalog for Unity Catalog using InMemoryUCClient.
 * This catalog simulates Unity Catalog behavior for testing without real UC dependencies.
 */
public class SimpleUnityCatalogForTesting implements TableCatalog {

    private String catalogName;
    private String warehouseName;
    private InMemoryUCClient ucClient;
    private UCCatalogManagedClient ucCatalogManagedClient;
    private Engine engine;

    // In-memory storage for tables
    private final Map<String, TableInfo> tables = new ConcurrentHashMap<>();

    // Internal class to store table information
    private static class TableInfo {
        final String tableId;
        final String schemaName;
        final String tableName;
        final String storageLocation;

        TableInfo(String tableId, String schemaName, String tableName, String storageLocation) {
            this.tableId = tableId;
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.storageLocation = storageLocation;
        }
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        // Verify namespace
        if (namespace.length != 1) {
            throw new NoSuchNamespaceException(namespace);
        }

        String schemaName = namespace[0];

        // Filter tables by schema name
        List<Identifier> tableIdentifiers = tables.values().stream()
                .filter(info -> info.schemaName.equalsIgnoreCase(schemaName))
                .map(info -> Identifier.of(namespace, info.tableName))
                .collect(Collectors.toList());

        return tableIdentifiers.toArray(new Identifier[0]);
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        return loadTable(ident, null);
    }

    @Override
    public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
        if (ident.namespace().length != 1) {
            throw new NoSuchTableException(ident);
        }

        String fullTableName = getFullTableName(ident);
        TableInfo tableInfo = findTable(fullTableName);

        if (tableInfo == null) {
            throw new NoSuchTableException(ident);
        }

        try {
            // Load the table using UCCatalogManagedClient
            Optional<Long> versionOpt = version == null ? Optional.empty() : Optional.of(Long.valueOf(version));
            ResolvedTable table = ucCatalogManagedClient.loadTable(
                    engine, tableInfo.tableId, tableInfo.storageLocation, versionOpt);

            // Create a mock DeltaCcv2Table with testing credentials
            return new DeltaCcv2Table(table, ident, engine, "mock-access-key", "mock-secret-key", "mock-session-token");
        } catch (Exception e) {
            throw new RuntimeException("Failed to load table: " + e.getMessage(), e);
        }
    }

    @Override
    public Table createTable(
            Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException {

        if (tableExists(ident)) {
            throw new TableAlreadyExistsException(ident);
        }

        // Create a new table entry
        String tableId = UUID.randomUUID().toString();
        String schemaName = ident.namespace()[0];
        String tableName = ident.name();
        String storageLocation = "s3://mock-bucket/test-tables/" + tableId;

        // Store the table information
        String fullTableName = getFullTableName(ident);
        tables.put(fullTableName, new TableInfo(tableId, schemaName, tableName, storageLocation));

        // Initialize table in the in-memory client
        ucClient.getTableDataElseThrow(tableId);

        // Return the newly created table
        return loadTable(ident);
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        throw new UnsupportedOperationException("Altering tables is not supported in testing implementation");
    }

    @Override
    public boolean dropTable(Identifier ident) {
        String fullTableName = getFullTableName(ident);
        return tables.remove(fullTableName) != null;
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException {
        throw new UnsupportedOperationException("Renaming tables is not supported in testing implementation");
    }

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.catalogName = name;
        this.warehouseName = options.getOrDefault("warehouse", "default");

        String metastoreId = options.getOrDefault("metastoreId", UUID.randomUUID().toString());

        // Initialize in-memory UC client
        this.ucClient = new InMemoryUCClient(metastoreId);
        this.ucCatalogManagedClient = new UCCatalogManagedClient(ucClient);

        // Create default engine for testing
        Configuration conf = new Configuration();
        this.engine = DefaultEngine.create(conf);
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public boolean tableExists(Identifier ident) {
        if (ident.namespace().length != 1) {
            return false;
        }

        String fullTableName = getFullTableName(ident);
        return tables.containsKey(fullTableName);
    }

    /**
     * Gets the full qualified name of a table.
     */
    private String getFullTableName(Identifier ident) {
        return String.format("%s.%s.%s", warehouseName, ident.namespace()[0], ident.name());
    }

    /**
     * Find table by full name.
     */
    private TableInfo findTable(String fullTableName) {
        return tables.get(fullTableName);
    }

    /**
     * For testing: Register a pre-existing table.
     */
    public void registerTable(String schemaName, String tableName, String tableId, String location) {
        String fullTableName = String.format("%s.%s.%s", warehouseName, schemaName, tableName);
        tables.put(fullTableName, new TableInfo(tableId, schemaName, tableName, location));
    }

    /**
     * For testing: Access to the underlying UC client.
     */
    public InMemoryUCClient getUcClient() {
        return ucClient;
    }
}