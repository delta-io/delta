package io.delta.flink.internal.table;

import java.util.List;
import java.util.Optional;

import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.Factory;

/**
 * Base implementation of Flink catalog. This class handles Catalog operations for Delta tables that
 * do not require interaction with _delta_log, for example view, database operations etc.
 */
public abstract class BaseCatalog extends AbstractCatalog {

    protected final Catalog decoratedCatalog;

    public BaseCatalog(
            String name,
            String defaultDatabase,
            Catalog decoratedCatalog) {
        super(name, defaultDatabase);

        this.decoratedCatalog = decoratedCatalog;
    }

    //////////////////////////////////////
    // Important, Delta related methods //
    //////////////////////////////////////

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(DeltaDynamicTableFactory.fromCatalog());
    }

    /////////////////////////////////////////////////////
    // Obvious, not Delta related pass-through methods //
    /////////////////////////////////////////////////////

    @Override
    public void open() throws CatalogException {
        this.decoratedCatalog.open();
    }

    @Override
    public void close() throws CatalogException {
        this.decoratedCatalog.close();
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return this.decoratedCatalog.listDatabases();
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
        throws DatabaseNotExistException, CatalogException {
        return this.decoratedCatalog.getDatabase(databaseName);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return this.decoratedCatalog.databaseExists(databaseName);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
        throws DatabaseAlreadyExistException, CatalogException {
        this.decoratedCatalog.createDatabase(name, database, ignoreIfExists);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
        throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        this.decoratedCatalog.dropDatabase(name, ignoreIfNotExists, cascade);

    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
        throws DatabaseNotExistException, CatalogException {
        this.decoratedCatalog.alterDatabase(name, newDatabase, ignoreIfNotExists);
    }

    @Override
    public List<String> listTables(String databaseName)
        throws DatabaseNotExistException, CatalogException {
        return this.decoratedCatalog.listTables(databaseName);
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
        throws TableNotExistException, TableAlreadyExistException, CatalogException {
        this.decoratedCatalog.renameTable(tablePath, newTableName, ignoreIfNotExists);
    }

    @Override
    public List<String> listViews(String databaseName)
        throws DatabaseNotExistException, CatalogException {
        return this.decoratedCatalog.listViews(databaseName);
    }

    @Override
    public List<String> listFunctions(String dbName)
        throws DatabaseNotExistException, CatalogException {
        return this.decoratedCatalog.listFunctions(dbName);
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
        throws FunctionNotExistException, CatalogException {
        return this.decoratedCatalog.getFunction(functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return this.decoratedCatalog.functionExists(functionPath);
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function,
        boolean ignoreIfExists)
        throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        this.decoratedCatalog.createFunction(functionPath, function, ignoreIfExists);
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction,
        boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
        this.decoratedCatalog.alterFunction(functionPath, newFunction, ignoreIfNotExists);
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
        throws FunctionNotExistException, CatalogException {
        this.decoratedCatalog.dropFunction(functionPath, ignoreIfNotExists);
    }

}
