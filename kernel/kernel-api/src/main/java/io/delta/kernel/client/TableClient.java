package io.delta.kernel.client;

/**
 * Interface encapsulating all clients needed by the Delta Kernel in order to read the
 * Delta table. Connectors are expected to pass an implementation of this interface when reading
 * a Delta table.
 */
public interface TableClient
{

    /**
     * Get the connector provided {@link ExpressionHandler}.
     * @return An implementation of {@link ExpressionHandler}.
     */
    ExpressionHandler getExpressionHandler();

    /**
     * Get the connector provided {@link JsonHandler}.
     * @return An implementation of {@link JsonHandler}.
     */
    JsonHandler getJsonHandler();

    /**
     * Get the connector provided {@link FileSystemClient}.
     * @return An implementation of {@link FileSystemClient}.
     */
    FileSystemClient getFileSystemClient();

    /**
     * Get the connector provided {@link ParquetHandler}.
     * @return An implementation of {@link ParquetHandler}.
     */
    ParquetHandler getParquetHandler();
}
