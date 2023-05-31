package io.delta.kernel.client;

import io.delta.kernel.data.Row;

/**
 * Placeholder interface allowing connectors to attach their own custom implementation
 * to allow context about a {@link io.delta.kernel.fs.FileStatus} from connectors through
 * Delta Kernel back to the connectors.
 */
public interface FileReadContext
{
    /**
     * Get the scan file info associated with the read context.
     * @return File scan {@link Row}
     */
    Row getScanFileRow();
}
