package io.delta.kernel.client;

import io.delta.kernel.data.Row;

/**
 * Placeholder interface allowing connectors to attach their own custom implementation. Connectors
 * can use this to pass additional context about a scan file through Delta Kernel and back to the
 * connector for interpretation.
 */
public interface FileReadContext
{
    /**
     * Get the scan file info associated with the read context.
     * @return scan file {@link Row}
     */
    Row getScanFileRow();
}
