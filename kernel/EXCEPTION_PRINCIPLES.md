# Exception principles in Delta Kernel
## Introduction
Exceptions thrown in Delta Kernel are either user-facing or developer-facing.
- **User-facing exceptions** are expected to be thrown. Delta Kernel is unable to complete the requested operation for a fundamental reason inherent to the nature of the request, the table of interest, and the capabilities of Delta Kernel and the Delta protocol. These errors are intentional and are used to communicate with the end-user why an operation cannot be completed.
- **Developer-facing exceptions** are unexpected and generally indicate that something has gone wrong or is incorrect. They can target either Kernel developers or connector developers that are using Kernel APIs. These exceptions should be used for debugging; a perfectly working connector + Kernel should never encounter these.

See [User-facing vs developer-facing exceptions](#User-facing-vs-developer-facing-exceptions) for examples of these types of exceptions.

## Principles
These are the general exception principles to follow and enforce when contributing code or reviewing pull requests.
- All **user-facing exceptions** should be of type `KernelException`.
    - Create a new subclass for exceptions that may require special handling (such as `TableNotFoundException`) otherwise just use `KernelException`. Subclasses should expose useful exception parameters on a case-by-case basis.
    - All `KernelException`s should be instantiated with a method in the [DeltaErrors](https://github.com/delta-io/delta/blob/master/kernel/kernel-api/src/main/java/io/delta/kernel/internal/DeltaErrors.java) file.
    - Error messages should be clear and actionable.
        - Clearly state (1) the problem, (2) why it occurred and (3) how it can be solved.
- **User-facing exceptions** should be consistent across releases. Any changes to user-facing exception classes or messages should be carefully reviewed.
- Any unchecked exceptions originating from the `Engine` implementation should be wrapped with `KernelEngineException` and should include additional context about the failing operation.
    - This means all method calls to the `Engine` implementation should be wrapped. See [Wrapping exceptions thrown from the Engine implementation](#Wrapping-exceptions-thrown-from-the-Engine-implementation) for more details.
- **Developer-facing exceptions** should be informative and provide useful information for debugging.

## Further details

### User-facing vs developer-facing exceptions

User-facing exceptions:
- `TableNotFoundException` when there is no Delta table at the provided path.
- Reading the Change Data Feed from a table without CDF enabled.
- The input data violates table constraints when writing to the table.
- Kernel doesn’t support reading a table with XXX table feature.

Developer-facing exceptions:
- `getInt` is called on a boolean `ColumnVector`.
- A column mapping mode besides “none”, “id”, and “name” is encountered.
- An empty iterator is returned from the `Engine` implementation when reading files.

### Wrapping exceptions thrown from the Engine implementation
We want to wrap any unchecked exceptions thrown from the `Engine` implementation with `KernelEngineException` and include additional context about the failing operation. This makes it clear where the exception is originating from, and the additional context can help future debugging.

This requires wrapping all method calls into the `Engine` implementation. We do this using helper methods in `DeltaErrors` like [wrapEngineException](https://github.com/delta-io/delta/blob/4fefba182f81d39f1d11e2f2b85bfa140079ea11/kernel/kernel-api/src/main/java/io/delta/kernel/internal/DeltaErrors.java#L228-L240). For usage see [example 1](https://github.com/delta-io/delta/blob/2b2ef732533c707b7ca1af30e2a059da86c3c3ff/kernel/kernel-api/src/main/java/io/delta/kernel/internal/TransactionImpl.java#L246-L256) and [example 2](https://github.com/delta-io/delta/blob/2b2ef732533c707b7ca1af30e2a059da86c3c3ff/kernel/kernel-api/src/main/java/io/delta/kernel/internal/ScanImpl.java#L236-L244).

Note: this does not catch all exceptions originating from the engine implementation, as exceptions that are not thrown until access will not be wrapped (i.e. exceptions thrown within iterators, in `ColumnVector` implementations, etc)
- When checked exceptions cannot be thrown we instead wrap the checked exception in a `KernelEngineException`. See [here](https://github.com/delta-io/delta/blob/2b2ef732533c707b7ca1af30e2a059da86c3c3ff/kernel/kernel-defaults/src/main/java/io/delta/kernel/defaults/internal/parquet/ParquetFileReader.java#L148-L150) for an example.
