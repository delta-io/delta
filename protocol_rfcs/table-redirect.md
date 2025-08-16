# Redirect

**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/3702**
<!-- Remove this: Replace XXXX with the actual github issue number -->
<!--  Give a general description / context of the protocol change, and remove this comment. -->
This RFC introduces path-to-catalog redirect, a mechanism for Delta Lake to transparently reroute operations
from a path-based Delta table (source) to a catalog-registered Delta table (target). This allows legacy access patterns,
such as reading or writing directly by path, to benefit from centralized governance, access control, and consistent
metadata offered by modern catalogs.

## Terminology:

* Path-based Table: A Delta Lake table identified and accessed directly via its storage path (e.g., `s3://bucket/path/to/table`).
* Catalog-based Table: A Delta Lake table registered within a catalog system and accessed using a fully qualified identifier
  (e.g., `catalog.schema.table`)
* Path-to-Catalog Redirect: A mechanism that enables engines to redirect operations from a path-based Delta table to a
  corresponding catalog-based table. This redirection facilitates transitions to catalog-managed tables, allowing for
  centralized governance and management.
* Redirectable Delta clients: These are Delta clients that support the redirect feature.
* Non-redirectable Delta clients: These refer to Delta clients that do not support the redirection feature.
* Source Table: The original path-based Delta table where the redirect configuration is stored.
* Target Table: The catalog-based Delta table specified in the source table’s redirect configuration. Reads and writes
  against the source table are redirected to this target table.

## Overview

The proposal includes two distinct table features:

* **RedirectReaderWriter**:
  When enabled, redirectable clients must redirect **both read and write operations** from the source table to the target table.
  All operations from non-redirectable clients are blocked.

* **RedirectWriterOnly**:
  When enabled, redirectable clients must redirect both reads and writes. For non-redirectable clients,
  **reads from the source table are still allowed**, but **writes are blocked**.

Redirect is enabled and configured at the source table via the corresponding Delta table properties:  
`redirectReaderWriter` and `redirectWriterOnly`. These are JSON strings specific to the implementation, but they should
include the following attributes in general:

* **Type**: Describes the type of the redirection. Client uses this value to determine how to parse the redirect
  specification. The type is a string that identifies the exact redirect implementation.

* **State**: Indicates the status of the redirect. While the exact values are implementation-specific, the following
  high-level states are recommended:
    * `IN-PROGRESS`: The redirect is being set up. No redirect is performed. The source table is still readable but not writable.
    * `READY`: The redirect is fully configured and ready for use. Reads and writes must be redirected.

* **Spec**: This is a free-form JSON object that describes the information for accessing the redirected target table.
  Its value is determined by the `Type` attribute and implementation. In generally, it must include enough metadata for
  a client that supports the redirect feature to access the target table.

### Engine Responsibilities

When a redirectable Delta engine detects a redirect configuration:

* It must **block writes** to the source table if the redirect is in `IN-PROGRESS` state. This is to prevent data
  inconsistency and ensure that the source table is not modified while the redirect is being set up.
* It must **redirect reads and writes** to the target table if the redirect is in `READY` state.
* It could optionally allow client-specific bypasses for **certain writes**, such as metadata updates or data synchronization
  operations, even when the redirect is in `READY` state. This is implementation-specific and should be documented
  by the engine (see below).

### Engine Bypass

By default, when a redirect configuration is present, the table becomes effectively **read-only**, and all write operations
to the source table must be blocked. However, engines may support limited bypass logic to allow specific operations that are
necessary for managing or synchronizing the redirect state. Only the following categories of operations should be allowed to bypass redirect
enforcement:

* **Metadata updates** that modify the redirect configuration itself, such as enabling, disabling, or transitioning
  redirect state (e.g., from `IN-PROGRESS` to `READY`).

* **Data changes intended to synchronize the source table’s state with the target table**, such as backfilling commits
  or copying checkpoint and log files. These operations are typically part of controlled workflows and must be gated by
  engine-specific rules.

Any other writes to the source table must be considered invalid while redirection is enabled. The engine is responsible
for enforcing this policy and for ensuring that bypass behavior is intentional and explicit.

### Use Cases

Path-to-catalog redirect is a general-purpose mechanism. It enables:

* Redirecting legacy path-based readers/writers to canonical catalog-based targets
* Enforcing read-only semantics on cloned tables while optionally redirecting queries
* Providing upgrade paths to catalog-based management without breaking path-based jobs

This specification does **not** prescribe specific workflows. Instead, it defines the core redirection behavior that such
workflows can build upon.