# DRC LoadTable First Slice Design

## Goal

Implement the first Delta-side slice of Delta REST Catalog (DRC) integration for Unity Catalog:

- DRC-backed `loadTable`
- DRC-backed `getTableCredentials`

This slice is intentionally read-only. It does not implement DRC `createTable`, DRC commit, or DRC get-commits behavior yet.

## Authority and Scope

This design is grounded in:

- `delta_rest_catalog_design_doc_clarifications.md`
- `delta_rest_catalog_api_spec.md`
- `delta_rest_catalog_provider_design.md`
- the current generated UC SDK under `unitycatalog/clients/java/target/src/main/java/io/unitycatalog/client/delta/`

The old Delta POC PR `delta-io/delta#6575` is used only for shape:

- `UCDeltaClient`
- `DRCMetadataAdapter`
- `DeltaRestSchemaConverter`
- `AbstractDeltaCatalog.loadTable` wiring

Any conflict between the POC and the current docs / SDK is resolved in favor of the docs and the current SDK.

## Confirmed Decisions

- Delta builds directly against UC master at a pinned SHA. No compile-time DRC shim architecture.
- `UCDeltaClient` extends the existing `UCClient`.
- The DRC read-side surface should mirror the server contract and return raw UC SDK types.
- For this slice, the new read-side methods are name-based:
  - `loadTable(catalog, schema, table)`
  - `getTableCredentials(catalog, schema, table, operation)`
- `tableId` is not an input to the read-side methods. It is returned by `LoadTableResponse.metadata.table-uuid`.
- If the DRC path is unavailable or any DRC load/credential call fails, Delta must log a WARN and fall back to the legacy UC API path.
- `UCDeltaClientProvider` is deferred. It belongs to the later shared-client / commit-coordinator slice, not this first load-only slice.

## Contract Clarification: Credentials Are Separate

The current local spec and generated SDK do not include credentials in `LoadTableResponse`.

- The prose spec explicitly says credential vending is not part of `loadTable`.
- The generated `LoadTableResponse` model contains only:
  - `metadata`
  - `commits`
  - `uniform`
  - `latest-table-version`

Therefore, Delta must treat DRC table loading as two distinct RPCs:

1. `TablesApi.loadTable(catalog, schema, table)`
2. `TemporaryCredentialsApi.getTableCredentials(READ, catalog, schema, table)`

## Recommended Architecture

### 1. Delta-side client

Add a Delta-side `UCDeltaClient` interface in storage that extends `UCClient`.

For this first slice, it will expose:

- legacy `UCClient` methods unchanged
- new DRC read-side methods returning raw SDK types:
  - `io.unitycatalog.client.delta.model.LoadTableResponse loadTable(...)`
  - `io.unitycatalog.client.delta.model.CredentialsResponse getTableCredentials(...)`

It will also define future DRC-shaped write methods now so the interface grows in the right direction:

- DRC `createTable(...)`
- DRC name-based `commit(...)`

Those future methods should include the data the update path will need later:

- `oldMetadata`
- `newMetadata`
- `oldProtocol`
- `newProtocol`
- optional `etag`
- optional `uniform`

For this slice, those future write methods throw immediately with a clear unsupported message.

### 2. Concrete implementation

Implement one concrete `UCDeltaClient` against the current UC client jars.

It should:

- reuse the UC-owned `ApiClient`
- construct DRC `TablesApi` and `TemporaryCredentialsApi` from that `ApiClient`
- use the existing legacy `UCClient` behavior as fallback

This is not a stop-the-world refactor. The existing path-based legacy APIs remain intact.

### 3. Catalog wiring

`AbstractDeltaCatalog.loadTable` is the first DRC consumer.

When the runtime DRC flag is enabled:

- inspect `delegate`
- if it implements `io.unitycatalog.client.delta.DeltaRestClientProvider`, first check `getDeltaTablesApi()`
- if `getDeltaTablesApi()` is empty, treat DRC as unavailable and fall back
- otherwise obtain the shared UC `ApiClient`
- build or reuse a `UCDeltaClient`
- attempt DRC `loadTable`
- attempt DRC `getTableCredentials(..., READ)`
- convert the DRC schema directly to Spark schema
- build the Delta table using the DRC metadata and READ credentials

If any part of that DRC path fails, log:

- `WARN: falling back to legacy UC API`

and continue via the existing legacy `super.loadTable(...)` path.

Fallback conditions include:

- DRC runtime flag off
- delegate does not implement `DeltaRestClientProvider`
- provider returns no DRC support
- `loadTable` call fails
- `getTableCredentials` call fails
- schema conversion fails
- metadata adaptation fails

## Metadata and Schema Adaptation

### `DRCMetadataAdapter`

Add a Delta-side adapter around the UC SDK `TableMetadata` implementing `AbstractMetadata`.

Purpose:

- expose Delta metadata through the storage abstraction
- allow DRC-aware consumers to access the UC schema object directly
- keep lazy `getSchemaString()` only as fallback / compatibility support

This follows the useful shape of the POC while matching the current SDK, not the older SDK shape.

### `DeltaRestSchemaConverter`

Add a converter from the current UC SDK DRC schema model to Spark `StructType`.

Requirements:

- no JSON roundtrip on the happy path
- support nested struct / array / map / decimal types
- preserve field nullability
- preserve field metadata needed by Delta and Spark, especially column mapping metadata

The POC converter is the right idea but not authoritative implementation.

## Behavior of Unsupported Methods in This Slice

The following DRC methods may exist on `UCDeltaClient` now but should throw for this slice:

- DRC `createTable`
- DRC name-based `commit`
- DRC name-based `getCommits` if added

This keeps the interface shape aligned with the later design without accidentally implying write support exists.

## File Impact

Expected initial touch points:

- `storage/.../uccommitcoordinator/UCDeltaClient.java`
- `storage/.../uccommitcoordinator/` concrete implementation
- `storage/.../uccommitcoordinator/DRCMetadataAdapter.java`
- `spark/.../catalog/AbstractDeltaCatalog.scala`
- `spark/.../catalog/DeltaRestSchemaConverter.scala`

Explicitly not in this slice:

- `UCCommitCoordinatorBuilder`
- `CatalogTrackedInfo`
- `SnapshotManagement`
- `OptimisticTransaction`
- kill switch weakening
- DRC create / commit logic

## Testing

Required tests for this slice:

- schema converter unit tests for primitive, decimal, array, map, and nested struct cases
- field metadata preservation tests, especially column mapping metadata
- successful DRC `loadTable` path
- successful DRC `getTableCredentials(READ)` path
- fallback when provider is absent
- fallback when provider exposes no DRC support
- fallback when DRC `loadTable` throws
- fallback when DRC credential vending throws
- WARN log assertion on fallback

## Non-Goals

This slice does not:

- implement DRC `createTable`
- implement DRC commit or update-table behavior
- plumb etag through snapshot or commit state
- share a Delta-owned `UCDeltaClient` instance through `UCDeltaClientProvider`
- weaken UC-managed metadata kill switches

Those belong to later slices.
