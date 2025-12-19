# V2 Connector Interop

This package contains abstract traits that enable interoperability between the V1 and V2 Delta connectors.

## Purpose

These abstractions allow V1 utilities to be reused by the V2 connector through adapters.

## Usage

For reusing V1 connector code in V2:

1. **Refactor V1 utilities** to depend on abstract traits (`AbstractMetadata`, `AbstractProtocol`, `AbstractCommitInfo`) instead of the concrete V1 implementations.

2. **Implement adapters in V2 connector** that extend these abstractions, wrapping Kernel's action types (e.g., Kernel's `Metadata` → `AbstractMetadata`).

## Traits

- `AbstractMetadata` - Common abstraction for table metadata (schema, partitions, configuration)
- `AbstractProtocol` - Common abstraction for protocol (reader/writer versions, features)
- `AbstractCommitInfo` - Common abstraction for commit information (timestamps)

