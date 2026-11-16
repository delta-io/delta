# V2 Connector Interop

This package contains abstract traits that enable interoperability between the Delta Spark connectors.

## Purpose

The V2 snapshot facade exposes concrete V1 metadata and protocol actions converted from Kernel.
These actions also implement the abstract traits consumed by shared V1 utilities.

## Usage

When reusing V1 utilities in V2, pass the metadata and protocol exposed by the snapshot facade.
Use the existing abstract traits (`AbstractMetadata`, `AbstractProtocol`) where a shared utility
only needs their narrower contract.
