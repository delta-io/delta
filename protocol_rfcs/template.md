# Table feature name / meaningful name
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/XXXX**
<!-- Remove this: Replace XXXX with the actual github issue number -->

<!--
How to use this template:
- Everything above the `--------` separator is context for reviewers. It is not copied into PROTOCOL.md.
- Everything below the separator is the proposed PROTOCOL.md text, written as spec text.
- Fill in every section. If a section doesn't apply, keep the heading and write "Not applicable because ...".
  Reviewers ask for each of these sections, so leaving one out usually costs a review round.
- Describe the protocol, not an implementation. An RFC is written before the feature is implemented.
- Remove these comments when you are done.
-->

## Motivation

<!-- What problem this solves, and why existing features or the current spec are not enough. If this RFC replaces an earlier RFC, say what was wrong with the earlier one. -->

## Feature summary

<!-- Fill in the list below. -->
- Table feature name: `featureName-dev` <!-- lowerCamelCase. Keep the `-dev` suffix until the RFC is accepted. Write "No table feature" and give the reason if none is needed. -->
- Readers and writers, or writers only: <!-- Give the reason. An old reader that ignores this feature must still return correct results for the feature to be writers only. -->
- Minimum protocol versions: <!-- e.g. reader version 3, writer version 7 -->
- Required features: <!-- Features that must also be supported, e.g. `domainMetadata`. -->
- Features it can't be used with: <!-- e.g. clustering and partitioning can't be used together. -->
- Supported vs enabled: <!-- What readers and writers do when the feature is in the protocol (supported), and when a table property turns it on (enabled). If there is no property, say that being supported makes it active. -->
- Table properties: <!-- Each new `delta.` property, with allowed values and default. -->

## Compatibility

<!-- For each case, say whether the client fails cleanly (rejected by the protocol check), works correctly, or could silently return wrong results or corrupt the table. Silent wrong results are not acceptable. -->

| Case | Outcome |
|-|-|
| Old reader, table with this feature | |
| Old writer, table with this feature | |
| New reader, table without this feature | |
| New writer, table without this feature | |
| Existing table that enables the feature (mix of old and new files) | |
| Checkpoint writer that doesn't know the feature | |

## Interactions with other features

<!-- Describe the interaction, or write "None, because ...". Add rows for any other affected feature. -->

| Feature | Interaction |
|-|-|
| Column mapping | |
| Deletion vectors | |
| Checkpoints (classic, multi-part, V2, sidecars) | |
| Log compaction | |
| Metadata cleanup and VACUUM | |
| Version checksum (`.crc`) | |
| Action reconciliation | |
| Row tracking | |
| Change data feed | |
| Domain metadata | |
| In-commit timestamps | |
| Type widening | |
| Generated columns, CHECK constraints, invariants, default columns, identity columns | |
| Clustering and partitioning | |
| Statistics and data skipping | |
| IcebergCompatV1/V2/V3, Iceberg writer compat | |
| Catalog-managed tables | |
| Collations, variant, timestamp without timezone, other types | |

## Concurrency

<!-- Which concurrent transactions conflict because of the new actions or fields. Any race to claim a resource (files, IDs, domains), and who prevents it. -->

## Alternatives considered

<!-- Other designs and why they were not chosen. -->

## Open questions

<!-- Anything not decided yet. -->

--------

<!--
Below is the proposed PROTOCOL.md text. Follow the structure and wording of existing features
such as Column Mapping, In-Commit Timestamps and Type Widening. Use MUST / MUST NOT / SHOULD / MAY
for requirements, and use terms already defined in PROTOCOL.md (data file, logical file, supported, enabled, ...).
-->

# Feature name

<!-- Introduction: what the feature is and why it exists. Put rationale here, not in the requirement sections. -->

## Enablement

<!-- The table feature, the required protocol versions and features, the table property, and when a writer may enable the feature. -->

## Schema

<!-- One table per new action or field. Give exact types, say whether each field is required or optional, and what a missing optional field means. Define allowed values completely. -->

Field Name | Data Type | Description | optional/required
-|-|-|-
`fieldName` | String | | required

## Reader Requirements for Feature name

<!-- Everything a reader must do, including what to do on unknown or invalid values. -->

## Writer Requirements for Feature name

<!-- Everything a writer must do across the lifecycle: create, enable, append, update and delete, OPTIMIZE, checkpoint, log compaction, metadata cleanup and VACUUM, CLONE and RESTORE, and upgrading existing tables. Writers must also meet the reader requirements. -->

## Removing the feature

<!-- What writers must do before the feature can be dropped from the protocol, and what readers can assume afterwards. -->

## Examples

<!-- At least one complete, valid example for each new action or concept, plus edge cases. Examples must match the schema tables exactly. -->

## Changes to existing sections

<!-- Changes to other parts of PROTOCOL.md, e.g. checkpoint schema, other features' requirements, IcebergCompat sections. Quote the existing text and show the new text. -->

## Appendix changes

<!-- New rows for "Valid Feature Names in Table Features" and the "Table Properties" table, and new table of contents entries. -->
