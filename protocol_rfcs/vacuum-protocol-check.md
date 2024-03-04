# Vacuum Protocol Check

This RFC introduces a new ReaderWriter feature named `vacuumProtocolCheck`. This feature ensures that the Vacuum operation consistently performs both reader and writer protocol check. The motivation for this change is to address inconsistencies in Vacuum's behavior across different delta implementations, as some of them skip the writer protocol checks in practice. This omission blocks any protocol changes that might impact vacuum, including improvements to vacuum itself. The writer protocol check addresses an initial oversight in the original Delta specification where an older Delta Client executing a Vacuum command might incorrectly delete files that are still in use by newer versions, potentially leading to data corruption.

**For further discussions about this protocol change, please refer to the Github issue - https://github.com/delta-io/delta/issues/2630**

--------


> ***New Section***
# VACUUM Protocol Check

The `vacuumProtocolCheck` ReaderWriter feature ensures consistent application of reader and writer protocol checks during `VACUUM` operations, addressing potential protocol discrepancies and mitigating the risk of data corruption due to skipped writer checks.

Enablement:
- The table must be on Writer Version 7 and Reader Version 3.
- The feature `vacuumProtocolCheck` must exist in the table `protocol`'s `writerFeatures` and `readerFeatures`.

## Writer Requirements for Vacuum Protocol Check

This feature affects only the VACUUM operations; standard commits remain unaffected.

Before performing a VACUUM operation, writers must ensure that they check the table's write protocol. This is most easily implemented by adding an unconditional write protocol check for all tables, which removes the need to examine individual table properties.

Writers that do not implement VACUUM do not need to change anything and can safely write to tables that enable the feature.

## Recommendations for Readers of Tables with Vacuum Protocol Check feature

For tables with Vacuum Protocol Check enabled, readers don’t need to understand or change anything new; they just need to acknowledge the feature exists.