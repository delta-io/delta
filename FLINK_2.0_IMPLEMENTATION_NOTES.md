# Flink 2.0 Support Implementation Notes

## Summary

This document outlines the investigation and implementation progress for adding Apache Flink 2.0+ support to the delta-flink connector (Issue #5228).

## Current Status

**Branch:** `flink/add-flink-2.0-support`

Initial implementation work has been completed, revealing significant breaking changes in Apache Flink 2.0 that require substantial refactoring of the connector.

## Breaking Changes Identified

### 1. Removal of Scala API (✅ FIXED)
**Issue:** `flink-scala_2.12` artifact no longer exists in Flink 2.0+

**Resolution:** Removed the deprecated dependency from build.sbt

**Commit:** `a96e331fd` - "[Flink] Remove flink-scala dependency removed in Flink 2.0"

### 2. Complete Sink API Refactoring (⚠️ IN PROGRESS)

Apache Flink 2.0 introduced a complete redesign of the Sink API architecture. The following packages and classes have been removed or significantly modified:

#### Removed/Changed APIs:
- ❌ `org.apache.flink.api.connector.sink.Committer`
- ❌ `org.apache.flink.api.connector.sink.GlobalCommitter`
- ❌ `org.apache.flink.api.connector.sink.Sink`
- ❌ `org.apache.flink.api.connector.sink.SinkWriter`
- ❌ `org.apache.flink.table.api.TableSchema` (replaced with new schema system)
- ❌ `org.apache.flink.table.connector.sink.SinkProvider`
- ❌ `org.apache.flink.table.catalog.hive.factories.HiveCatalogFactory` (Hive integration changed)

#### Affected Files (15+ classes):
```
connectors/flink/src/main/java/io/delta/flink/sink/internal/
├── DeltaSinkInternal.java
├── DeltaSinkBuilder.java
├── committer/
│   ├── DeltaCommitter.java
│   └── DeltaGlobalCommitter.java
└── writer/
    └── DeltaWriter.java

connectors/flink/src/main/java/io/delta/flink/internal/table/
├── CatalogLoader.java
├── DeltaCatalogTableHelper.java
└── DeltaDynamicTableSink.java
```

## Technical Analysis

### Complexity Assessment

The migration from Flink 1.16.x to 2.0+ requires:

1. **Complete Sink API Rewrite**
   - New architecture with different commit semantics
   - Changed interfaces and lifecycle methods
   - Updated state management approach

2. **Table API Updates**
   - New schema system replacing TableSchema
   - Updated catalog integration
   - Modified sink provider mechanism

3. **Testing Requirements**
   - Unit test updates for new API
   - Integration tests with Flink 2.0 runtime
   - Verification of commit semantics and exactly-once guarantees

### Estimated Effort

| Task Category | Estimated Hours | Complexity |
|--------------|----------------|------------|
| Sink API Refactoring | 24-32h | High |
| Table API Updates | 8-12h | Medium |
| Catalog Integration | 4-6h | Medium |
| Testing & Validation | 12-16h | High |
| Documentation | 2-4h | Low |
| **TOTAL** | **50-70h** | **High** |

## Implementation Roadmap

### Phase 1: Core Sink API Migration (24-32h)
**Priority:** Critical

**Tasks:**
1. [ ] Research Flink 2.0 Sink API architecture and migration guide
2. [ ] Update `DeltaSinkInternal` to use new Sink interface
3. [ ] Refactor `DeltaSinkBuilder` for new builder pattern
4. [ ] Migrate `DeltaWriter` to new SinkWriter interface
5. [ ] Update commit handling in `DeltaCommitter`
6. [ ] Refactor `DeltaGlobalCommitter` for new API

**Files to Update:**
- `DeltaSinkInternal.java` (~300 LOC changes)
- `DeltaSinkBuilder.java` (~200 LOC changes)
- `DeltaWriter.java` (~400 LOC changes)
- `DeltaCommitter.java` (~150 LOC changes)
- `DeltaGlobalCommitter.java` (~600 LOC changes)

### Phase 2: Table API Integration (8-12h)
**Priority:** High

**Tasks:**
1. [ ] Update schema handling to use new Table API
2. [ ] Migrate `DeltaCatalogTableHelper` to new schema system
3. [ ] Update `DeltaDynamicTableSink` with new SinkProvider API
4. [ ] Fix catalog integration points

**Files to Update:**
- `DeltaCatalogTableHelper.java` (~200 LOC changes)
- `DeltaDynamicTableSink.java` (~100 LOC changes)
- `DeltaDynamicTableSource.java` (minor updates)

### Phase 3: Catalog & Metadata (4-6h)
**Priority:** Medium

**Tasks:**
1. [ ] Update Hive catalog integration (if needed)
2. [ ] Verify metadata handling with new APIs
3. [ ] Update catalog loader for Flink 2.0

**Files to Update:**
- `CatalogLoader.java`
- `DeltaCatalog.java`

### Phase 4: Testing & Validation (12-16h)
**Priority:** Critical

**Tasks:**
1. [ ] Update unit tests for new API contracts
2. [ ] Create integration tests with Flink 2.0 runtime
3. [ ] Verify exactly-once semantics
4. [ ] Test with various Delta table configurations
5. [ ] Performance benchmarking
6. [ ] Update test utilities and helpers

**Test Files to Update:**
- All tests in `connectors/flink/src/test/java/`
- Integration test suites
- Example applications

### Phase 5: Documentation & Examples (2-4h)
**Priority:** Medium

**Tasks:**
1. [ ] Update README with Flink 2.0 compatibility
2. [ ] Update code examples
3. [ ] Document breaking changes
4. [ ] Update API documentation

## Alternative Approaches

### Option A: Flink 1.19.x Support (Faster)
**Effort:** ~8-12 hours

Instead of Flink 2.0, upgrade to latest Flink 1.x (1.19.x):
- ✅ Maintains API compatibility
- ✅ Less breaking changes
- ✅ Faster implementation
- ❌ Does not address the original request (Flink 2.0)

### Option B: Conditional Support (Medium)
**Effort:** ~60-80 hours

Maintain both Flink 1.x and 2.x support:
- ✅ Backward compatibility
- ✅ Gradual migration path
- ❌ Increased maintenance burden
- ❌ More complex build configuration

### Option C: Complete Migration to Flink 2.0 (Recommended for Long-term)
**Effort:** ~50-70 hours

Full migration to Flink 2.0+ only:
- ✅ Clean codebase
- ✅ Modern API usage
- ✅ Long-term support
- ❌ Requires significant effort
- ❌ Breaks compatibility with Flink 1.x

## Community Contribution Guidelines

We welcome contributions to help complete this migration! Here's how you can help:

### Getting Started
1. Fork the repository
2. Check out branch: `flink/add-flink-2.0-support`
3. Review this document and Flink 2.0 migration guides
4. Pick a task from the roadmap above
5. Comment on this issue indicating which phase/task you're working on

### Useful Resources
- [Apache Flink 2.0 Release Notes](https://flink.apache.org/)
- [Flink Sink API Migration Guide](https://nightlies.apache.org/flink/flink-docs-master/)
- [Delta Lake Connector Documentation](https://docs.delta.io/latest/flink-integration.html)

### Testing Your Changes
```bash
# Compile the connector
./build/sbt flink/compile

# Run unit tests
./build/sbt flink/test

# Run integration tests
./build/sbt flink/it:test
```

### Code Review Checklist
- [ ] Code compiles without errors
- [ ] All unit tests pass
- [ ] Integration tests added/updated
- [ ] Exactly-once semantics preserved
- [ ] Documentation updated
- [ ] Code follows project style guidelines

## Current Branch Status

**Commits:**
1. `83f82740d` - Initial Flink 2.0 setup and dependency updates
2. `a96e331fd` - Remove flink-scala dependency

**Compilation Status:** ❌ Failed (expected - Phase 1 work needed)

**Next Steps:**
1. Community feedback on approach
2. Task assignment for Phase 1
3. Create sub-tasks/issues for each major component

## Questions for Community

1. **Approach Preference:** Should we go with Option C (full migration) or Option A (Flink 1.19.x)?
2. **Timeline:** Is there a specific release target for this feature?
3. **Compatibility:** Should we maintain backward compatibility with Flink 1.x?
4. **Resources:** Are there Databricks/Delta Lake team members who can assist with review?

## Conclusion

While the initial investigation revealed more complexity than anticipated, the migration is definitely feasible with community collaboration. The work is well-structured and can be divided among multiple contributors.

**Status:** 🟡 In Progress - Awaiting community decision on implementation approach

---

**Contributors:**
- @ksnunes - Initial investigation and analysis

**Related Issues:**
- #5228 - Original feature request

