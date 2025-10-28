# [Question] How to expose Committer in custom Sink implementation (Flink 2.0)?

## Context

Migrating Delta Lake connector from Flink 1.16 to 2.0. All code compiles successfully, but `Committer` is never invoked by the framework during integration tests.

**Repository:** https://github.com/delta-io/delta  
**Issue:** https://github.com/delta-io/delta/issues/5228  
**PR with implementation:** [Will be added - fork PR link]

## Problem

`DeltaSinkInternal` implements `Sink<IN>` and has public methods:
```java
public Optional<Committer<DeltaCommittable>> createCommitter() throws IOException
public Optional<SimpleVersionedSerializer<DeltaCommittable>> getCommittableSerializer()
```

**Framework behavior:**
- ✅ `createWriter()` is discovered and invoked correctly
- ❌ `createCommitter()` is never invoked (confirmed via debug logging)

**Compilation issue when attempting `@Override`:**
```
error: method does not override or implement a method from a supertype
@Override
public Optional<Committer<DeltaCommittable>> createCommitter()
```

## What We Tried

1. **Added `@Override` annotations** → Compilation error (methods not in interface)
2. **Searched for interfaces:** `TwoPhaseCommittingSink`, `StatefulSink` → Do not exist in Flink 2.1.0
3. **Verified all imports:** Using `org.apache.flink.api.connector.sink2.*` correctly

## Current Implementation

```java
// DeltaSinkInternal.java
public class DeltaSinkInternal<IN> implements Sink<IN> {
    
    @Override
    public SinkWriter<IN> createWriter(WriterInitContext context) throws IOException {
        return sinkBuilder.createWriter(context); // ✅ WORKS
    }

    // NOTE: These methods are NOT part of Sink<IN> interface
    // but were working in Flink 1.x with TwoPhaseCommittingSink
    public Optional<Committer<DeltaCommittable>> createCommitter() throws IOException {
        return Optional.of(sinkBuilder.createCommitter()); // ❌ NEVER CALLED
    }

    public Optional<SimpleVersionedSerializer<DeltaCommittable>> getCommittableSerializer() {
        return Optional.of(sinkBuilder.getCommittableSerializer()); // ❌ NEVER CALLED
    }
}
```

## Question

**How should a custom Sink expose its Committer in Flink 2.0?**

We understand `FileSink` works correctly with Committer support. Specific questions:

1. Is there an interface (like `TwoPhaseCommittingSink`) that marks Sink as having a Committer?
2. Are there specific method names/signatures the framework discovers via reflection?
3. Should we implement a different pattern (e.g., `CommittingSinkWriter`)?

## Test Results

- **Unit tests (DeltaCommitterTest):** ✅ 4/4 pass (direct Committer invocation)
- **Integration tests (DeltaSinkBatchExecutionITCase):** ❌ Fail - Committer never invoked
- **Compilation:** ✅ 0 errors
- **SinkWriter functionality:** ✅ Works perfectly

## Environment

- Flink: 2.1.0
- JDK: 22
- Build: SBT
- Complete implementation available in PR (linked above)

## Request

Could you point us to:
1. The correct interface/pattern for Committer-enabled Sinks in Flink 2.0
2. FileSink source code reference showing how it exposes its Committer
3. Any migration documentation for `TwoPhaseCommittingSink` → Flink 2.0 equivalent

Thank you for your assistance!

