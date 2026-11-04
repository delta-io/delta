# Native S3 put-if-absent LogStore

Delta Spark automatically selects `io.delta.storage.S3LogStore` for filesystem-based commits on S3 when the filesystem advertises the required native-write capabilities.
It uses Hadoop S3A for credentials, endpoints, encryption, uploads, and request retries.
Delta's OCC configuration and the default providers used by other engines are unchanged.

## Configuration and assumptions

No LogStore override is needed for supported S3A filesystems.
Automatic selection applies to `s3`, `s3a`, and `s3n` paths and uses the operation's Hadoop configuration.
Instances are cached by scheme and selected implementation, while the capability decision is evaluated for each operation.
An explicit `spark.delta.logStore.class` or `spark.delta.logStore.<scheme>.impl` override always takes precedence without a filesystem capability probe.
The existing aliases without the `spark.` prefix are also honored.
For example, an explicit native selection remains supported and fails rather than falling back if the filesystem cannot perform native writes:

```text
spark.delta.logStore.s3a.impl=io.delta.storage.S3LogStore
```

The native LogStore requires Hadoop S3A 3.4.2 or later with conditional creation enabled, user metadata, metadata reads, immediate publication on close, and abortable streams.
Select matching Hadoop/S3A runtime dependencies; they are supplied by the application, not bundled with Delta storage.
Non-overwrite writes require S3A and delta-storage to use compatible classloaders.
A classloader mismatch fails before upload while reads and overwrites remain available.
During automatic selection, filesystems without the required capabilities retain the legacy `S3SingleDriverLogStore` with a warning that it cannot coordinate concurrent drivers.
This is a compatibility fallback, not the recommended configuration.
Use a supported S3A runtime and keep `fs.s3a.create.conditional.enabled` enabled, including any bucket-specific override.
S3A enables this option by default; it configures the connector, not AWS S3's support for conditional writes.
Explicit legacy selection is still respected, but should not be used for concurrent drivers.
Capability-probe errors propagate rather than causing a fallback, and a native write failure never retries through the legacy writer.
Magic output streams are unsupported because they defer publication until a later job commit.
AWS general-purpose S3 buckets are the intended production target; the local tests do not qualify S3 Express or other S3-compatible services.

Concurrent writers in independent drivers must use a compatible atomic publication protocol.
Do not overlap native and legacy `S3SingleDriverLogStore` writers across independent drivers.
An unconditional writer can overwrite a conditionally created object.
Destination keys must remain immutable during reconciliation, including protection from external deletion and replacement.
These assumptions apply to non-overwrite writes; overwrite calls retain the existing S3 behavior.

Within a JVM and classloader, the native and legacy Java S3 LogStores share a lock for each resolved file path.
The lock covers publication and ownership recovery, so a legacy writer cannot pass its existence check and then overwrite a concurrent native commit.
Different file paths can still be written concurrently.
The lock does not coordinate independent JVMs or URI aliases that resolve to different lock keys.

## Publication and recovery

Every `overwrite=false` invocation generates a fresh UUID and writes it as `delta-log-store-write-id` user metadata.
The stream builder requires `fs.option.create.conditional.overwrite=true`, which instructs S3A to send `If-None-Match: *` on PUT or multipart completion.
Actions are consumed once and encoded as UTF-8 with one newline after each action.
No whole-upload replay, payload spooling, or additional write retry loop is introduced.

Once all bytes have been submitted, close publishes the object.
If close throws, a metadata lookup may establish the outcome:

| Evidence | Result |
| --- | --- |
| Exact destination file contains this invocation's UUID | Success |
| Known conditional conflict and exact destination file contains another or no UUID | Java `FileAlreadyExistsException`, enabling ordinary Delta OCC |
| Missing destination, directory marker, failed metadata lookup, or inconclusive error | `S3UnknownWriteOutcomeException` |

`S3UnknownWriteOutcomeException` extends `IOException`, so callers can identify an unresolved write without parsing the message.
It retains the publication failure as its cause and any recovery-probe failure as a suppressed exception.
Cancellation continues to throw `InterruptedIOException` with the interrupt flag preserved.

The implementation checks file status before reading metadata because S3A's xattr implementation can fall back from `key` to `key/`.
A directory marker must not become a false winning commit.
Matching bytes alone are not treated as ownership: independent writers can submit identical content.
A fresh UUID is sufficient for this single-invocation model because recovery runs only after the entire payload is submitted and the UUID is never reused for a reconstructed payload.

An action-iterator or stream-write failure aborts instead of closing, so cleanup cannot publish a prefix of the intended actions.
Abort is best effort and never deletes the final object.
In particular, S3A marks a stream closed before publication and can ignore a later abort after a failed close.
Actual interruption is preserved; socket timeouts remain eligible for ownership recovery.

## Retry and process-failure limits

SDK and S3A request retries remain active.
A first request can succeed, lose its response, and return HTTP 412 on retry; 412 alone does not establish a competing writer.
Multipart completion can similarly succeed before a retry encounters `NoSuchUpload`.
Both cases can be recovered when the UUID is readable.

AWS allows a conditional PUT to be retried after HTTP 409, while multipart completion requires a fresh upload and re-uploading all parts.
The LogStore reports an unresolved outcome instead of consuming an exhausted action iterator or retrying a consumed multipart upload at the LogStore layer.

An unknown-outcome exception does not mean that the object was not committed.
Delta's current filesystem OCC loop does not automatically reconcile arbitrary I/O failures.
Its optional `CommitInfo.txnId` self-commit check is disabled by default and only applies after entering conflict handling.
This store does not depend on that check to recover a confirmed write.

Neither the per-invocation UUID nor an in-memory transaction ID provides automatic recovery after process death.
A terminated process can leave an incomplete multipart upload or a fully committed transaction with no acknowledgement.
Restart-safe application retries require a durable application identity, such as `txnAppId` and `txnVersion` where supported.
Bucket lifecycle cleanup for incomplete multipart uploads remains an operational responsibility.

## Architecture

S3A sends the conditional PUT and multipart completion requests.
The LogStore reconciles individual writes, and Delta OCC checks transaction conflicts.
Ownership recovery applies to every non-overwrite LogStore write, including callers outside Spark transactions.

## Validation

Local tests cover conditional PUT and multipart writes, ownership recovery, cancellation, LogStore selection, classloader compatibility, and shared path locking.
They use real S3A and SDK clients against a local HTTP endpoint, separate JVM writers, and Spark transactions.
The HTTP fixture does not validate IAM, signing, encryption, or S3 service limits.

Live AWS tests exercised explicit native-store selection on general-purpose S3 with SSE-S3, using OSS Spark 4.2.0, Hadoop S3A 3.4.2, and AWS SDK 2.29.52.
They covered independent Spark drivers, multipart publication, lost acknowledgements, and process termination.
Automatic selection, shared-lock changes, classloader checks, and the unknown-outcome exception type have local regression coverage only.
Unresolved service-generated 409 responses and HTTP 200 responses containing multipart errors have fault-injection coverage but were not observed in the AWS tests.
SSE-KMS, bucket versioning, S3 Express, other S3-compatible services, and other runtime distributions remain unqualified.

Legacy compatibility smoke checks use some newer support libraries and do not qualify complete historical Hadoop distributions.

Run the relevant suites from the repository root:

```sh
build/sbt 'storage/test'
build/sbt 'spark/testOnly org.apache.spark.sql.delta.SharedS3LogStoreLockSuite org.apache.spark.sql.delta.CapabilityBasedLogStoreSuite org.apache.spark.sql.delta.NativeS3CommitSuite org.apache.spark.sql.delta.IdempotentCommitRetrySuite org.apache.spark.sql.delta.DelegatingLogStoreSuite org.apache.spark.sql.delta.LogStoreProviderSuite org.apache.spark.sql.delta.*LogStoreSuite'
build/sbt 'storageS3DynamoDB/test' 'spark/Test/scalastyle'
```

Whole-upload replay, persistent restart identity, and generalized OCC recovery for unknown outcomes are outside this implementation.

## References

- [Hadoop conditional-write tracker](https://issues.apache.org/jira/browse/HADOOP-19256)
- [AWS conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
- [CompleteMultipartUpload behavior](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
- [AWS SDK lost-acknowledgement issue](https://github.com/aws/aws-sdk-java-v2/issues/6580)
