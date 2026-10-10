# Native S3 put-if-absent prototype

`io.delta.storage.S3LogStore` is an opt-in native conditional writer for filesystem-based Delta commits.
It uses Hadoop S3A for credentials, endpoints, encryption, uploads, and request retries.
It does not change the default S3 LogStore or Delta's OCC configuration.

## Configuration and assumptions

Select the store for every S3 scheme used by the application, for example:

```text
spark.delta.logStore.s3a.impl=io.delta.storage.S3LogStore
```

The prototype requires Hadoop S3A 3.4.2 or later with conditional creation enabled, user metadata, metadata reads, immediate publication on close, and abortable streams.
Select matching Hadoop/S3A runtime dependencies; they are supplied by the application, not bundled with Delta storage.
Unsupported filesystems and capabilities fail explicitly.
Magic output streams are unsupported because they defer publication until a later job commit.
AWS general-purpose S3 buckets are the intended production target; the local tests do not qualify S3 Express or other S3-compatible services.

All concurrent writers must use a compatible atomic publication protocol.
Do not overlap these writers with legacy `S3SingleDriverLogStore` writers or assume an existing DynamoDB writer deployment can be mixed without a migration protocol.
An unconditional writer can overwrite a conditionally created object.
Destination keys must remain immutable during reconciliation, including protection from external deletion and replacement.
These assumptions apply to non-overwrite writes; overwrite calls retain the existing S3 behavior.

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
| Missing destination, directory marker, failed metadata lookup, or inconclusive error | `IOException` reporting an unknown publication outcome |

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
The prototype reports an unresolved outcome instead of consuming an exhausted action iterator or retrying a consumed multipart upload at the LogStore layer.

An unknown-outcome exception does not mean that the object was not committed.
Delta's current filesystem OCC loop does not automatically reconcile arbitrary I/O failures.
Its optional `CommitInfo.txnId` self-commit check is disabled by default and only applies after entering conflict handling.
This store does not depend on that check to recover a confirmed write.

Neither the per-invocation UUID nor an in-memory transaction ID provides automatic recovery after process death.
A terminated process can leave an incomplete multipart upload or a fully committed transaction with no acknowledgement.
Restart-safe application retries require a durable application identity, such as `txnAppId` and `txnVersion` where supported.
Bucket lifecycle cleanup for incomplete multipart uploads remains an operational responsibility.

## Architecture and evidence

The existing `S3SingleDriverLogStore` uses a JVM lock, an existence check, and classic `FileSystem.create`.
In the inspected upstream Hadoop 3.4.2, 3.4.3, and trunk implementations, configuration alone does not turn that call into conditional publication; create-performance is not a substitute for the mandatory builder option.

The prototype places atomic publication in S3A, individual-write reconciliation in the LogStore, and transaction conflict checking in Delta OCC.
A direct SDK implementation would duplicate connector configuration and lifecycle behavior.
OCC-only recovery would not cover other LogStore writes and would couple storage safety to optional Spark transaction behavior.

Research baseline: Delta master `3d1969ee8b5d5d5e67313902a1d32c1aaf5508b6` and draft PR 7767 at `df8c1b5d7ce14c1c3d4292f169e09c406750012b`.
The draft was a reference, not the implementation baseline.
DBR implementations informed the comparison of ownership and checksum recovery; no DBR code is copied here.

- [Hadoop conditional-write tracker](https://issues.apache.org/jira/browse/HADOOP-19256)
- [AWS conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
- [CompleteMultipartUpload behavior](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
- [AWS SDK lost-acknowledgement issue](https://github.com/aws/aws-sdk-java-v2/issues/6580)
- [Delta draft PR 7767](https://github.com/delta-io/delta/pull/7767)

## Validation and production follow-up

The tests use real S3A and SDK clients against a deterministic local HTTP endpoint, plus connector-boundary fault tests and Spark transaction tests.
The fixture validates conditional publication, metadata round trips, request retries, multipart completion, independent JVM races, and process termination.
It does not emulate IAM, signing validation, encryption, checksums, versioning, service limits, or the full S3 protocol.
Local test success must not be reported as live AWS validation.
Concurrency coverage combines independent JVM LogStore writers with Spark transactions sharing a session.
A workload with two independent Spark drivers against AWS remains untested.

Run the relevant suites from the repository root:

```sh
build/sbt 'storage/test'
build/sbt 'spark/testOnly org.apache.spark.sql.delta.NativeS3CommitSuite org.apache.spark.sql.delta.IdempotentCommitRetrySuite org.apache.spark.sql.delta.DelegatingLogStoreSuite org.apache.spark.sql.delta.LogStoreProviderSuite org.apache.spark.sql.delta.*LogStoreSuite'
build/sbt 'storageS3DynamoDB/test' 'spark/Test/scalastyle'
```

The implementation was developed against failing connector-boundary and real-S3A feature tests.
Legacy characterization confirms that classic create sends unconditional requests.
The HTTP tests inspect actual `If-None-Match` headers and retry outcomes, including PUT retry 412, consumed-upload retry 404, and HTTP 200 containing a multipart completion error.
Fresh correctness review identified an interrupt-restoration defect; regression tests reproduced it before the fix.
Legacy compatibility smoke checks exercised external Java callers and subclasses on Hadoop 2.7.3 and 3.3.4, including callers compiled against the original class hierarchy.
These local checks used selected older Hadoop jars with some newer support dependencies and do not qualify complete historical runtime distributions.

Verified locally on 2026-10-07 with OpenJDK 17, Hadoop S3A 3.4.2, AWS SDK 2.29.52, and the repository's default Spark 4.2.0 test lane:

| Check | Result |
| --- | --- |
| Full storage module | 153 tests passed |
| Spark LogStore, provider, native commit, and idempotent retry suites | 114 tests passed |
| Full DynamoDB storage module | 36 tests passed |
| Spark test Scala style | No errors or warnings |
| Legacy compatibility smoke checks (2026-10-02) | 12 JVM runs, 48 local scenarios passed |

The test fixture initially rejected small listing pages used by S3A directory probes, preventing Spark checksum publication.
The same Spark lost-acknowledgement test failed against that fixture and passed after its pagination was corrected, retaining the checksum assertions.
No live AWS bucket was used.
Final independent correctness review of the completed diff found no blocking issues within the approved prototype scope.

Before making this store the default, qualify real AWS PUT and multipart failure behavior, supported Hadoop distributions and endpoints, homogeneous writer rollout, unknown-outcome diagnostics, and cleanup.
Whole-upload replay, persistent restart identity, and generalized OCC recovery for unknown outcomes are follow-up work.
