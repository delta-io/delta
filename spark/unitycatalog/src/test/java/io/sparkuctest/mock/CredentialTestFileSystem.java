/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sparkuctest.mock;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * A wrapper over the local filesystem for testing UC table credential vending. Cloud-scheme paths
 * (e.g. {@code s3://bucket/...}) are mapped onto local files, and every access verifies the
 * connector's vended credentials: each credential encodes its mint timestamp and is valid for
 * {@code [mint, mint + duration)}, and this filesystem asserts the credential is still valid at the
 * current time, and counts how many distinct credentials (distinct mint timestamps) it observes.
 *
 * <p>Validating by containment, rather than recomputing an expected value from the clock and
 * demanding exact equality, is robust to a naturally advancing wall clock: it does not race the
 * credential's expiry between the connector minting it and this filesystem checking it. The clock
 * is resolved from the Hadoop conf key {@code UC_TEST_CLOCK_NAME} -- a manual clock when a test
 * configures one, otherwise the system clock, exactly as the production credential provider does.
 *
 * <p>The number of distinct mint timestamps observed, minus the first, is the renewal count.
 *
 * @param <T> the cloud-specific vended-token provider type
 */
public abstract class CredentialTestFileSystem<T> extends RawLocalFileSystem {
  // volatile: toggled on the driver thread (see UCDeltaTable*Test) but read inside Spark tasks on
  // executor threads under local[N], which are separate threads in the same JVM.
  public static volatile boolean credentialCheckEnabled = true;

  /** The single fake bucket the credentials are vended for. */
  public static final String BUCKET_NAME = "test-bucket";

  /** Default credential validity duration. */
  public static final long DEFAULT_DURATION_MILLIS = 30_000L;

  /**
   * Hadoop conf key overriding the credential validity duration (set via {@code
   * spark.hadoop.<key>}). The manual-clock renewal suites keep the short default; wall-clock suites
   * configure a long duration so a test never outlives its credential (which would race the expiry
   * check).
   */
  public static final String DURATION_CONF_KEY = "sparkuctest.credrenew.durationMillis";

  // Distinct credential mint timestamps verified so far; its size drives renewalCount().
  private final Set<Long> observedMintTimestamps = new HashSet<>();
  private volatile T lazyProvider;

  /** The URI scheme this filesystem handles, without the trailing colon (e.g. {@code "s3"}). */
  protected abstract String scheme();

  /** Creates the cloud-specific vended-token provider from the Hadoop conf the connector wired. */
  protected abstract T createProvider();

  /**
   * Resolves the current credential from the provider and returns the mint timestamp it encodes.
   * The base class then asserts the credential is still valid at the current time.
   */
  protected abstract long resolveCredentialMintMillis(T provider);

  @Override
  protected void checkPath(Path path) {
    // Skip RawLocalFileSystem's scheme/authority validation so cloud-scheme paths (e.g.
    // s3://bucket/...) are accepted rather than rejected as non-local.
  }

  @Override
  public FSDataOutputStream create(
      Path f,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    return super.create(toLocalPath(f), overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    if (f.toString().startsWith(scheme() + "://")) {
      String cloudPrefix = scheme() + "://" + f.toUri().getHost();
      return restorePathInFileStatus(cloudPrefix, super.getFileStatus(toLocalPath(f)));
    } else {
      // Non-cloud path (e.g. a local file: path): pass through unchanged.
      return super.getFileStatus(f);
    }
  }

  @Override
  public FSDataInputStream open(Path f) throws IOException {
    return super.open(toLocalPath(f));
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    String cloudPrefix = scheme() + "://" + f.toUri().getHost();
    FileStatus[] files;
    try {
      files = super.listStatus(toLocalPath(f));
    } catch (FileNotFoundException e) {
      return new FileStatus[0]; // Object stores return an empty listing for a non-existent prefix.
    }
    FileStatus[] res = new FileStatus[files.length];
    for (int i = 0; i < files.length; i++) {
      res[i] = restorePathInFileStatus(cloudPrefix, files[i]);
    }
    return res;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return super.mkdirs(toLocalPath(f), permission);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return super.rename(toLocalPath(src), toLocalPath(dst));
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return super.delete(toLocalPath(f), recursive);
  }

  private FileStatus restorePathInFileStatus(String cloudPrefix, FileStatus f) {
    String path = f.getPath().toString().replace("file:", cloudPrefix);
    return new FileStatus(
        f.getLen(),
        f.isDirectory(),
        f.getReplication(),
        f.getBlockSize(),
        f.getModificationTime(),
        new Path(path));
  }

  private Path toLocalPath(Path f) {
    checkCredentials(f);
    return new Path(f.toString().replaceAll(scheme() + "://.*?/", "file:///"));
  }

  private void checkCredentials(Path f) {
    if (!credentialCheckEnabled) return;
    // Every access must target the vended bucket; an unexpected host means the test drove I/O
    // somewhere its credentials were never issued for, which should fail loudly.
    assertThat(f.toUri().getHost()).isEqualTo(BUCKET_NAME);

    T provider = accessProvider();
    assertThat(provider).isNotNull();

    // Resolve the credential first, then read the clock. This ordering guarantees the lower bound
    // (mintMillis <= now). The upper bound can only be tripped if the current time crosses the
    // credential's expiry between the two reads. Under a frozen manual clock that cannot happen,
    // and under a wall clock the suites use a long duration (see DURATION_CONF_KEY) so a single
    // access cannot span it.
    long durMillis = durationMillis();
    long mintMillis = resolveCredentialMintMillis(provider);
    long now = clock().now().toEpochMilli();
    assertThat(now)
        .as(
            "vended credential valid for [%s, %s) must still be valid at %s",
            mintMillis, mintMillis + durMillis, now)
        .isGreaterThanOrEqualTo(mintMillis)
        .isLessThan(mintMillis + durMillis);

    observedMintTimestamps.add(mintMillis);
  }

  private Clock clock() {
    String clockName = getConf().get(UCHadoopConfConstants.UC_TEST_CLOCK_NAME);
    return clockName != null ? Clock.getManualClock(clockName) : Clock.systemClock();
  }

  private long durationMillis() {
    return getConf().getLong(DURATION_CONF_KEY, DEFAULT_DURATION_MILLIS);
  }

  private synchronized T accessProvider() {
    if (lazyProvider == null) {
      lazyProvider = createProvider();
    }
    return lazyProvider;
  }

  /** Parses the trailing mint timestamp from a {@code <prefix><ts>} credential value. */
  protected static long mintMillisOf(String value, String prefix) {
    assertThat(value).startsWith(prefix);
    return Long.parseLong(value.substring(prefix.length()));
  }

  /**
   * Number of credential renewals observed: distinct mint timestamps seen minus the first. Returns
   * 0 before any credential has been verified (rather than a negative count).
   */
  public int renewalCount() {
    return Math.max(0, observedMintTimestamps.size() - 1);
  }
}
