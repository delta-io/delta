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

package io.delta.flink.table;

import io.delta.flink.Conf;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Manages credentials with proactive refresh semantics.
 *
 * <p>{@code CredentialManager} is responsible for fetching credentials from an external source and
 * refreshing them before expiration. The expiration time is expected to be encoded in the
 * credential map under the key {@link #CREDENTIAL_EXPIRATION_KEY}.
 *
 * <p>The manager caches the most recently fetched credentials and re-fetches them when the
 * expiration time is approaching.
 *
 * <p>This class is intentionally agnostic of the underlying credential source and refresh strategy.
 * Callers provide:
 *
 * <ul>
 *   <li>A {@link Supplier} that fetches the latest credentials
 *   <li>A {@link Runnable} callback that is invoked when a refresh occurs (for example, to notify
 *       dependent components)
 * </ul>
 *
 * Typical usage: <code>
 *     CredentialManager credManager = new CredentialManager(loadCredFromCatalog, callback);
 *     credManager.getCredentials(); // Guaranteed to be an refreshed credential
 *     // Wait for a long time
 *     credManager.getCredentials(); // Internally refreshed, still return valid credentials.
 * </code>
 *
 * <p>This class is thread-safe.
 */
public class CredentialManager {

  /**
   * Key in the credential map that represents the credential expiration time.
   *
   * <p>The value is expected to be a string representation of a timestamp (for example, epoch
   * milliseconds), interpretable by the implementation.
   */
  protected static String CREDENTIAL_EXPIRATION_KEY = "credential.expiration";

  protected static ScheduledExecutorService refreshExecutors =
      Executors.newScheduledThreadPool(Conf.getInstance().getCredentialsRefreshThreadPoolSize());

  /**
   * Determines whether the given exception indicates a credential-related failure.
   *
   * <p>This predicate can be used by callers to detect failures that should trigger a credential
   * refresh or retry logic.
   *
   * @return a Predicate that returns {@code true} if the exception is related to invalid or expired
   *     credentials; {@code false} otherwise
   */
  public static Predicate<Throwable> isCredentialsExpired =
      ExceptionUtils.recursiveCheck(ex -> ex instanceof java.nio.file.AccessDeniedException);

  /** Supplier used to fetch the latest credentials from the underlying source. */
  private final Supplier<Map<String, String>> credSupplier;

  /**
   * Callback invoked after credentials are refreshed.
   *
   * <p>This can be used to trigger downstream updates or reconfiguration when credentials change.
   */
  private final Runnable refreshCallback;

  private AtomicReference<Map<String, String>> cachedCredentials = new AtomicReference<>();

  public CredentialManager(Supplier<Map<String, String>> supplier, Runnable refreshCallback) {
    this.credSupplier = supplier;
    this.refreshCallback = refreshCallback;
  }

  /**
   * Returns the current credentials, refreshing them if expiration is approaching.
   *
   * <p>If no credentials have been fetched yet, this method will fetch and cache them. On
   * subsequent calls, the cached credentials are returned unless they are near expiration, in which
   * case a refresh is triggered.
   *
   * <p>The refresh strategy (for example, how close to expiration a refresh occurs) is
   * implementation-defined.
   *
   * @return the current valid credentials
   */
  Map<String, String> getCredentials() {
    Map<String, String> cached = cachedCredentials.get();
    if (cached != null) return cached;
    Map<String, String> newCredentials = credSupplier.get();
    if (cachedCredentials.compareAndSet(null, newCredentials)) {
      scheduleNextRefresh(newCredentials);
      return newCredentials;
    }
    return cachedCredentials.get();
  }

  protected void scheduleNextRefresh(Map<String, String> newCredentials) {
    long expiration = -1;
    try {
      expiration = Long.parseLong(newCredentials.getOrDefault(CREDENTIAL_EXPIRATION_KEY, "-1"));
    } catch (NumberFormatException ignore) {
    }
    if (expiration >= 0) {
      long refreshDelay =
          Math.max(
              100, // A minimal wait of 100ms if the refresh delay is too small
              expiration
                  - Conf.getInstance().getCredentialsRefreshAheadInMs()
                  - System.currentTimeMillis());
      refreshExecutors.schedule(
          () -> {
            Map<String, String> existingCredential = cachedCredentials.get();
            Map<String, String> refreshedCredential = credSupplier.get();
            if (cachedCredentials.compareAndSet(existingCredential, refreshedCredential)) {
              refreshCallback.run();
              scheduleNextRefresh(refreshedCredential);
            }
          },
          refreshDelay,
          TimeUnit.MILLISECONDS);
    }
  }
}
