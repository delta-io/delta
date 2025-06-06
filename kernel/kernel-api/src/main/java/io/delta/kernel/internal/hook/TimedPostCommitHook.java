/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.hook;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.MetricsReporter;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.internal.metrics.hook.PostCommitHookExecutionContext;
import io.delta.kernel.internal.metrics.hook.PostCommitHookMetricsReportImpl;
import io.delta.kernel.metrics.PostCommitHookMetricsReport;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for post-commit hooks that automatically times execution and reports metrics.
 * Implementations need only provide the hook logic and context creation.
 */
public abstract class TimedPostCommitHook implements PostCommitHook {
  private static final Logger LOG = LoggerFactory.getLogger(TimedPostCommitHook.class);

  /////////////////////////////////////////////////////////////////////////////////
  /// Public methods.                                                           ///
  /////////////////////////////////////////////////////////////////////////////////
  @Override
  public final void threadSafeInvoke(Engine engine) throws IOException {
    PostCommitHookExecutionContext context = createHookExecutionContext();
    long startNs = System.nanoTime();
    Exception caughtException = null;

    try {
      executeHook(engine);
    } catch (IOException | RuntimeException e) {
      // Re-throw the exception after recording metrics
      caughtException = e;
      throw e;
    } catch (Exception e) {
      caughtException = e;
      throw new RuntimeException("Unexpected hook failure", e);
    } finally {
      long durationNs = System.nanoTime() - startNs;
      context.setExecutionDurationNs(durationNs);
      recordPostCommitHookReport(engine, context, caughtException);
    }
  }

  /////////////////////////////////////////////////////////////////////////////////
  /// Protected methods.                                                        ///
  /////////////////////////////////////////////////////////////////////////////////
  /**
   * Execute the actual hook logic with timing and metrics collection handled automatically.
   *
   * @param engine the engine to use for the hook execution
   * @throws IOException if an IO error occurs during hook execution
   */
  protected abstract void executeHook(Engine engine) throws IOException;

  /**
   * Create the execution context containing table path, hook type, and execution details.
   *
   * @return the context for this hook execution.
   */
  protected abstract PostCommitHookExecutionContext createHookExecutionContext();

  /**
   * Creates a {@link PostCommitHookMetricsReport} and pushes it to any {@link MetricsReporter}s.
   */
  private void recordPostCommitHookReport(
      Engine engine, PostCommitHookExecutionContext hookExecutionContext, Exception e) {
    try {
      PostCommitHookMetricsReport report =
          (e != null)
              ? PostCommitHookMetricsReportImpl.forError(hookExecutionContext, e)
              : PostCommitHookMetricsReportImpl.forSuccess(hookExecutionContext);

      engine.getMetricsReporters().forEach(reporter -> reporter.report(report));
    } catch (Exception reportingException) {
      // Don't let metrics reporting failures affect the hook execution.
      LOG.error("Failed to report post-commit hook metrics", reportingException);
    }
  }
}
