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
package io.delta.kernel.internal.metrics.hook;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.internal.metrics.DeltaOperationReportImpl;
import io.delta.kernel.metrics.PostCommitHookMetricsReport;
import io.delta.kernel.metrics.PostCommitHookMetricsResult;
import java.util.Map;
import java.util.Optional;

/** A basic POJO implementation of {@link PostCommitHookMetricsReport} for creating them */
public class PostCommitHookMetricsReportImpl extends DeltaOperationReportImpl
    implements PostCommitHookMetricsReport {

  private final PostCommitHookMetricsResult postCommitHookMetrics;
  private final String hookType;
  private final Map<String, String> hookExecutionDetails;

  private PostCommitHookMetricsReportImpl(
      String tablePath,
      PostCommitHookMetricsResult postCommitHookMetrics,
      String hookType,
      Map<String, String> hookExecutionDetails,
      Optional<Exception> exception) {
    super(tablePath, exception);
    this.postCommitHookMetrics = requireNonNull(postCommitHookMetrics);
    this.hookType = requireNonNull(hookType);
    this.hookExecutionDetails = requireNonNull(hookExecutionDetails);
  }

  @Override
  public PostCommitHookMetricsResult getPostCommitHookMetrics() {
    return postCommitHookMetrics;
  }

  @Override
  public String getHookType() {
    return hookType;
  }

  @Override
  public Map<String, String> getHookExecutionDetails() {
    return hookExecutionDetails;
  }

  /**
   * Creates a {@link PostCommitHookMetricsReport} for failed post-commit hook execution.
   *
   * @param hookExecutionContext context/metadata about the post-commit hook execution
   * @param e the exception that was thrown
   */
  public static PostCommitHookMetricsReport forError(
      PostCommitHookExecutionContext hookExecutionContext, Exception e) {
    return new PostCommitHookMetricsReportImpl(
        hookExecutionContext.getTablePath(),
        hookExecutionContext.getPostCommitHookMetrics(),
        hookExecutionContext.getHookType().name(),
        hookExecutionContext.getHookExecutionDetails(),
        Optional.of(e));
  }

  /**
   * Creates a {@link PostCommitHookMetricsReport} for successful post-commit hook execution.
   *
   * @param hookExecutionContext context/metadata about the post-commit hook execution
   */
  public static PostCommitHookMetricsReport forSuccess(
      PostCommitHookExecutionContext hookExecutionContext) {
    return new PostCommitHookMetricsReportImpl(
        hookExecutionContext.getTablePath(),
        hookExecutionContext.getPostCommitHookMetrics(),
        hookExecutionContext.getHookType().name(),
        hookExecutionContext.getHookExecutionDetails(),
        Optional.empty() /* exception */);
  }
}
