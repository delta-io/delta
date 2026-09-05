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

package shadedForDelta.org.apache.iceberg.rest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import shadedForDelta.org.apache.iceberg.BaseFileScanTask;
import shadedForDelta.org.apache.iceberg.DeleteFile;
import shadedForDelta.org.apache.iceberg.FileMetadata;
import shadedForDelta.org.apache.iceberg.FileScanTask;
import shadedForDelta.org.apache.iceberg.PartitionSpecParser;
import shadedForDelta.org.apache.iceberg.SchemaParser;
import shadedForDelta.org.apache.iceberg.Table;
import shadedForDelta.org.apache.iceberg.TableScan;
import shadedForDelta.org.apache.iceberg.catalog.Catalog;
import shadedForDelta.org.apache.iceberg.catalog.TableIdentifier;
import shadedForDelta.org.apache.iceberg.io.CloseableIterable;
import shadedForDelta.org.apache.iceberg.rest.HTTPRequest;
import shadedForDelta.org.apache.iceberg.rest.RESTCatalogAdapter;
import shadedForDelta.org.apache.iceberg.rest.requests.PlanTableScanRequest;
import shadedForDelta.org.apache.iceberg.rest.requests.PlanTableScanRequestParser;
import shadedForDelta.org.apache.iceberg.rest.responses.ErrorResponse;
import shadedForDelta.org.apache.iceberg.rest.PlanStatus;
import shadedForDelta.org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import shadedForDelta.org.apache.iceberg.rest.responses.PlanTableScanResponse;
import shadedForDelta.org.apache.iceberg.expressions.Expression;
import shadedForDelta.org.apache.iceberg.expressions.Expressions;
import shadedForDelta.org.apache.iceberg.expressions.ResidualEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends RESTCatalogAdapter to add support for server-side scan planning via the /plan endpoint.
 * This adapter intercepts /plan requests and handles them by executing Iceberg table scans locally,
 * returning file scan tasks to the client. Other catalog operations are delegated to the parent
 * RESTCatalogAdapter implementation.
 */
class IcebergRESTCatalogAdapterWithPlanSupport extends RESTCatalogAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergRESTCatalogAdapterWithPlanSupport.class);

  private final Catalog catalog;
  // Catalog prefix returned in /v1/config that gets inserted into REST paths.
  // Example: prefix="iceberg" transforms /v1/namespaces/db/tables/t1/plan
  //          to /v1/iceberg/namespaces/db/tables/t1/plan
  private String catalogPrefix = null;  // null = no prefix (fallback case)
  private volatile Map<String, String> catalogDefaults = Collections.emptyMap();
  private volatile Map<String, String> catalogOverrides = Collections.emptyMap();
  private volatile boolean advertiseEndpoints = true;
  private volatile boolean advertiseFetchPlanningResult = true;
  private volatile boolean advertiseCancelPlanning = true;

  // Static fields for test verification - captures filter and projection from requests
  // Volatile is used to guarantee correct cross-thread access (test thread and Jetty server thread).
  private static volatile Expression capturedFilter = null;
  private static volatile List<String> capturedProjection = null;
  private static volatile Long capturedMinRowsRequested = null;
  private static volatile Boolean capturedCaseSensitive = null;

  // Static field for test credential injection - credentials to inject into /plan responses
  // Volatile is used to guarantee correct cross-thread access (test thread and Jetty server thread).
  private static volatile Map<String, String> testCredentials = null;

  // Static field for test residual injection - residual expression to override in /plan responses.
  // When set, all FileScanTasks in the response will have this residual instead of the default.
  // Volatile is used to guarantee correct cross-thread access (test thread and Jetty server thread).
  private static volatile Expression testResidual = null;
  private static volatile boolean injectDeleteFiles = false;
  private static volatile boolean injectPlanTasks = false;

  // Failure injection for the GET fetch-planning-result (poll) path, mirroring the POST fields.
  // fetchFailCount: number of remaining poll requests to fail before allowing success.
  // fetchFailStatusCode: HTTP status code to return for injected poll failures.
  private static final AtomicInteger fetchFailCount = new AtomicInteger(0);
  private static volatile int fetchFailStatusCode = 503;

  // Static field to capture the request path of /plan requests for test verification
  // Volatile is used to guarantee correct cross-thread access (test thread and Jetty server thread).
  private static volatile String capturedPlanRequestPath = null;

  // Failure injection fields for testing HTTP retry logic.
  // planRequestFailCount: number of remaining /plan requests to fail before allowing success.
  // planRequestFailStatusCode: HTTP status code to return for injected failures.
  // planRequestCount: total number of /plan requests received (for verifying retry behavior).
  private static final AtomicInteger planRequestFailCount = new AtomicInteger(0);
  private static volatile int planRequestFailStatusCode = 503;
  private static final AtomicInteger planRequestCount = new AtomicInteger(0);

  // Async planning injection fields. A non-negative value enables async planning and specifies
  // how many GET fetchPlanningResult requests should return SUBMITTED before the completed result.
  // The completed result is served as a FetchPlanningResultResponse, matching the GET endpoint's
  // schema (no plan-id), while the POST /plan submitted response carries the plan-id.
  private static final String TEST_PLAN_ID = "test-plan-id";
  private static final AtomicInteger submittedPollsRemaining = new AtomicInteger(-1);
  private static final AtomicInteger planPollRequestCount = new AtomicInteger(0);
  private static final AtomicInteger planCancelRequestCount = new AtomicInteger(0);
  private static volatile PlanStatus fetchTerminalStatus = PlanStatus.COMPLETED;
  private static volatile FetchPlanningResultResponse pendingPlanResult = null;

  IcebergRESTCatalogAdapterWithPlanSupport(Catalog catalog) {
    super(catalog);
    this.catalog = catalog;
  }

  /**
   * Set the catalog prefix to be returned by /v1/config endpoint.
   * The prefix is inserted into REST paths: /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan
   * Used for testing prefix-based endpoint construction.
   * Package-private as this is an implementation detail - tests should use
   * IcebergRESTServer.setCatalogPrefix() instead.
   *
   * @param prefix The prefix to return in config.overrides, or null for no prefix
   */
  void setCatalogPrefix(String prefix) {
    this.catalogPrefix = prefix;
  }

  /**
   * Get the catalog prefix for testing.
   * Package-private for servlet access.
   */
  String getCatalogPrefix() {
    return this.catalogPrefix;
  }

  void setCatalogDefaults(Map<String, String> defaults) {
    this.catalogDefaults =
        defaults != null
            ? Collections.unmodifiableMap(new HashMap<>(defaults))
            : Collections.emptyMap();
  }

  Map<String, String> getCatalogDefaults() {
    return catalogDefaults;
  }

  void setCatalogOverrides(Map<String, String> overrides) {
    this.catalogOverrides =
        overrides != null
            ? Collections.unmodifiableMap(new HashMap<>(overrides))
            : Collections.emptyMap();
  }

  Map<String, String> getCatalogOverrides() {
    return catalogOverrides;
  }

  void setAdvertiseEndpoints(boolean advertise) {
    this.advertiseEndpoints = advertise;
  }

  boolean getAdvertiseEndpoints() {
    return advertiseEndpoints;
  }

  void setAdvertiseFetchPlanningResult(boolean advertise) {
    this.advertiseFetchPlanningResult = advertise;
  }

  boolean getAdvertiseFetchPlanningResult() {
    return advertiseFetchPlanningResult;
  }

  void setAdvertiseCancelPlanning(boolean advertise) {
    this.advertiseCancelPlanning = advertise;
  }

  boolean getAdvertiseCancelPlanning() {
    return advertiseCancelPlanning;
  }

  /**
   * Get the filter captured from the most recent /plan request.
   * Package-private for test access.
   */
  static Expression getCapturedFilter() {
    return capturedFilter;
  }

  /**
   * Get the projection (list of column names) captured from the most recent /plan request.
   * Package-private for test access.
   */
  static List<String> getCapturedProjection() {
    return capturedProjection;
  }

  /**
   * Get the min-rows-requested captured from the most recent /plan request.
   * Package-private for test access.
   */
  static Long getCapturedMinRowsRequested() {
    return capturedMinRowsRequested;
  }

  /**
   * Get the caseSensitive flag captured from the most recent /plan request.
   * Package-private for test access.
   */
  static Boolean getCapturedCaseSensitive() {
    return capturedCaseSensitive;
  }

  /**
   * Get the request path captured from the most recent /plan request.
   * Package-private for test access.
   */
  static String getCapturedPlanRequestPath() {
    return capturedPlanRequestPath;
  }

  /**
   * Set test credentials to inject into /plan responses.
   * Package-private for test access.
   *
   * @param credentials Map of credential config (e.g., "s3.access-key-id" -> "...")
   */
  static void setTestCredentials(Map<String, String> credentials) {
    testCredentials = credentials;
  }

  /**
   * Get the test credentials configured for injection into /plan responses.
   * Package-private for servlet access.
   */
  static Map<String, String> getTestCredentials() {
    return testCredentials;
  }

  /**
   * Set test residual expression to inject into /plan responses.
   * When set, all FileScanTasks in the response will have this residual expression
   * instead of the default (alwaysTrue). Used for testing client-side residual validation.
   * Package-private for test access via IcebergRESTServer.
   *
   * @param residual The residual expression to inject, or null to use the default
   */
  static void setTestResidual(Expression residual) {
    testResidual = residual;
  }

  static void setInjectDeleteFiles(boolean inject) {
    injectDeleteFiles = inject;
  }

  static void setInjectPlanTasks(boolean inject) {
    injectPlanTasks = inject;
  }

  /** Configure the server to fail the next N poll (GET /plan/{plan-id}) requests. */
  static void setFailNextFetchRequests(int count, int statusCode) {
    fetchFailCount.set(count);
    fetchFailStatusCode = statusCode;
  }

  /** Atomically get and decrement the remaining poll failure count. */
  static int getAndDecrementFetchFailCount() {
    return fetchFailCount.getAndDecrement();
  }

  static int getFetchFailStatusCode() {
    return fetchFailStatusCode;
  }

  /** Increment the poll request count (used by the servlet when injecting poll failures). */
  static void incrementPlanPollRequestCount() {
    planPollRequestCount.incrementAndGet();
  }

  /**
   * Clear captured filter, projection, and limit. Call between tests to avoid pollution.
   * Package-private for test access.
   */
  static void clearCaptured() {
    capturedFilter = null;
    capturedProjection = null;
    capturedMinRowsRequested = null;
    capturedCaseSensitive = null;
    testCredentials = null;
    testResidual = null;
    injectDeleteFiles = false;
    injectPlanTasks = false;
    fetchFailCount.set(0);
    fetchFailStatusCode = 503;
    capturedPlanRequestPath = null;
    planRequestFailCount.set(0);
    planRequestCount.set(0);
    submittedPollsRemaining.set(-1);
    planPollRequestCount.set(0);
    planCancelRequestCount.set(0);
    fetchTerminalStatus = PlanStatus.COMPLETED;
    pendingPlanResult = null;
  }

  static void setSubmittedPollsBeforeCompletion(int count) {
    submittedPollsRemaining.set(count);
  }

  static int getPlanPollRequestCount() {
    return planPollRequestCount.get();
  }

  static void recordPlanCancellation() {
    planCancelRequestCount.incrementAndGet();
    pendingPlanResult = null;
  }

  static int getPlanCancelRequestCount() {
    return planCancelRequestCount.get();
  }

  static void setFetchTerminalStatus(PlanStatus status) {
    fetchTerminalStatus = status;
  }

  /**
   * Configure the server to fail the next N /plan requests with the specified HTTP status code.
   * After N failures, subsequent requests proceed normally.
   * Used for testing HTTP retry logic in the client.
   *
   * @param count Number of /plan requests to fail
   * @param statusCode HTTP status code to return for injected failures (e.g., 503, 404)
   */
  static void setFailNextPlanRequests(int count, int statusCode) {
    planRequestFailCount.set(count);
    planRequestFailStatusCode = statusCode;
  }

  /**
   * Atomically get and decrement the remaining failure count.
   * Returns the value before decrement. If > 0, the request should be failed.
   * Package-private for servlet access.
   */
  static int getAndDecrementFailCount() {
    return planRequestFailCount.getAndDecrement();
  }

  /**
   * Get the HTTP status code to use for injected failures.
   * Package-private for servlet access.
   */
  static int getPlanRequestFailStatusCode() {
    return planRequestFailStatusCode;
  }

  /**
   * Increment and return the total /plan request count.
   * Package-private for servlet access.
   */
  static void incrementPlanRequestCount() {
    planRequestCount.incrementAndGet();
  }

  /**
   * Get the total number of /plan requests received.
   * Package-private for test access.
   */
  static int getPlanRequestCount() {
    return planRequestCount.get();
  }

  @Override
  protected <T extends RESTResponse> T execute(
          HTTPRequest request,
          Class<T> responseType,
          Consumer<ErrorResponse> errorHandler,
          Consumer<Map<String, String>> responseHeaders,
          ParserContext parserContext) {
    LOG.debug("Executing request: {} {}", request.method(), request.path());

    // Intercept /plan requests before they reach the base adapter
    if (isPlanTableScanRequest(request)) {
      capturedPlanRequestPath = request.path();  // Capture the path for test verification
      try {
        PlanTableScanResponse response = handlePlanTableScan(request, parserContext);
        return (T) response;
      } catch (Exception e) {
        LOG.error("Error handling plan table scan: {}", e.getMessage(), e);
        ErrorResponse error = ErrorResponse.builder()
            .responseCode(500)
            .withType("InternalServerError")
            .withMessage("Failed to plan table scan: " + e.getMessage())
            .build();
        errorHandler.accept(error);
        return null;
      }
    }

    if (isFetchPlanningResultRequest(request)) {
      planPollRequestCount.incrementAndGet();
      try {
        return (T) handleFetchPlanningResult(request);
      } catch (Exception e) {
        LOG.error("Error fetching plan table scan result: {}", e.getMessage(), e);
        ErrorResponse error = ErrorResponse.builder()
            .responseCode(500)
            .withType("InternalServerError")
            .withMessage("Failed to fetch plan table scan result: " + e.getMessage())
            .build();
        errorHandler.accept(error);
        return null;
      }
    }

    return super.execute(
        request, responseType, errorHandler, responseHeaders, parserContext);
  }

  private boolean isPlanTableScanRequest(HTTPRequest request) {
    return HTTPRequest.HTTPMethod.POST.equals(request.method()) &&
           request.path().endsWith("/plan");
  }

  private boolean isFetchPlanningResultRequest(HTTPRequest request) {
    return HTTPRequest.HTTPMethod.GET.equals(request.method()) &&
        request.path().endsWith("/plan/" + TEST_PLAN_ID);
  }

  private FetchPlanningResultResponse handleFetchPlanningResult(HTTPRequest request) {
    if (pendingPlanResult == null) {
      throw new IllegalArgumentException("Unknown plan ID in path: " + request.path());
    }

    // The GET fetchPlanningResult submitted response carries no plan-id, unlike POST /plan.
    if (submittedPollsRemaining.getAndDecrement() > 0) {
      return FetchPlanningResultResponse.builder()
          .withPlanStatus(PlanStatus.SUBMITTED)
          .build();
    }

    FetchPlanningResultResponse response = pendingPlanResult;
    pendingPlanResult = null;
    return response;
  }

  private TableIdentifier extractTableIdentifier(String path) {
    // Path format: /v1/namespaces/{namespace}/tables/{table}/plan
    // or: /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan

    String[] parts = path.split("/");
    int namespacesIdx = -1;
    for (int i = 0; i < parts.length; i++) {
      if ("namespaces".equals(parts[i])) {
        namespacesIdx = i;
        break;
      }
    }

    if (namespacesIdx == -1 || namespacesIdx + 3 >= parts.length) {
      throw new IllegalArgumentException("Invalid path format: " + path);
    }

    String namespace = parts[namespacesIdx + 1];
    String tableName = parts[namespacesIdx + 3]; // skip "tables"

    return TableIdentifier.of(namespace, tableName);
  }

  private PlanTableScanRequest parsePlanRequest(HTTPRequest request) {
    // The request body should be a JSON string
    Object body = request.body();
    if (body == null) {
      throw new IllegalArgumentException("Request body is null");
    }
    String jsonBody = body.toString();
    return PlanTableScanRequestParser.fromJson(jsonBody);
  }

  private PlanTableScanResponse handlePlanTableScan(
      HTTPRequest request,
      ParserContext parserContext) throws Exception {

    LOG.debug("Handling plan table scan request");

    // Extract table identifier
    TableIdentifier tableIdent = extractTableIdentifier(request.path());
    LOG.debug("Table identifier: {}", tableIdent);

    // Parse request (min-rows-requested is natively supported in Iceberg 1.11.0)
    PlanTableScanRequest planRequest = parsePlanRequest(request);
    Long minRowsRequested = planRequest.minRowsRequested();
    LOG.debug("Plan request parsed: snapshotId={}, minRowsRequested={}",
        planRequest.snapshotId(), minRowsRequested);

    // Load table from catalog
    Table table = catalog.loadTable(tableIdent);
    LOG.debug("Table loaded: {}", table);

    // Create table scan
    TableScan tableScan = table.newScan();

    // Apply snapshot if specified and valid
    if (planRequest.snapshotId() != null && planRequest.snapshotId() != 0) {
      tableScan = tableScan.useSnapshot(planRequest.snapshotId());
      LOG.debug("Using snapshot: {}", planRequest.snapshotId());
    } else {
      LOG.debug("Using current snapshot (snapshotId was null or 0)");
    }

    // Capture filter, projection, and limit for test verification
    capturedFilter = planRequest.filter();
    capturedProjection = planRequest.select();
    capturedMinRowsRequested = minRowsRequested;
    capturedCaseSensitive = planRequest.caseSensitive();
    LOG.debug("Captured filter: {}", capturedFilter);
    LOG.debug("Captured projection: {}", capturedProjection);
    LOG.debug("Captured min-rows-requested: {}", capturedMinRowsRequested);
    LOG.debug("Captured caseSensitive: {}", capturedCaseSensitive);

    // Validate caseSensitive=false requirement
    if (planRequest.caseSensitive()) {
      throw new IllegalArgumentException("caseSensitive=true is not supported");
    }

    // Validate that unsupported features are not requested
    if (planRequest.startSnapshotId() != null) {
      throw new UnsupportedOperationException(
          "Incremental scans are not supported in this test implementation");
    }
    if (planRequest.endSnapshotId() != null) {
      throw new UnsupportedOperationException(
          "Incremental scans are not supported in this test implementation");
    }
    if (planRequest.statsFields() != null && !planRequest.statsFields().isEmpty()) {
      throw new UnsupportedOperationException(
          "Column stats are not supported in this test implementation");
    }

    // Execute scan planning
    List<FileScanTask> fileScanTasks = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = tableScan.planFiles()) {
      tasks.forEach(task -> fileScanTasks.add(task));
    }
    LOG.debug("Planned {} file scan tasks", fileScanTasks.size());

    // If a test residual is configured, rebuild tasks with the injected residual
    Expression residualOverride = testResidual;
    List<FileScanTask> tasksToReturn;
    if (residualOverride != null || injectDeleteFiles) {
      tasksToReturn = new ArrayList<>();
      for (FileScanTask task : fileScanTasks) {
        DeleteFile[] deleteFiles = task.deletes().toArray(new DeleteFile[0]);
        if (injectDeleteFiles) {
          DeleteFile injectedDelete = FileMetadata.deleteFileBuilder(task.spec())
              .ofPositionDeletes()
              .withPath("file:/tmp/test-position-delete.parquet")
              .withFileSizeInBytes(1L)
              .withRecordCount(1L)
              .build();
          deleteFiles = new DeleteFile[] {injectedDelete};
        }
        Expression residual =
            residualOverride != null ? residualOverride : Expressions.alwaysTrue();
        tasksToReturn.add(new BaseFileScanTask(
            task.file(),
            deleteFiles,
            SchemaParser.toJson(task.spec().schema()),
            PartitionSpecParser.toJson(task.spec()),
            ResidualEvaluator.of(task.spec(), residual, capturedCaseSensitive)));
      }
      LOG.debug("Rebuilt {} file scan tasks with test overrides", tasksToReturn.size());
    } else {
      tasksToReturn = fileScanTasks;
    }

    // Get partition specs for serialization
    Map<Integer, shadedForDelta.org.apache.iceberg.PartitionSpec> specsById = table.specs();
    LOG.debug("Table has {} partition specs", specsById.size());

    // Async planning: stash the terminal FetchPlanningResultResponse and return a submitted
    // POST response carrying the plan-id.
    if (submittedPollsRemaining.get() >= 0) {
      FetchPlanningResultResponse.Builder resultBuilder =
          FetchPlanningResultResponse.builder().withPlanStatus(fetchTerminalStatus);
      if (fetchTerminalStatus == PlanStatus.COMPLETED) {
        resultBuilder
            .withFileScanTasks(tasksToReturn)
            .withSpecsById(specsById);
        if (injectPlanTasks) {
          resultBuilder.withPlanTasks(Collections.singletonList("test-plan-task"));
        }
      } else if (fetchTerminalStatus == PlanStatus.FAILED) {
        resultBuilder.withErrorResponse(ErrorResponse.builder()
            .responseCode(500)
            .withType("TestPlanningFailure")
            .withMessage("Injected planning failure")
            .build());
      }
      pendingPlanResult = resultBuilder.build();
      return PlanTableScanResponse.builder()
          .withSpecsById(specsById)
          .withPlanStatus(PlanStatus.SUBMITTED)
          .withPlanId(TEST_PLAN_ID)
          .build();
    }

    // Synchronous planning (Pattern 1: COMPLETED with direct tasks).
    PlanTableScanResponse.Builder syncBuilder = PlanTableScanResponse.builder()
        .withPlanStatus(PlanStatus.COMPLETED)
        .withFileScanTasks(tasksToReturn)
        .withSpecsById(specsById);
    if (injectPlanTasks) {
      syncBuilder.withPlanTasks(Collections.singletonList("test-plan-task"));
    }
    return syncBuilder.build();
  }
}
