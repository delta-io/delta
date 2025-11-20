/*
 * Copyright (2021) The Delta Lake Project Authors.
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
import java.util.function.Consumer;

import shadedForDelta.org.apache.iceberg.FileScanTask;
import shadedForDelta.org.apache.iceberg.Table;
import shadedForDelta.org.apache.iceberg.TableScan;
import shadedForDelta.org.apache.iceberg.catalog.Catalog;
import shadedForDelta.org.apache.iceberg.catalog.TableIdentifier;
import shadedForDelta.org.apache.iceberg.io.CloseableIterable;
import shadedForDelta.org.apache.iceberg.rest.RESTCatalogAdapter;
import shadedForDelta.org.apache.iceberg.rest.requests.PlanTableScanRequest;
import shadedForDelta.org.apache.iceberg.rest.requests.PlanTableScanRequestParser;
import shadedForDelta.org.apache.iceberg.rest.responses.ErrorResponse;
import shadedForDelta.org.apache.iceberg.rest.PlanStatus;
import shadedForDelta.org.apache.iceberg.rest.responses.PlanTableScanResponse;
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

  // Test credentials to inject into /plan responses (for testing credential flow)
  // Default to empty strings so we always have credentials in response
  private String testAccessKey = "";
  private String testSecretKey = "";
  private String testSessionToken = "";

  IcebergRESTCatalogAdapterWithPlanSupport(Catalog catalog) {
    super(catalog);
    this.catalog = catalog;
  }

  /**
   * Set test credentials to be returned in /plan responses.
   * Used for testing credential injection flow.
   */
  public void setTestCredentials(String accessKey, String secretKey, String sessionToken) {
    this.testAccessKey = accessKey;
    this.testSecretKey = secretKey;
    this.testSessionToken = sessionToken;
  }

  /**
   * Get test credentials for /plan responses.
   * Always returns an array of 3 strings (may be empty strings if not configured).
   */
  public String[] getTestCredentials() {
    return new String[] { testAccessKey, testSecretKey, testSessionToken };
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

    return super.execute(
        request, responseType, errorHandler, responseHeaders, parserContext);
  }

  private boolean isPlanTableScanRequest(HTTPRequest request) {
    return HTTPRequest.HTTPMethod.POST.equals(request.method()) &&
           request.path().endsWith("/plan");
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

    // 1. Extract table identifier
    TableIdentifier tableIdent = extractTableIdentifier(request.path());
    LOG.debug("Table identifier: {}", tableIdent);

    // 2. Parse request
    PlanTableScanRequest planRequest = parsePlanRequest(request);
    LOG.debug("Plan request parsed: snapshotId={}", planRequest.snapshotId());

    // 3. Load table from catalog
    Table table = catalog.loadTable(tableIdent);
    LOG.debug("Table loaded: {}", table);

    // 4. Create table scan
    TableScan tableScan = table.newScan();

    // 5. Apply snapshot if specified and valid
    if (planRequest.snapshotId() != null && planRequest.snapshotId() != 0) {
      tableScan = tableScan.useSnapshot(planRequest.snapshotId());
      LOG.debug("Using snapshot: {}", planRequest.snapshotId());
    } else {
      LOG.debug("Using current snapshot (snapshotId was null or 0)");
    }

    // 6. Validate that unsupported features are not requested
    if (planRequest.filter() != null) {
      throw new UnsupportedOperationException(
          "Filter pushdown is not supported in this test implementation");
    }
    if (planRequest.select() != null && !planRequest.select().isEmpty()) {
      throw new UnsupportedOperationException(
          "Column selection/projection is not supported in this test implementation");
    }
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

    // 7. Execute scan planning
    List<FileScanTask> fileScanTasks = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = tableScan.planFiles()) {
      tasks.forEach(task -> fileScanTasks.add(task));
    }
    LOG.debug("Planned {} file scan tasks", fileScanTasks.size());

    // 8. Get partition specs for serialization
    Map<Integer, shadedForDelta.org.apache.iceberg.PartitionSpec> specsById = table.specs();
    LOG.debug("Table has {} partition specs", specsById.size());

    // 9. Build response (Pattern 1: COMPLETED with direct tasks)
    // Note: Servlet will inject test credentials into JSON if this is a /plan request
    return PlanTableScanResponse.builder()
        .withPlanStatus(PlanStatus.COMPLETED)
        .withFileScanTasks(fileScanTasks)
        .withSpecsById(specsById)
        .build();
  }
}
