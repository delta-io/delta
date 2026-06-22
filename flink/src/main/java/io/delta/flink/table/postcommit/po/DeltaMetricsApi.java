/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.table.postcommit.po;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.ApiResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Hand-written API class for {@code POST /delta/preview/metrics}, following the same structure as
 * the OpenAPI-generated {@code *Api} classes (e.g. {@code TablesApi}, {@code SchemasApi}).
 *
 * <p>This class is intentionally modeled after the generated code so that it benefits from the same
 * {@link ApiClient} infrastructure: authentication, retry via {@code RetryingHttpClient},
 * User-Agent headers, read timeouts, and response interceptors.
 *
 * <p>Usage (identical to any generated API class):
 *
 * <pre>{@code
 * ApiClient apiClient = ApiClientBuilder.create()
 *     .uri("http://localhost:8080")
 *     .tokenProvider(TokenProvider.create(config))
 *     .build();
 *
 * DeltaMetricsApi metricsApi = new DeltaMetricsApi(apiClient);
 * metricsApi.reportMetrics(new ReportDeltaMetricsRequest(tableId, report));
 * }</pre>
 */
public class DeltaMetricsApi {

  private final HttpClient memberVarHttpClient;
  private final ObjectMapper memberVarObjectMapper;
  private final String memberVarBaseUri;
  private final Consumer<HttpRequest.Builder> memberVarInterceptor;
  private final Duration memberVarReadTimeout;
  private final Consumer<HttpResponse<InputStream>> memberVarResponseInterceptor;

  public DeltaMetricsApi(ApiClient apiClient) {
    memberVarHttpClient = apiClient.getHttpClient();
    memberVarObjectMapper = apiClient.getObjectMapper();
    memberVarBaseUri = apiClient.getBaseUri();
    memberVarInterceptor = apiClient.getRequestInterceptor();
    memberVarReadTimeout = apiClient.getReadTimeout();
    memberVarResponseInterceptor = apiClient.getResponseInterceptor();
  }

  // ---------------------------------------------------------------------------
  //  reportMetrics — POST /delta/preview/metrics
  // ---------------------------------------------------------------------------

  /**
   * Report commit metrics for a UC-managed Delta table.
   *
   * @param reportDeltaMetricsRequest the metrics request payload (required)
   * @throws ApiException if the API call fails
   */
  public void reportMetrics(ReportDeltaMetricsRequest reportDeltaMetricsRequest)
      throws ApiException {
    reportMetrics(reportDeltaMetricsRequest, null);
  }

  /**
   * Report commit metrics for a UC-managed Delta table.
   *
   * @param reportDeltaMetricsRequest the metrics request payload (required)
   * @param headers optional additional headers to include in the request
   * @throws ApiException if the API call fails
   */
  public void reportMetrics(
      ReportDeltaMetricsRequest reportDeltaMetricsRequest, Map<String, String> headers)
      throws ApiException {
    reportMetricsWithHttpInfo(reportDeltaMetricsRequest, headers);
  }

  /**
   * Report commit metrics (with full HTTP response info).
   *
   * @param reportDeltaMetricsRequest the metrics request payload (required)
   * @return ApiResponse wrapping the HTTP status, headers, and response
   * @throws ApiException if the API call fails
   */
  public ApiResponse<ReportDeltaMetricsResponse> reportMetricsWithHttpInfo(
      ReportDeltaMetricsRequest reportDeltaMetricsRequest) throws ApiException {
    return reportMetricsWithHttpInfo(reportDeltaMetricsRequest, null);
  }

  /**
   * Report commit metrics (with full HTTP response info).
   *
   * @param reportDeltaMetricsRequest the metrics request payload (required)
   * @param headers optional additional headers to include in the request
   * @return ApiResponse wrapping the HTTP status, headers, and response
   * @throws ApiException if the API call fails
   */
  public ApiResponse<ReportDeltaMetricsResponse> reportMetricsWithHttpInfo(
      ReportDeltaMetricsRequest reportDeltaMetricsRequest, Map<String, String> headers)
      throws ApiException {
    HttpRequest.Builder localVarRequestBuilder =
        reportMetricsRequestBuilder(reportDeltaMetricsRequest, headers);
    try {
      HttpResponse<InputStream> localVarResponse =
          memberVarHttpClient.send(
              localVarRequestBuilder.build(), HttpResponse.BodyHandlers.ofInputStream());
      if (memberVarResponseInterceptor != null) {
        memberVarResponseInterceptor.accept(localVarResponse);
      }
      InputStream localVarResponseBody = null;
      try {
        if (localVarResponse.statusCode() / 100 != 2) {
          throw getApiException("reportMetrics", localVarResponse);
        }
        localVarResponseBody = localVarResponse.body();
        if (localVarResponseBody != null) {
          localVarResponseBody.readAllBytes();
        }
        return new ApiResponse<>(
            localVarResponse.statusCode(),
            localVarResponse.headers().map(),
            new ReportDeltaMetricsResponse());
      } finally {
        if (localVarResponseBody != null) {
          localVarResponseBody.close();
        }
      }
    } catch (IOException e) {
      throw new ApiException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ApiException(e);
    }
  }

  private HttpRequest.Builder reportMetricsRequestBuilder(
      ReportDeltaMetricsRequest reportDeltaMetricsRequest, Map<String, String> headers)
      throws ApiException {
    if (reportDeltaMetricsRequest == null) {
      throw new ApiException(
          400,
          "Missing the required parameter 'reportDeltaMetricsRequest'"
              + " when calling reportMetrics");
    }

    HttpRequest.Builder localVarRequestBuilder = HttpRequest.newBuilder();

    String localVarPath = "/delta/preview/metrics";
    localVarRequestBuilder.uri(URI.create(memberVarBaseUri + localVarPath));

    localVarRequestBuilder.header("Content-Type", "application/json");
    localVarRequestBuilder.header("Accept", "application/json");

    try {
      byte[] localVarPostBody = memberVarObjectMapper.writeValueAsBytes(reportDeltaMetricsRequest);
      localVarRequestBuilder.method(
          "POST", HttpRequest.BodyPublishers.ofByteArray(localVarPostBody));
    } catch (IOException e) {
      throw new ApiException(e);
    }

    if (memberVarReadTimeout != null) {
      localVarRequestBuilder.timeout(memberVarReadTimeout);
    }
    if (headers != null) {
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        localVarRequestBuilder.header(entry.getKey(), entry.getValue());
      }
    }
    if (memberVarInterceptor != null) {
      memberVarInterceptor.accept(localVarRequestBuilder);
    }
    return localVarRequestBuilder;
  }

  // ---------------------------------------------------------------------------
  //  Error handling (same as generated code)
  // ---------------------------------------------------------------------------

  protected ApiException getApiException(String operationId, HttpResponse<InputStream> response)
      throws IOException {
    String body = response.body() == null ? null : new String(response.body().readAllBytes());
    String message = formatExceptionMessage(operationId, response.statusCode(), body);
    return new ApiException(response.statusCode(), message, response.headers(), body);
  }

  private String formatExceptionMessage(String operationId, int statusCode, String body) {
    if (body == null || body.isEmpty()) {
      body = "[no body]";
    }
    return operationId + " call failed with: " + statusCode + " - " + body;
  }
}
