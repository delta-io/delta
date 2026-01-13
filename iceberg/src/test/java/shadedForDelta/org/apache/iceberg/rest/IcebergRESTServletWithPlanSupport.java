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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import shadedForDelta.org.apache.iceberg.rest.responses.ConfigResponse;
import shadedForDelta.org.apache.iceberg.rest.responses.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension of RESTCatalogServlet that adds support for the /plan endpoint.
 */
public class IcebergRESTServletWithPlanSupport extends RESTCatalogServlet {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergRESTServletWithPlanSupport.class);

  private final RESTCatalogAdapter adapter;
  private final ObjectMapper mapper;

  public IcebergRESTServletWithPlanSupport(RESTCatalogAdapter adapter) {
    super(adapter);
    this.adapter = adapter;
    this.mapper = RESTObjectMapper.mapper();
  }

  /**
   * Override GET to handle /v1/config requests with catalog prefix.
   * Note: We handle this at servlet level because the shaded RESTCatalogAdapter
   * doesn't expose the server-side execute() method needed for interception.
   */
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws IOException {

    String path = req.getPathInfo();

    // Check if this is a /v1/config request
    if (path != null && (path.equals("/v1/config") || path.endsWith("/v1/config"))) {
      LOG.debug("Custom servlet handling /v1/config request");
      handleConfigRequest(req, resp);
    } else {
      // For all other GET requests, use standard handling
      super.doGet(req, resp);
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws IOException {

    String path = req.getPathInfo();

    // Check if this is a /plan endpoint request
    if (path != null && path.endsWith("/plan")) {
      LOG.debug("Custom servlet handling /plan request for path: {}", path);
      handlePlanRequest(req, resp);
    } else {
      // For all other requests, use standard handling
      super.doPost(req, resp);
    }
  }

  /**
   * Test helper for Iceberg REST /v1/config endpoint that returns optional catalog
   * prefix, following the Iceberg REST catalog spec pattern.
   */
  private void handleConfigRequest(HttpServletRequest req, HttpServletResponse resp)
      throws IOException {
    try {
      // Build ConfigResponse with prefix if adapter has one set
      ConfigResponse.Builder builder = ConfigResponse.builder();

      // If adapter is our custom type, get the prefix and add it to overrides
      if (adapter instanceof IcebergRESTCatalogAdapterWithPlanSupport) {
        IcebergRESTCatalogAdapterWithPlanSupport customAdapter =
            (IcebergRESTCatalogAdapterWithPlanSupport) adapter;
        String prefix = customAdapter.getCatalogPrefix();
        if (prefix != null && !prefix.isEmpty()) {
          LOG.info("Adding prefix to /v1/config response: {}", prefix);
          builder.withOverride("prefix", prefix);
        }
      }

      ConfigResponse config = builder.build();

      // Write JSON response
      resp.setStatus(200);
      resp.setContentType("application/json");
      mapper.writeValue(resp.getWriter(), config);

    } catch (Exception e) {
      LOG.error("Error handling /v1/config request: {}", e.getMessage(), e);
      resp.setStatus(500);
    }
  }

  private void handlePlanRequest(HttpServletRequest req, HttpServletResponse resp)
      throws IOException {
    try {
      // Extract request components
      String path = req.getPathInfo();
      // HTTPRequest paths should not start with /, so strip it
      if (path != null && path.startsWith("/")) {
        path = path.substring(1);
      }
      Map<String, String> headers = extractHeaders(req);
      Map<String, String> queryParams = extractQueryParams(req);
      String body = extractBody(req);

      LOG.debug("Plan request - path: {}", path);
      LOG.debug("Plan request - body: {}", body);

      // Build HTTPRequest - body should be kept as string, not parsed
      HTTPRequest httpRequest = adapter.buildRequest(
          HTTPRequest.HTTPMethod.POST,
          path,
          queryParams,
          headers,
          body  // Pass body as string, not parsed
      );

      // Set up response handling
      resp.setStatus(200);
      resp.setContentType("application/json");

      // Execute the request through the adapter
      RESTResponse response = adapter.execute(
          httpRequest,
          RESTResponse.class,
          error -> handleError(resp, error),
          responseHeaders -> responseHeaders.forEach((k, v) -> resp.setHeader(k, v))
      );

      // Write response
      if (response != null) {
        PrintWriter writer = resp.getWriter();
        mapper.writeValue(writer, response);
        writer.flush();
      }

    } catch (Exception e) {
      LOG.error("Error handling /plan request: {}", e.getMessage(), e);
      resp.setStatus(500);
      ErrorResponse error = ErrorResponse.builder()
          .responseCode(500)
          .withType("InternalServerError")
          .withMessage("Failed to process plan request: " + e.getMessage())
          .build();
      mapper.writeValue(resp.getWriter(), error);
    }
  }

  private void handleError(HttpServletResponse resp, ErrorResponse error) {
    try {
      resp.setStatus(error.code());
      mapper.writeValue(resp.getWriter(), error);
    } catch (IOException e) {
      LOG.error("Failed to write error response: {}", e.getMessage(), e);
    }
  }

  private Map<String, String> extractHeaders(HttpServletRequest req) {
    Map<String, String> headers = new HashMap<>();
    Enumeration<String> headerNames = req.getHeaderNames();
    while (headerNames.hasMoreElements()) {
      String name = headerNames.nextElement();
      headers.put(name, req.getHeader(name));
    }
    return headers;
  }

  private Map<String, String> extractQueryParams(HttpServletRequest req) {
    Map<String, String> params = new HashMap<>();
    if (req.getQueryString() != null) {
      for (String param : req.getQueryString().split("&")) {
        String[] pair = param.split("=", 2);
        if (pair.length == 2) {
          params.put(pair[0], pair[1]);
        }
      }
    }
    return params;
  }

  private String extractBody(HttpServletRequest req) throws IOException {
    BufferedReader reader = req.getReader();
    return reader.lines().collect(Collectors.joining());
  }
}
