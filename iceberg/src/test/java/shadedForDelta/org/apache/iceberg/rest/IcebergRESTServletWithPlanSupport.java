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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import shadedForDelta.org.apache.iceberg.rest.responses.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension of RESTCatalogServlet that adds support for the /plan endpoint
 * which is not available in the base test servlet.
 *
 * This servlet intercepts requests to /plan and directly invokes the adapter's
 * execute() method, bypassing the route validation in the parent servlet.
 */
public class IcebergRESTServletWithPlanSupport extends RESTCatalogServlet {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergRESTServletWithPlanSupport.class);

  private final IcebergRESTCatalogAdapterWithPlanSupport adapter;
  private final ObjectMapper mapper;

  public IcebergRESTServletWithPlanSupport(RESTCatalogAdapter adapter) {
    super(adapter);
    this.adapter = (IcebergRESTCatalogAdapterWithPlanSupport) adapter;
    this.mapper = RESTObjectMapper.mapper();
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

      // Write response with injected credentials
      if (response != null) {
        // Get test credentials from adapter
        String[] credentials = adapter.getTestCredentials();

        // Serialize response to JSON
        String jsonResponse = mapper.writeValueAsString(response);

        // Parse to Jackson node for modification
        ObjectNode jsonNode = (ObjectNode) mapper.readTree(jsonResponse);

        // Build storage-credentials structure
        ObjectNode config = mapper.createObjectNode();
        config.put("s3.access-key-id", credentials[0]);
        config.put("s3.secret-access-key", credentials[1]);
        config.put("s3.session-token", credentials[2]);

        ObjectNode credential = mapper.createObjectNode();
        credential.put("type", "aws");
        credential.set("config", config);

        ArrayNode storageCredentials = mapper.createArrayNode();
        storageCredentials.add(credential);

        // Add storage-credentials to response JSON
        jsonNode.set("storage-credentials", storageCredentials);

        // Write modified JSON
        PrintWriter writer = resp.getWriter();
        mapper.writeValue(writer, jsonNode);
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
