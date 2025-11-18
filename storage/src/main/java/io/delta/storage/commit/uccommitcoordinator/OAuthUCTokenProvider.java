package io.delta.storage.commit.uccommitcoordinator;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class OAuthUCTokenProvider implements UCTokenProvider, AutoCloseable{
    public static final long renewLeadTimeMillis = 30_000L;
    private final String uri;
    private final String clientId;
    private final String clientSecret;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper mapper;

    private volatile TemporaryToken tempToken;

    public OAuthUCTokenProvider(String uri, String clientId, String clientSecret) {
        this.uri = uri;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.httpClient = HttpClientBuilder.create().build();

        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    }

    @Override
    public String accessToken() {
        if(tempToken == null || tempToken.isReadyToRenew()) {
            synchronized (this) {
                if(tempToken == null || tempToken.isReadyToRenew()) {
                    try {
                        tempToken = requestToken();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return tempToken.token();
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
    }

    private TemporaryToken requestToken() throws IOException {
        HttpPost request = new HttpPost(uri);
        request.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
        request.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + Base64.getEncoder().encodeToString(String.format("%s:%s", clientId, clientSecret).getBytes(StandardCharsets.UTF_8)));

        String requestBody = mapToUrlEncoded(Map.of(
                "grant_type", "client_credentials",
                "scope", "all-apis"));
        request.setEntity(new StringEntity(requestBody, StandardCharsets.UTF_8));

        try(CloseableHttpResponse response = httpClient.execute(request)){
            StatusLine statusLine = response.getStatusLine();
            String body = EntityUtils.toString(response.getEntity());
            if(statusLine.getStatusCode() == HttpStatus.SC_OK){
                Map<String, String> result = mapper.readValue(body, new TypeReference<>() {});
                String accessToken = result.get("access_token");
                long expiresInSeconds = Long.parseLong(result.get("expires_in"));
                return new TemporaryToken(accessToken, expiresInSeconds * 1000L);
            } else{
                throw new IOException(String.format("Failed to obtain access token from %s, status code: %s, body: %s", uri, statusLine.getStatusCode(), body));
            }
        }
    }

    private static String mapToUrlEncoded(Map<String, String> params) {
        return params.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .reduce((a, b) -> a + "&" + b).orElse("");
    }

    public static class TemporaryToken {
        private final String token;
        private final long expirationTimeMillis;

        public TemporaryToken(String token, long expirationTimeMillis) {
            this.token = token;
            this.expirationTimeMillis = expirationTimeMillis;
        }

        public String token() {
            return token;
        }

        public long expirationTimeMillis(){
            return expirationTimeMillis;
        }

        public boolean isReadyToRenew(){
            return  System.currentTimeMillis() + renewLeadTimeMillis >= expirationTimeMillis;
        }
    }
}
