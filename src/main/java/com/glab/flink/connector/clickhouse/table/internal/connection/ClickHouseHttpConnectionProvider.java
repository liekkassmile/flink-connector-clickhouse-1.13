package com.glab.flink.connector.clickhouse.table.internal.connection;

import com.glab.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.utils.URIBuilder;

import java.io.Closeable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class ClickHouseHttpConnectionProvider implements Closeable {
    private final ClickHouseOptions options;
    private final String targetUrl;

    private final CloseableHttpClient httpClient;

    public ClickHouseHttpConnectionProvider(ClickHouseOptions options, String targetUrl) {
        this.options = options;
        this.targetUrl = targetUrl;
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout((int) options.getHttpConnectTimeout().toMillis())
                .setSocketTimeout((int) options.getHttpSocketTimeout().toMillis())
                .build();
        this.httpClient = HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .build();
    }

    public CloseableHttpClient getHttpClient() {
        return this.httpClient;
    }

    public HttpPost createInsertPost(String databaseName, String insertSql) throws Exception {
        URI uri = buildUri(databaseName, insertSql);
        HttpPost httpPost = new HttpPost(uri);
        if (this.options.getUsername().isPresent()) {
            String auth = this.options.getUsername().orElse("") + ":" + this.options.getPassword().orElse("");
            String authHeader = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
            httpPost.setHeader("Authorization", "Basic " + authHeader);
        }
        return httpPost;
    }

    private URI buildUri(String databaseName, String insertSql) throws Exception {
        String baseUrl = this.targetUrl;
        if (baseUrl.startsWith("jdbc:")) {
            baseUrl = baseUrl.substring("jdbc:".length());
        }
        return new URIBuilder(baseUrl)
                .setPath("/")
                .addParameter("database", databaseName)
                .addParameter("query", insertSql)
                .build();
    }

    @Override
    public void close() throws java.io.IOException {
        this.httpClient.close();
    }
}
