package com.glab.flink.connector.clickhouse.table.internal.executor;

import com.glab.flink.connector.clickhouse.table.internal.ClickHouseStatementFactory;
import com.glab.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.glab.flink.connector.clickhouse.table.internal.connection.ClickHouseHttpConnectionProvider;
import com.glab.flink.connector.clickhouse.table.internal.converter.ClickHouseRowBinaryEncoder;
import com.glab.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.util.EntityUtils;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpRowBinaryWriter implements ClickHouseBulkWriter {
    private final ClickHouseOptions options;
    private final ClickHouseConnectionProvider connectionProvider;
    private final ClickHouseConnection directConnection;
    private final String targetUrl;
    private final String databaseName;
    private final String tableName;
    private final List<String> fieldNames;
    private final LogicalType[] logicalTypes;

    private ClickHouseHttpConnectionProvider httpConnectionProvider;
    private ClickHouseRowBinaryEncoder encoder;
    private String insertSql;

    public HttpRowBinaryWriter(ClickHouseOptions options,
                               ClickHouseConnectionProvider connectionProvider,
                               ClickHouseConnection directConnection,
                               String targetUrl,
                               String databaseName,
                               String tableName,
                               List<String> fieldNames,
                               LogicalType[] logicalTypes) {
        this.options = options;
        this.connectionProvider = connectionProvider;
        this.directConnection = directConnection;
        this.targetUrl = targetUrl == null ? options.getUrl() : targetUrl;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.logicalTypes = logicalTypes;
    }

    @Override
    public void open() throws Exception {
        this.httpConnectionProvider = new ClickHouseHttpConnectionProvider(this.options, this.targetUrl);
        this.insertSql = ClickHouseStatementFactory.getInsertFormatStatement(this.tableName, this.fieldNames, "RowBinary");
        this.encoder = new ClickHouseRowBinaryEncoder(this.logicalTypes, queryClickHouseTypes());
    }

    @Override
    public void write(List<RowData> rows) throws Exception {
        HttpPost httpPost = this.httpConnectionProvider.createInsertPost(this.databaseName, this.insertSql);
        httpPost.setEntity(createEntity(rows));
        try (CloseableHttpResponse response = this.httpConnectionProvider.getHttpClient().execute(httpPost)) {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode / 100 != 2) {
                String responseBody = response.getEntity() == null ? "" : EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                if (statusCode >= 400 && statusCode < 500) {
                    throw new NonRetryableClickHouseException("ClickHouse http-rowbinary insert failed, status=" + statusCode + ", body=" + responseBody);
                }
                throw new IOException("ClickHouse http-rowbinary insert failed, status=" + statusCode + ", body=" + responseBody);
            }
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }

    @Override
    public void reopen() throws Exception {
        close();
        open();
    }

    @Override
    public void close() throws IOException {
        if (this.httpConnectionProvider != null) {
            this.httpConnectionProvider.close();
            this.httpConnectionProvider = null;
        }
    }

    private HttpEntity createEntity(List<RowData> rows) {
        return new AbstractHttpEntity() {
            @Override
            public boolean isRepeatable() {
                return true;
            }

            @Override
            public long getContentLength() {
                return -1;
            }

            @Override
            public java.io.InputStream getContent() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void writeTo(OutputStream outputStream) throws IOException {
                encoder.encodeRows(rows, outputStream);
            }

            @Override
            public boolean isStreaming() {
                return true;
            }
        };
    }

    private List<String> queryClickHouseTypes() throws Exception {
        Map<String, String> typesByName = new HashMap<>();
        ClickHouseConnection metadataConnection = this.directConnection;
        boolean closeAfterUse = false;
        if (metadataConnection == null && this.connectionProvider != null) {
            metadataConnection = this.connectionProvider.getConnection();
        } else if (metadataConnection == null) {
            metadataConnection = this.connectionProvider.createNewConnection();
            closeAfterUse = true;
        }

        try (PreparedStatement statement = metadataConnection.prepareStatement(
                "SELECT name, type FROM system.columns WHERE database = ? AND table = ?")) {
            statement.setString(1, this.databaseName);
            statement.setString(2, this.tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    typesByName.put(resultSet.getString("name"), resultSet.getString("type"));
                }
            }
        } finally {
            if (closeAfterUse && metadataConnection != null) {
                metadataConnection.close();
            }
        }

        java.util.ArrayList<String> orderedTypes = new java.util.ArrayList<>(this.fieldNames.size());
        for (String fieldName : this.fieldNames) {
            String typeName = typesByName.get(fieldName);
            if (typeName == null) {
                throw new IllegalStateException("Unable to find ClickHouse type for field `" + fieldName + "`");
            }
            orderedTypes.add(typeName);
        }
        return orderedTypes;
    }
}
