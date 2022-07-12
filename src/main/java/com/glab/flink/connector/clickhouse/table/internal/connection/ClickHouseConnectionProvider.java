package com.glab.flink.connector.clickhouse.table.internal.connection;

import com.glab.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClickHouseConnectionProvider implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseConnectionProvider.class);

    private static final String CLICKHOUSE_DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";

    private static final Pattern PATTERN = Pattern.compile("You must use port (?<port>[0-9]+) for HTTP.");

    //jdbc url模板
    private static final Pattern URL_TEMPLATE = Pattern.compile("clickhouse:" + "" +
            "//([a-zA-Z0-9_:,.-]+)" +
            "(/[a-zA-Z0-9_]+" +
            "([?][a-zA-Z0-9_]+[=][a-zA-Z0-9_]+([&][a-zA-Z0-9_]+[=][a-zA-Z0-9_]+)*)?" +
            ")?");

    private transient ClickHouseConnection connection;

    private transient List<ClickHouseConnection> shardConnections;

    private final ClickHouseOptions options;

    public ClickHouseConnectionProvider(ClickHouseOptions options) {
        this.options = options;
    }

    public synchronized ClickHouseConnection getConnection() throws SQLException {
        if(this.connection == null) {
            this.connection = createConnection(this.options.getUrl(), this.options.getDatabaseName());
        }
        return this.connection;
    }

    /**
     * 暂支持单个url例如：clickhouse://192.168.8.94:8123，后续根据需求支持多个clickhouse://192.168.8.94:8123；192.168.8.95:8123
     * @param url
     * @param dbName
     * @return
     * @throws SQLException
     */
    private ClickHouseConnection createConnection(String url, String dbName) throws SQLException {
        //String url = parseUrl(urls);

        ClickHouseConnection conn;
        LOG.info("connection to {}", url);


        try {
            Class.forName(CLICKHOUSE_DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }

        if(options.getUsername().isPresent()) {
            conn = (ClickHouseConnection) DriverManager.getConnection(getJdbcUrl(url, dbName),
                    options.getUsername().orElse(null), options.getPassword().orElse(null));
        } else {
            conn = (ClickHouseConnection) DriverManager.getConnection(getJdbcUrl(url, dbName));
        }

        return conn;
    }

    //多个url拆分
    private String parseUrl(String urls) {
        Matcher matcher = URL_TEMPLATE.matcher(urls);
        if(!matcher.matches()) {
            throw new IllegalArgumentException("Incorrect url!");
        }
        return "";
    }

    /**
     * 如果采用的是插入单机表模式，分别获取每台机器的jdbc连接
     * @param remoteCluster
     * @param remoteDataBase
     * @return
     * @throws SQLException
     */
    public synchronized List<ClickHouseConnection> getShardConnections(String remoteCluster, String remoteDataBase) throws SQLException{
        if(this.shardConnections == null) {
            ClickHouseConnection conn = this.getConnection();
            String shardSql = String.format("SELECT shard_num, host_address, port FROM system.clusters WHERE cluster = '%s'", remoteCluster);
            //查询ck集群各个分片信息
            PreparedStatement stmt = conn.prepareStatement(shardSql);

            ResultSet rs = stmt.executeQuery();
            try {
                this.shardConnections = new ArrayList<>();
                while(rs.next()) {
                    String host_address = rs.getString("host_address");
                    int port = getActualHttpPort(host_address, rs.getInt("port"));
                    String url = "clickhouse://" + host_address + ":" + port;
                    this.shardConnections.add(createConnection(url, remoteDataBase));
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if(rs != null) {
                    rs.close();
                }
            }

            if(this.shardConnections.isEmpty()) {
                throw new SQLException("unable to query shards in system.clusters");
            }
        }

        return this.shardConnections;
    }

    private int getActualHttpPort(String host_address, int port) throws Exception{
        try(CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet((new URIBuilder()).setScheme("http").setHost(host_address).setPort(port).build());
            CloseableHttpResponse closeableHttpResponse = httpClient.execute((HttpUriRequest) request);
            int statusCode = closeableHttpResponse.getStatusLine().getStatusCode();
            if(statusCode == 200) {
                return port;
            }

            String raw = EntityUtils.toString(closeableHttpResponse.getEntity());
            Matcher matcher = PATTERN.matcher(raw);
            if(matcher.find()) {
                return Integer.parseInt(matcher.group("port"));
            }
            throw new SQLException("Cannot query ClickHouse http port");
        } catch (Exception e) {
          throw new SQLException("Cannot connect to ClickHouse server using HTTP", e);
        }
    }

    private String getJdbcUrl(String url, String dbName) throws SQLException{
        try {
            return "jdbc:" + (new URIBuilder(url)).setPath("/" + dbName).build().toString();
        }catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public void closeConnection() throws SQLException{
        if(this.connection != null) {
            this.connection.close();
        }
        if(this.shardConnections != null) {
            for (ClickHouseConnection shardConnection : shardConnections) {
                shardConnection.close();
            }
        }
    }

    /**
     * 根据WITH中传入的distributed表名称获取单机表名称相关信息
     * @param databaseName
     * @param tableName
     * @return
     * @throws SQLException
     */
    public String queryTableEngine(String databaseName, String tableName) throws SQLException {
        ClickHouseConnection conn = getConnection();
        try (PreparedStatement stmt = conn.prepareStatement("SELECT engine_full FROM system.tables WHERE database = ? AND name = ?")) {
            stmt.setString(1, databaseName);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next())
                    return rs.getString("engine_full");
            }
        }
        throw new SQLException("table `" + databaseName + "`.`" + tableName + "` does not exist");
    }

}
