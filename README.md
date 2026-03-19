当前版本最推荐的使用方式是：
1. append-only 数据同步。
2. ClickHouse 集群写入优先采用本地表直写。
3. sink 模式使用 clickhouse-friendly。
4. 底层 writer 使用 http-rowbinary。
5. 使用大批次、低并发 in-flight 的写入策略，尽量减少 ClickHouse 小 part 和 merge 压力。

二、最佳实践结论
推荐组合如下：
1. sink.mode = clickhouse-friendly
2. sink.writer-type = http-rowbinary
3. sink.ignore-delete = true
4. sink.write-local = true
5. sink.partition-strategy = hash
6. sink.partition-key = 与 ClickHouse 分片键一致
7. sink.batch-size = 50000 到 100000，优先从 100000 开始压测
8. sink.batch-bytes = 64MB
9. sink.flush-interval = 10s
10. sink.max-inflight-batches = 1
11. sink.prefer-large-batch = true

三、推荐 DDL 模板

CREATE TABLE ck_sink (
  id BIGINT,
  user_id BIGINT,
  event_time TIMESTAMP(3),
  amount DECIMAL(18, 2),
  payload STRING
) WITH (
  'connector' = 'clickhouse',
  'url' = 'jdbc:clickhouse://ck01:8123',
  'database-name' = 'ods',
  'table-name' = 'events_local',
  'username' = 'default',
  'password' = '***',

  'sink.mode' = 'clickhouse-friendly',
  'sink.writer-type' = 'http-rowbinary',
  'sink.ignore-delete' = 'true',

  'sink.write-local' = 'true',
  'sink.partition-strategy' = 'hash',
  'sink.partition-key' = 'user_id',

  'sink.batch-size' = '100000',
  'sink.batch-bytes' = '67108864',
  'sink.flush-interval' = '10 s',
  'sink.max-buffered-rows' = '300000',
  'sink.max-inflight-batches' = '1',
  'sink.flush-thread-num' = '1',
  'sink.prefer-large-batch' = 'true',
  'sink.partial-flush-min-rows' = '20000',

  'sink.max-retries' = '3',
  'sink.http.connect-timeout' = '10 s',
  'sink.http.socket-timeout' = '30 s'
);

四、全部配置参数说明

1. 连接与基础参数

1) connector
含义：连接器名称。
默认值：无。
推荐值：clickhouse。
备注：Flink SQL DDL 中必须指定。

2) url
含义：ClickHouse 连接地址。
默认值：无。
推荐值：jdbc:clickhouse://host:8123。
备注：当前项目内部 JDBC 与 HTTP bulk writer 都基于该地址衍生连接能力。

3) database-name
含义：目标数据库名。
默认值：default。
推荐值：显式填写业务库名。

4) table-name
含义：目标表名。
默认值：无。
推荐值：显式填写。

5) username
含义：ClickHouse 用户名。
默认值：无。
推荐值：显式填写。
备注：如果设置 username，则必须同时设置 password。

6) password
含义：ClickHouse 密码。
默认值：无。
推荐值：显式填写。

2. Sink 运行模式参数

7) sink.mode
含义：sink 工作模式。
可选值：normal, clickhouse-friendly。
默认值：normal。
推荐值：clickhouse-friendly。
说明：
- normal：保持通用模式。
- clickhouse-friendly：自动采用更适合 ClickHouse 大批量写入的默认策略。

8) sink.writer-type
含义：底层写入实现。
可选值：jdbc, http-rowbinary。
默认值：jdbc。
推荐值：http-rowbinary。
说明：
- jdbc：兼容模式，仍使用 JDBC 批写。
- http-rowbinary：append-only 场景下推荐，性能和 ClickHouse 友好性更好。
限制：
- 当 sink.writer-type=http-rowbinary 时，必须 sink.ignore-delete=true。

3. 批次控制参数

9) sink.batch-size
含义：单批最大行数，达到该行数触发 flush。
默认值：1000。
推荐值：50000 到 100000。
推荐起点：100000。
说明：
- 对 ClickHouse 来说，批次越大，越容易减少小 part。
- 如果单行非常大，可结合 batch-bytes 调整。

10) sink.batch-bytes
含义：单批最大字节数，达到阈值触发 flush。
默认值：0，表示关闭。
推荐值：67108864，即 64MB。
说明：
- 用于避免只按行数切批。
- 当单行记录大小波动很大时，建议必须开启。

11) sink.flush-interval
含义：时间兜底 flush 间隔。
默认值：1s。
推荐值：10s。
说明：
- 这是兜底阈值，不应成为主导 flush 的条件。
- 如果设置过短，会把大批次切碎，增加 ClickHouse merge 压力。

12) sink.max-buffered-rows
含义：内存中最多允许缓存的记录数，超过后施加背压。
默认值：0。
实际解释：0 表示运行时按 sink.batch-size * 3 兜底。
推荐值：300000。
说明：
- 用于控制 Flink sink 端内存堆积。
- 建议至少是 batch-size 的 3 倍。

13) sink.max-inflight-batches
含义：同时处于排队或写入中的批次数。
默认值：1。
推荐值：1。
说明：
- 该参数非常关键。
- 对 ClickHouse，不建议并发发送多个小批次。
- 多数情况下保持 1 最稳妥。

14) sink.flush-thread-num
含义：flush worker 线程数。
默认值：1。
推荐值：1。
说明：
- 当前 clickhouse-friendly 最佳实践不建议靠增加 flush 并发来提吞吐。
- 真正目标是形成更大的单批次，而不是更多并发小批次。

15) sink.prefer-large-batch
含义：时间阈值触发时，如果当前批次太小，允许延后一次 flush，尽量凑大批次。
默认值：false。
推荐值：true。
说明：
- 这是 ClickHouse 友好模式中的关键参数之一。
- 建议在大吞吐写入场景始终开启。

16) sink.partial-flush-min-rows
含义：时间触发 flush 时希望至少达到的最小行数。
默认值：0。
实际解释：0 表示运行时回退为 sink.batch-size / 5。
推荐值：20000。
说明：
- 当 prefer-large-batch=true 时，该值用于抑制过小时间批次。
- 在 clickhouse-friendly 模式下，内部默认会取 max(20000, batch-size/5)。

17) sink.max-retries
含义：sink 写入失败时的最大重试次数。
默认值：3。
推荐值：3。
说明：
- 失败重试不应作为常态策略。
- 如果频繁重试，优先检查 ClickHouse 端负载与批次策略。

4. HTTP RowBinary Writer 参数

18) sink.http.connect-timeout
含义：http-rowbinary writer 的连接超时时间。
默认值：10s。
推荐值：10s。
说明：
- 集群网络较复杂时可适当增加。

19) sink.http.socket-timeout
含义：http-rowbinary writer 的读写超时时间。
默认值：30s。
推荐值：30s 到 60s。
说明：
- 大批次写入时如果网络或服务端处理稍慢，可以酌情提高。

5. 本地表直写与分片参数

20) sink.write-local
含义：是否将 Distributed 表写入转为本地表直写。
默认值：false。
推荐值：true。
说明：
- 在 ClickHouse 集群场景下，推荐开启。
- 这样可以由 Flink 端按 shard 路由直接打到本地表，减少 Distributed 层带来的批次打散。

21) sink.partition-strategy
含义：本地表写入时的分片策略。
可选值：balanced, hash, shuffle。
默认值：balanced。
推荐值：hash。
说明：
- balanced：较均衡地选择分片。
- shuffle：随机。
- hash：按分片键稳定路由。
最佳实践：
- 如果是生产场景，优先 hash。
- hash 使用的字段应与 ClickHouse 的分片键尽量一致。

22) sink.partition-key
含义：hash 分片策略使用的字段名。
默认值：无。
推荐值：与 ClickHouse 分片键一致。
说明：
- 仅在 sink.partition-strategy=hash 时必须设置。
- 如果未设置会在建表时直接失败。

6. 变更语义参数

23) sink.ignore-delete
含义：是否忽略 DELETE，并将 UPDATE 按 INSERT 处理。
默认值：true。
推荐值：true。
说明：
- 这是 append-only 场景的关键参数。
- ClickHouse 本身不适合高频 mutation。
- http-rowbinary 路径明确要求该值为 true。

7. Lookup 参数

24) lookup.cache.max-rows
含义：lookup 缓存最大行数。
默认值：-1。
推荐值：按 lookup 场景决定。
说明：
- 仅在 source/lookup 使用场景下相关。
- 与 sink 吞吐优化无直接关系。

25) lookup.cache.ttl
含义：lookup 缓存生存时间。
默认值：10s。
推荐值：10s 到 60s。

26) lookup.max-retries
含义：lookup 失败最大重试次数。
默认值：3。
推荐值：3。

五、clickhouse-friendly 模式下的自动推荐值
如果设置：
'sink.mode' = 'clickhouse-friendly'

且你没有显式覆盖这些参数，当前实现会自动采用以下值：
1. sink.batch-size = 100000
2. sink.batch-bytes = 67108864，即 64MB
3. sink.flush-interval = 10s
4. sink.max-buffered-rows = 300000
5. sink.max-inflight-batches = 1
6. sink.flush-thread-num = 1
7. sink.prefer-large-batch = true
8. sink.partial-flush-min-rows = max(20000, batch-size / 5)
9. sink.writer-type = http-rowbinary
10. sink.write-local = true
11. sink.ignore-delete = true

六、最佳参数组合建议

1. 中高吞吐 append-only 场景
推荐：
1) sink.mode = clickhouse-friendly
2) sink.writer-type = http-rowbinary
3) sink.write-local = true
4) sink.partition-strategy = hash
5) sink.partition-key = 分片键
6) sink.batch-size = 100000
7) sink.batch-bytes = 64MB
8) sink.flush-interval = 10s
9) sink.max-inflight-batches = 1
10) sink.prefer-large-batch = true

2. 单机或小规模验证场景
推荐：
1) sink.writer-type = jdbc 或 http-rowbinary 均可
2) sink.batch-size = 5000 到 20000
3) sink.flush-interval = 3s 到 5s
4) sink.max-inflight-batches = 1

3. 兼容性优先场景
推荐：
1) sink.writer-type = jdbc
2) 仍建议保留较大的 batch-size
3) 仍建议 max-inflight-batches = 1
说明：
- 如果你的字段类型里包含当前 RowBinary 尚未覆盖的复杂类型，先用 jdbc 更稳。

七、不推荐的配置组合
1. sink.writer-type = http-rowbinary 且 sink.ignore-delete = false
原因：当前实现只支持 append-only 语义，会直接校验失败。

2. sink.flush-interval 非常短，例如 1s 或更短
原因：容易持续产生小批次，最终把 ClickHouse 的 part 和 merge 压力打高。

3. sink.max-inflight-batches > 1
原因：对 ClickHouse 通常收益小，风险大，容易让多个批次并发落地，增加 part 和后台 merge 压力。

4. sink.partition-strategy = balanced 但业务表本身存在明显的 shard key
原因：这样会削弱本地表直写的稳定路由效果。

八、当前 http-rowbinary 支持范围
当前优先支持以下 ClickHouse 类型：
1. Bool
2. Int8 / UInt8
3. Int16 / UInt16
4. Int32 / UInt32
5. Int64 / UInt64
6. Float32 / Float64
7. String
8. FixedString
9. Date
10. Date32
11. DateTime
12. DateTime64
13. Decimal32 / Decimal64 / Decimal128 / Decimal256
14. Nullable(以上类型)

当前不建议直接依赖的复杂类型：
1. Array
2. Map
3. Tuple / Row
4. 其他复杂嵌套类型

如果表结构包含上述复杂类型，建议先使用 jdbc writer，或继续扩展 RowBinary encoder 后再启用 http-rowbinary。

九、监控与调优建议
优化后的 connector 已经补充了关键 metrics，建议重点观察：
1. pendingRows
   含义：当前还未写出的缓存行数。
2. pendingBytes
   含义：当前缓存字节量。
3. inFlightBatches
   含义：当前在途批次数。
4. lastFlushRows
   含义：上一次 flush 的行数。
5. lastFlushBytes
   含义：上一次 flush 的字节数。
6. lastFlushLatencyMs
   含义：上一次 flush 时延。
7. flushCount
   含义：flush 次数。
8. retryCount
   含义：重试次数。
9. writeErrorCount
   含义：写错误次数。
10. smallBatchFlushCount
    含义：小批次 flush 次数。
