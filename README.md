flink-connector-clickhouse

flink版本1.13

支持sink和source

    DROP TABLE if exists test.lbs_ck;
    CREATE TABLE if not exists test.lbs_ck (
       ts BIGINT,
       id STRING,
       geohash12 STRING,
       loc_type STRING,
       wifimac STRING,
       id_type STRING,
       .....
       address STRING，
       PRIMARY KEY(ts, id) NOT ENFORCED
    ) WITH (
        'connector' = 'clickhouse',  -- 使用 ck connector
        'url' = 'clickhouse://xxxxx:8123',  --集群中任意一台
        'username' = '',  
        'password' = '',  
        'database-name' = 'test', 
        'table-name' = 'lbs',  
        -----以下为sink参数------
        'sink.batch-size' = '1000',  -- 批量插入数量
        'sink.flush-interval' = '',  --刷新时间,默认1s
        'sink.max-retries' = '3',  --最大重试次数
        'sink.partition-strategy' = 'balanced', --插入策略hash\balanced\shuffle
        --'sink.write-local' = 'false',--是否写入本地表
        'sink.ignore-delete' = 'true',
        -----以下为source参数-----
        'lookup.cache.max-rows' = '100',
        'lookup.cache.ttl' = '10',
        'lookup.max-retries' = '3'
    );

    CREATE TABLE test.lbs (
        ts BIGINT,
        id STRING,
        geohash12 STRING,
        loc_type STRING,
        wifimac STRING,
        id_type STRING,
        .....
        address STRING,
        row_timestamp as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),--需要将bigint时间转为flink的timestamp
        proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列
        WATERMARK FOR row_timestamp as row_timestamp - INTERVAL '5' SECOND  -- 在ts上定义watermark，ts成为事件时间列
    ) WITH (
        'connector' = 'kafka',  -- 使用 kafka connector
        'topic' = 'LBS',  
        'scan.startup.mode' = 'latest-offfset',  
        --'scan.startup.mode' = 'earliest-offset',  
        'properties.group.ib' = 'group1',  
        'properties.bootstrap.servers' = 'xxxx1:9092,xxxx2:9092',  -- kafka broker 地址
        'format.type' = 'csv',  -- 数据源格式为 csv
        'csv.disable-quote-character' = 'true',
        'csv.ignore-parser-errors' = 'false',
        'csv.field-delimiter' = '|',
        'csv.null-literal' = ''
    );

    insert into test.lbs_ck select ..... from test.lbs
