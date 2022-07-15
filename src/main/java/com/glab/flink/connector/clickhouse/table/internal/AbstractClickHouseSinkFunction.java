package com.glab.flink.connector.clickhouse.table.internal;

import com.glab.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.glab.flink.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import com.glab.flink.connector.clickhouse.table.internal.executor.ClickHouseBatchExecutor;
import com.glab.flink.connector.clickhouse.table.internal.executor.ClickHouseExecutor;
import com.glab.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import com.glab.flink.connector.clickhouse.table.internal.partitioner.ClickHousePartitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public abstract class AbstractClickHouseSinkFunction extends RichSinkFunction<RowData> implements Flushable {
    private static final long serialVersionUID = 1L;

    public AbstractClickHouseSinkFunction(){}

    public void configure(Configuration param){}

    public static class Builder{
        private static final Logger LOG = LoggerFactory.getLogger(AbstractClickHouseSinkFunction.Builder.class);

        private List<DataType> fieldDataTypes;
        private ClickHouseOptions options;
        private List<String> fieldNames;
        private Optional<UniqueConstraint> primaryKey;
        private TypeInformation<RowData> rowDataTypeInformation;

        public Builder(){}

        public Builder withOptions(ClickHouseOptions options) {
            this.options = options;
            return this;
        }

        public Builder withFieldDataTypes(List<DataType> fieldDataTypes) {
            this.fieldDataTypes = fieldDataTypes;
            return this;
        }

        public Builder withFieldNames(List<String> fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder withRowDataTypeInfo(TypeInformation<RowData> rowDataTypeInfo) {
            this.rowDataTypeInformation = rowDataTypeInfo;
            return this;
        }

        //UniqueConstraint用的是flink1.13的
        public Builder withPrimaryKey(Optional<UniqueConstraint> primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }


        public AbstractClickHouseSinkFunction build() {
            Preconditions.checkNotNull(this.options);
            Preconditions.checkNotNull(this.fieldNames);
            Preconditions.checkNotNull(this.fieldDataTypes);
            LogicalType[] logicalTypes = fieldDataTypes.stream().map(DataType::getLogicalType).toArray(a -> new LogicalType[a]);
            ClickHouseRowConverter converter = new ClickHouseRowConverter(RowType.of(logicalTypes));
            if (this.primaryKey.isPresent()) {
                LOG.warn("If primary key is specified, connector will be in UPSERT mode.");
                LOG.warn("You will have significant performance loss.");
            }

            //如果是写入本地表
            return this.options.getWriteLocal() ? this.createShardSinkFunction(logicalTypes, converter) : createBatchSinkFunction(converter);
        }

        /**
         * 插入集群表
         * @param converter
         * @return
         */
        private ClickHouseBatchSinkFunction createBatchSinkFunction(ClickHouseRowConverter converter) {
            ClickHouseExecutor executor;
            if (this.primaryKey.isPresent() && !this.options.getIgnoreDelete()) {
                executor = ClickHouseExecutor.createUpsertExecutor(
                                this.options.getTableName(),
                                this.fieldNames,
                                listToStringArray((this.primaryKey.get()).getColumns()),
                                converter,
                                this.options);
            } else {
                String sql = ClickHouseStatementFactory.getInsertIntoStatement(this.options.getTableName(), this.fieldNames);
                executor = new ClickHouseBatchExecutor(
                                sql,
                                converter,
                                this.options.getFlushInterval(),
                                this.options.getBatchSize(),
                                this.options.getMaxRetries(),
                                this.rowDataTypeInformation);
            }
            return new ClickHouseBatchSinkFunction(new ClickHouseConnectionProvider(this.options), executor, this.options);
        }

        /**
         * 分片插入本地表使用ClickHouseShardSinkFunction，sink.partition-strategy不能为空
         * @param logicalTypes
         * @param converter
         * @return
         */
        private ClickHouseShardSinkFunction createShardSinkFunction(LogicalType[] logicalTypes, ClickHouseRowConverter converter) {
            ClickHousePartitioner partitioner;
            Optional<String[]> keyFields;
            int index;
            RowData.FieldGetter getter;
            switch (this.options.getPartitionStrategy()) {
                case "balanced":
                    partitioner = ClickHousePartitioner.createBalanced();
                    break;
                case "shuffle":
                    partitioner = ClickHousePartitioner.createShuffle();
                    break;
                case "hash":
                    index = fieldNames.indexOf(this.options.getPartitionKey());
                    if (index == -1)
                        throw new IllegalArgumentException("Partition key `" + this.options
                                .getPartitionKey() + "` not found in table schema");
                    getter = RowData.createFieldGetter(logicalTypes[index], index);
                    partitioner = ClickHousePartitioner.createHash(getter);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown sink.partition-strategy `" + this.options
                            .getPartitionStrategy() + "`");
            }
            if (this.primaryKey.isPresent() && !this.options.getIgnoreDelete()) {
                keyFields = Optional.of(listToStringArray((this.primaryKey.get()).getColumns()));
            } else {
                keyFields = Optional.empty();
            }
            return new ClickHouseShardSinkFunction(new ClickHouseConnectionProvider(this.options), logicalTypes, this.fieldNames, keyFields, converter, partitioner, this.options);
        }

        private String[] listToStringArray(List<String> lists) {
            if (lists == null)
                return new String[0];
            String[] keyFields = new String[lists.size()];
            int i = 0;
            for (String keyField : lists)
                keyFields[i++] = keyField;
            return keyFields;
        }
    }

}
