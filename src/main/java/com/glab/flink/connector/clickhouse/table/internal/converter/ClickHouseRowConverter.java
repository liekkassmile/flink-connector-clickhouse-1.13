package com.glab.flink.connector.clickhouse.table.internal.converter;

import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.Preconditions;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalTime;

public class ClickHouseRowConverter implements JdbcRowConverter {
    private static final long serialVersionUID = 1L;

    private RowType rowType;

    private LogicalType[] fieldTypes;

    private DeserializationConverter[] toInternalConverters;

    private SerializationConverter[] toClickHouseConverters;

    private JDBCSerializationConverter[] toExternalConverters;

    public ClickHouseRowConverter(RowType rowType) {
        this.rowType = (RowType) Preconditions.checkNotNull(rowType);
        this.fieldTypes = (LogicalType[]) rowType.getFields().stream().map(RowType.RowField::getType).toArray(x -> new LogicalType[x]);
        this.toInternalConverters = new DeserializationConverter[rowType.getFieldCount()];
        this.toClickHouseConverters = new SerializationConverter[rowType.getFieldCount()];
        this.toExternalConverters = new JDBCSerializationConverter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.toInternalConverters[i] = createToFlinkConverter(rowType.getTypeAt(i));
            this.toClickHouseConverters[i] = createToClickHouseConverter(this.fieldTypes[i]);
            this.toExternalConverters[i] = createToExternalConverter(this.fieldTypes[i]);
        }
    }

    public ClickHousePreparedStatement toClickHouse(RowData rowData, ClickHousePreparedStatement statement) throws SQLException {
        //getArity Returns the number of fields in this row.
        for(int idx = 0; idx < rowData.getArity(); idx++) {
            //isNullAt Returns true if the field is null at the given position
            if(rowData == null || rowData.isNullAt(idx)) {
                statement.setObject(idx + 1, null);
            } else {
                //通过函数式接口将rowdata转换为ck statement
                this.toClickHouseConverters[idx].serialize(rowData, idx, statement);
            }
        }
        return statement;
    }

    /**
     * toInternal与toExternal为继承jdbc，查询使用
     * @return
     * @throws SQLException
     */
    @Override
    public RowData toInternal(ResultSet resultSet) throws SQLException {
        GenericRowData genericRowData = new GenericRowData(this.rowType.getFieldCount());
        for(int idx = 0; idx < genericRowData.getArity(); idx++) {
            Object field = resultSet.getObject(idx + 1);
            genericRowData.setField(idx, this.toInternalConverters[idx].deserialize(field));
        }
        return (RowData) genericRowData;
    }

    @Override
    public FieldNamedPreparedStatement toExternal(RowData rowData, FieldNamedPreparedStatement statement) throws SQLException {
        //getArity Returns the number of fields in this row.
        for(int idx = 0; idx < rowData.getArity(); idx++) {
            //isNullAt Returns true if the field is null at the given position
            if(rowData == null || rowData.isNullAt(idx)) {
                statement.setObject(idx + 1, null);
            } else {
                //通过函数式接口将rowdata转换为ck statement
                this.toExternalConverters[idx].serialize(rowData, idx, statement);
            }
        }
        return statement;
    }

    private SerializationConverter createToClickHouseConverter(LogicalType type) {
        int timestampPrecision;
        int decimalPrecision;
        int decimalScale;
        switch(type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, statement) -> statement.setBoolean(index + 1, val.getBoolean(index));
            case TINYINT:
                return (val, index, statement) -> statement.setByte(index + 1, val.getByte(index));
            case SMALLINT:
                return (val, index, statement) -> statement.setShort(index + 1, val.getShort(index));
            case INTERVAL_YEAR_MONTH:
            case INTEGER:
                return (val, index, statement) -> statement.setInt(index + 1, val.getInt(index));
            case INTERVAL_DAY_TIME:
            case BIGINT:
                return (val, index, statement) -> statement.setLong(index + 1, val.getLong(index));
            case FLOAT:
                return (val, index, statement) -> statement.setFloat(index + 1, val.getFloat(index));
            case DOUBLE:
                return (val, index, statement) -> statement.setDouble(index + 1, val.getDouble(index));
            case CHAR:
            case VARCHAR:
                return (val, index, statement) -> statement.setString(index + 1, val.getString(index).toString());
            case VARBINARY:
                return (val, index, statement) -> statement.setBytes(index + 1, val.getBinary(index));
            case DATE:
                return (val, index, statement) -> statement.setDate(index + 1, Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, statement) -> statement.setTime(index + 1, Time.valueOf(LocalTime.ofNanoOfDay(val.getInt(index) * 1000000L)));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                timestampPrecision = ((TimestampType)type).getPrecision();
                return (val, index, statement) -> statement.setTimestamp(index + 1, val.getTimestamp(index, timestampPrecision).toTimestamp());
            case DECIMAL:
                decimalPrecision = ((DecimalType)type).getPrecision();
                decimalScale = ((DecimalType)type).getScale();
                return (val, index, statement) -> statement.setBigDecimal(index + 1, val.getDecimal(index, decimalPrecision, decimalScale).toBigDecimal());
        }
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }

    private JDBCSerializationConverter createToExternalConverter(LogicalType type) {
        int timestampPrecision;
        int decimalPrecision;
        int decimalScale;
        switch(type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, statement) -> statement.setBoolean(index + 1, val.getBoolean(index));
            case TINYINT:
                return (val, index, statement) -> statement.setByte(index + 1, val.getByte(index));
            case SMALLINT:
                return (val, index, statement) -> statement.setShort(index + 1, val.getShort(index));
            case INTERVAL_YEAR_MONTH:
            case INTEGER:
                return (val, index, statement) -> statement.setInt(index + 1, val.getInt(index));
            case INTERVAL_DAY_TIME:
            case BIGINT:
                return (val, index, statement) -> statement.setLong(index + 1, val.getLong(index));
            case FLOAT:
                return (val, index, statement) -> statement.setFloat(index + 1, val.getFloat(index));
            case DOUBLE:
                return (val, index, statement) -> statement.setDouble(index + 1, val.getDouble(index));
            case CHAR:
            case VARCHAR:
                return (val, index, statement) -> statement.setString(index + 1, val.getString(index).toString());
            case VARBINARY:
                return (val, index, statement) -> statement.setBytes(index + 1, val.getBinary(index));
            case DATE:
                return (val, index, statement) -> statement.setDate(index + 1, Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, statement) -> statement.setTime(index + 1, Time.valueOf(LocalTime.ofNanoOfDay(val.getInt(index) * 1000000L)));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                timestampPrecision = ((TimestampType)type).getPrecision();
                return (val, index, statement) -> statement.setTimestamp(index + 1, val.getTimestamp(index, timestampPrecision).toTimestamp());
            case DECIMAL:
                decimalPrecision = ((DecimalType)type).getPrecision();
                decimalScale = ((DecimalType)type).getScale();
                return (val, index, statement) -> statement.setBigDecimal(index + 1, val.getDecimal(index, decimalPrecision, decimalScale).toBigDecimal());
        }
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }

    private DeserializationConverter createToFlinkConverter(LogicalType type) {
        int precision;
        int scale;
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return val -> val;
            case TINYINT:
                return val -> Byte.valueOf(((Integer)val).byteValue());
            case SMALLINT:
                return val -> (val instanceof Integer) ? Short.valueOf(((Integer)val).shortValue()):val;
            case INTEGER:
                return val -> val;
            case BIGINT:
                return val -> (val instanceof BigInteger) ? ((BigInteger) val).longValue() :val;
            case DECIMAL:
                precision = ((DecimalType)type).getPrecision();
                scale = ((DecimalType)type).getScale();
                return val -> (val instanceof BigInteger) ? DecimalData.fromBigDecimal(new BigDecimal((BigInteger)val, 0), precision, scale) : DecimalData.fromBigDecimal((BigDecimal)val, precision, scale);
            case DATE:
                return val -> Integer.valueOf((int)((Date)val).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val -> Integer.valueOf((int)(((Time)val).toLocalTime().toNanoOfDay() / 1000000L));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> TimestampData.fromTimestamp((Timestamp) val);
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(String.valueOf(val));
            case BINARY:
            case VARBINARY:
                return val -> (byte[])val;
        }
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }

    @FunctionalInterface
    static interface SerializationConverter extends Serializable{
        void serialize(RowData param1RowData, int param1Int, PreparedStatement param1PreparedStatement) throws SQLException;
    }

    @FunctionalInterface
    static interface JDBCSerializationConverter extends Serializable{
        void serialize(RowData param1RowData, int param1Int, FieldNamedPreparedStatement param1PreparedStatement) throws SQLException;
    }

    @FunctionalInterface
    static interface DeserializationConverter extends Serializable {
        Object deserialize(Object param1Object) throws SQLException;
    }
}
