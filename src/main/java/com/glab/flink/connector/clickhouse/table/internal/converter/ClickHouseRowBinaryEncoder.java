package com.glab.flink.connector.clickhouse.table.internal.converter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseRowBinaryEncoder {
    private final LogicalType[] logicalTypes;
    private final List<ColumnType> columnTypes;

    public ClickHouseRowBinaryEncoder(LogicalType[] logicalTypes, List<String> clickHouseTypeNames) {
        this.logicalTypes = logicalTypes;
        this.columnTypes = new ArrayList<>(clickHouseTypeNames.size());
        for (String typeName : clickHouseTypeNames) {
            this.columnTypes.add(ColumnType.parse(typeName));
        }
    }

    public void encodeRows(List<RowData> rows, OutputStream outputStream) throws IOException {
        for (RowData row : rows) {
            encodeRow(row, outputStream);
        }
    }

    public void encodeRow(RowData rowData, OutputStream outputStream) throws IOException {
        for (int i = 0; i < this.columnTypes.size(); i++) {
            encodeColumn(this.columnTypes.get(i), this.logicalTypes[i], rowData, i, outputStream);
        }
    }

    private void encodeColumn(ColumnType columnType, LogicalType logicalType, RowData rowData, int index, OutputStream outputStream) throws IOException {
        if (columnType.nullable) {
            boolean isNull = rowData.isNullAt(index);
            outputStream.write(isNull ? 1 : 0);
            if (isNull) {
                return;
            }
            encodeColumn(columnType.nestedType, logicalType, rowData, index, outputStream);
            return;
        }

        switch (columnType.kind) {
            case BOOL:
                outputStream.write(rowData.getBoolean(index) ? 1 : 0);
                return;
            case INT8:
            case UINT8:
                outputStream.write(extractNumber(rowData, logicalType, index).intValue());
                return;
            case INT16:
            case UINT16:
                writeLittleEndianShort(outputStream, extractNumber(rowData, logicalType, index).shortValue());
                return;
            case INT32:
            case UINT32:
                writeLittleEndianInt(outputStream, extractNumber(rowData, logicalType, index).intValue());
                return;
            case INT64:
            case UINT64:
                writeLittleEndianLong(outputStream, extractNumber(rowData, logicalType, index).longValue());
                return;
            case FLOAT32:
                writeLittleEndianInt(outputStream, Float.floatToIntBits((float) extractNumber(rowData, logicalType, index).doubleValue()));
                return;
            case FLOAT64:
                writeLittleEndianLong(outputStream, Double.doubleToLongBits(extractNumber(rowData, logicalType, index).doubleValue()));
                return;
            case STRING:
                writeLengthPrefixedBytes(outputStream, extractBytes(rowData, logicalType, index));
                return;
            case FIXED_STRING:
                writeFixedString(outputStream, extractBytes(rowData, logicalType, index), columnType.length);
                return;
            case DATE:
                writeLittleEndianShort(outputStream, (short) rowData.getInt(index));
                return;
            case DATE32:
                writeLittleEndianInt(outputStream, rowData.getInt(index));
                return;
            case DATETIME:
                writeLittleEndianInt(outputStream, (int) (extractTimestampMillis(rowData, logicalType, index) / 1000L));
                return;
            case DATETIME64:
                writeLittleEndianLong(outputStream, toDateTime64(extractTimestamp(rowData, logicalType, index), columnType.scale));
                return;
            case DECIMAL32:
                writeLittleEndianInt(outputStream, extractDecimal(rowData, logicalType, index).intValue());
                return;
            case DECIMAL64:
                writeLittleEndianLong(outputStream, extractDecimal(rowData, logicalType, index).longValue());
                return;
            case DECIMAL128:
                writeBigIntegerLittleEndian(outputStream, extractDecimal(rowData, logicalType, index), 16);
                return;
            case DECIMAL256:
                writeBigIntegerLittleEndian(outputStream, extractDecimal(rowData, logicalType, index), 32);
                return;
            default:
                throw new UnsupportedOperationException("Unsupported ClickHouse type for RowBinary: " + columnType.rawTypeName);
        }
    }

    private Number extractNumber(RowData rowData, LogicalType logicalType, int index) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return rowData.getBoolean(index) ? 1 : 0;
            case TINYINT:
                return rowData.getByte(index);
            case SMALLINT:
                return rowData.getShort(index);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                return rowData.getInt(index);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return rowData.getLong(index);
            case FLOAT:
                return rowData.getFloat(index);
            case DOUBLE:
                return rowData.getDouble(index);
            default:
                throw new UnsupportedOperationException("Unsupported numeric logical type: " + logicalType);
        }
    }

    private byte[] extractBytes(RowData rowData, LogicalType logicalType, int index) {
        switch (logicalType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                StringData stringData = rowData.getString(index);
                return stringData.toBytes();
            case BINARY:
            case VARBINARY:
                return rowData.getBinary(index);
            default:
                throw new UnsupportedOperationException("Unsupported bytes logical type: " + logicalType);
        }
    }

    private BigInteger extractDecimal(RowData rowData, LogicalType logicalType, int index) {
        DecimalType decimalType = (DecimalType) logicalType;
        DecimalData decimalData = rowData.getDecimal(index, decimalType.getPrecision(), decimalType.getScale());
        return decimalData.toBigDecimal().unscaledValue();
    }

    private TimestampData extractTimestamp(RowData rowData, LogicalType logicalType, int index) {
        TimestampType timestampType = (TimestampType) logicalType;
        return rowData.getTimestamp(index, timestampType.getPrecision());
    }

    private long extractTimestampMillis(RowData rowData, LogicalType logicalType, int index) {
        return extractTimestamp(rowData, logicalType, index).toTimestamp().getTime();
    }

    private long toDateTime64(TimestampData timestampData, int scale) {
        Timestamp timestamp = timestampData.toTimestamp();
        long millis = timestamp.getTime();
        if (scale <= 3) {
            return millis * pow10(scale) / 1000L;
        }
        long base = millis * pow10(scale - 3);
        int nanos = timestamp.getNanos() % 1_000_000;
        return base + nanos / pow10(9 - scale);
    }

    private long pow10(int exponent) {
        long result = 1L;
        for (int i = 0; i < exponent; i++) {
            result *= 10L;
        }
        return result;
    }

    private void writeLengthPrefixedBytes(OutputStream outputStream, byte[] bytes) throws IOException {
        writeVarUInt(outputStream, bytes.length);
        outputStream.write(bytes);
    }

    private void writeFixedString(OutputStream outputStream, byte[] bytes, int length) throws IOException {
        int writeLength = Math.min(bytes.length, length);
        outputStream.write(bytes, 0, writeLength);
        for (int i = writeLength; i < length; i++) {
            outputStream.write(0);
        }
    }

    private void writeVarUInt(OutputStream outputStream, long value) throws IOException {
        long remaining = value;
        while ((remaining & ~0x7FL) != 0L) {
            outputStream.write((int) ((remaining & 0x7F) | 0x80));
            remaining >>>= 7;
        }
        outputStream.write((int) remaining);
    }

    private void writeLittleEndianShort(OutputStream outputStream, short value) throws IOException {
        outputStream.write(value & 0xFF);
        outputStream.write((value >>> 8) & 0xFF);
    }

    private void writeLittleEndianInt(OutputStream outputStream, int value) throws IOException {
        outputStream.write(value & 0xFF);
        outputStream.write((value >>> 8) & 0xFF);
        outputStream.write((value >>> 16) & 0xFF);
        outputStream.write((value >>> 24) & 0xFF);
    }

    private void writeLittleEndianLong(OutputStream outputStream, long value) throws IOException {
        outputStream.write((int) (value & 0xFF));
        outputStream.write((int) ((value >>> 8) & 0xFF));
        outputStream.write((int) ((value >>> 16) & 0xFF));
        outputStream.write((int) ((value >>> 24) & 0xFF));
        outputStream.write((int) ((value >>> 32) & 0xFF));
        outputStream.write((int) ((value >>> 40) & 0xFF));
        outputStream.write((int) ((value >>> 48) & 0xFF));
        outputStream.write((int) ((value >>> 56) & 0xFF));
    }

    private void writeBigIntegerLittleEndian(OutputStream outputStream, BigInteger value, int width) throws IOException {
        byte[] raw = value.toByteArray();
        byte fill = value.signum() < 0 ? (byte) 0xFF : 0;
        for (int i = 0; i < width; i++) {
            int sourceIndex = raw.length - 1 - i;
            outputStream.write(sourceIndex >= 0 ? raw[sourceIndex] : fill);
        }
    }

    private static class ColumnType {
        private final Kind kind;
        private final boolean nullable;
        private final ColumnType nestedType;
        private final int scale;
        private final int length;
        private final String rawTypeName;

        private ColumnType(Kind kind, boolean nullable, ColumnType nestedType, int scale, int length, String rawTypeName) {
            this.kind = kind;
            this.nullable = nullable;
            this.nestedType = nestedType;
            this.scale = scale;
            this.length = length;
            this.rawTypeName = rawTypeName;
        }

        private static ColumnType parse(String typeName) {
            String normalized = typeName.trim();
            if (normalized.startsWith("Nullable(") && normalized.endsWith(")")) {
                String nested = normalized.substring("Nullable(".length(), normalized.length() - 1);
                return new ColumnType(null, true, parse(nested), 0, 0, normalized);
            }
            if (normalized.startsWith("LowCardinality(") && normalized.endsWith(")")) {
                String nested = normalized.substring("LowCardinality(".length(), normalized.length() - 1);
                return parse(nested);
            }
            if (normalized.startsWith("FixedString(")) {
                return new ColumnType(Kind.FIXED_STRING, false, null, 0, parseSingleIntArg(normalized), normalized);
            }
            if (normalized.startsWith("DateTime64(")) {
                return new ColumnType(Kind.DATETIME64, false, null, parseSingleIntArg(normalized), 0, normalized);
            }
            if (normalized.startsWith("Decimal32(")) {
                return new ColumnType(Kind.DECIMAL32, false, null, 0, 0, normalized);
            }
            if (normalized.startsWith("Decimal64(")) {
                return new ColumnType(Kind.DECIMAL64, false, null, 0, 0, normalized);
            }
            if (normalized.startsWith("Decimal128(")) {
                return new ColumnType(Kind.DECIMAL128, false, null, 0, 0, normalized);
            }
            if (normalized.startsWith("Decimal256(")) {
                return new ColumnType(Kind.DECIMAL256, false, null, 0, 0, normalized);
            }

            switch (normalized) {
                case "Bool":
                    return new ColumnType(Kind.BOOL, false, null, 0, 0, normalized);
                case "Int8":
                    return new ColumnType(Kind.INT8, false, null, 0, 0, normalized);
                case "UInt8":
                    return new ColumnType(Kind.UINT8, false, null, 0, 0, normalized);
                case "Int16":
                    return new ColumnType(Kind.INT16, false, null, 0, 0, normalized);
                case "UInt16":
                    return new ColumnType(Kind.UINT16, false, null, 0, 0, normalized);
                case "Int32":
                    return new ColumnType(Kind.INT32, false, null, 0, 0, normalized);
                case "UInt32":
                    return new ColumnType(Kind.UINT32, false, null, 0, 0, normalized);
                case "Int64":
                    return new ColumnType(Kind.INT64, false, null, 0, 0, normalized);
                case "UInt64":
                    return new ColumnType(Kind.UINT64, false, null, 0, 0, normalized);
                case "Float32":
                    return new ColumnType(Kind.FLOAT32, false, null, 0, 0, normalized);
                case "Float64":
                    return new ColumnType(Kind.FLOAT64, false, null, 0, 0, normalized);
                case "String":
                    return new ColumnType(Kind.STRING, false, null, 0, 0, normalized);
                case "Date":
                    return new ColumnType(Kind.DATE, false, null, 0, 0, normalized);
                case "Date32":
                    return new ColumnType(Kind.DATE32, false, null, 0, 0, normalized);
                case "DateTime":
                    return new ColumnType(Kind.DATETIME, false, null, 0, 0, normalized);
                default:
                    throw new UnsupportedOperationException("Unsupported ClickHouse type for RowBinary: " + normalized);
            }
        }

        private static int parseSingleIntArg(String raw) {
            int start = raw.indexOf('(');
            int end = raw.indexOf(')');
            return Integer.parseInt(raw.substring(start + 1, end).split(",")[0].trim());
        }
    }

    private enum Kind {
        BOOL,
        INT8,
        UINT8,
        INT16,
        UINT16,
        INT32,
        UINT32,
        INT64,
        UINT64,
        FLOAT32,
        FLOAT64,
        STRING,
        FIXED_STRING,
        DATE,
        DATE32,
        DATETIME,
        DATETIME64,
        DECIMAL32,
        DECIMAL64,
        DECIMAL128,
        DECIMAL256
    }
}
