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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseRowBinaryEncoder {
    private static final ZoneId DEFAULT_ZONE_ID = ZoneId.systemDefault();
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_TIME_MILLIS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

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
                outputStream.write(extractBoolean(rowData, logicalType, index) ? 1 : 0);
                return;
            case INT8:
            case UINT8:
                outputStream.write(extractNumber(columnType, rowData, logicalType, index).intValue());
                return;
            case INT16:
            case UINT16:
                writeLittleEndianShort(outputStream, extractNumber(columnType, rowData, logicalType, index).shortValue());
                return;
            case INT32:
            case UINT32:
                writeLittleEndianInt(outputStream, extractNumber(columnType, rowData, logicalType, index).intValue());
                return;
            case INT64:
            case UINT64:
                writeLittleEndianLong(outputStream, extractNumber(columnType, rowData, logicalType, index).longValue());
                return;
            case FLOAT32:
                writeLittleEndianInt(outputStream, Float.floatToIntBits((float) extractNumber(columnType, rowData, logicalType, index).doubleValue()));
                return;
            case FLOAT64:
                writeLittleEndianLong(outputStream, Double.doubleToLongBits(extractNumber(columnType, rowData, logicalType, index).doubleValue()));
                return;
            case STRING:
                writeLengthPrefixedBytes(outputStream, extractBytes(rowData, logicalType, index));
                return;
            case FIXED_STRING:
                writeFixedString(outputStream, extractBytes(rowData, logicalType, index), columnType.length);
                return;
            case DATE:
                writeLittleEndianShort(outputStream, (short) extractDateDays(rowData, logicalType, index));
                return;
            case DATE32:
                writeLittleEndianInt(outputStream, extractDateDays(rowData, logicalType, index));
                return;
            case DATETIME:
                writeLittleEndianInt(outputStream, (int) (extractTimestampMillis(rowData, logicalType, index) / 1000L));
                return;
            case DATETIME64:
                writeLittleEndianLong(outputStream, toDateTime64(extractTimestamp(rowData, logicalType, index), columnType.scale));
                return;
            case DECIMAL32:
                writeLittleEndianInt(outputStream, extractDecimal(rowData, logicalType, columnType, index).intValue());
                return;
            case DECIMAL64:
                writeLittleEndianLong(outputStream, extractDecimal(rowData, logicalType, columnType, index).longValue());
                return;
            case DECIMAL128:
                writeBigIntegerLittleEndian(outputStream, extractDecimal(rowData, logicalType, columnType, index), 16);
                return;
            case DECIMAL256:
                writeBigIntegerLittleEndian(outputStream, extractDecimal(rowData, logicalType, columnType, index), 32);
                return;
            default:
                throw new UnsupportedOperationException("Unsupported ClickHouse type for RowBinary: " + columnType.rawTypeName);
        }
    }

    private Number extractNumber(ColumnType columnType, RowData rowData, LogicalType logicalType, int index) {
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
            case CHAR:
            case VARCHAR:
                return parseStringNumber(rowData.getString(index).toString(), columnType);
            default:
                throw new UnsupportedOperationException("Unsupported numeric logical type: " + logicalType);
        }
    }

    private byte[] extractBytes(RowData rowData, LogicalType logicalType, int index) {
        switch (logicalType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                StringData stringData = rowData.getString(index);
                return stringData == null ? new byte[0] : stringData.toBytes();
            case BINARY:
            case VARBINARY:
                byte[] binary = rowData.getBinary(index);
                return binary == null ? new byte[0] : binary;
            default:
                throw new UnsupportedOperationException("Unsupported bytes logical type: " + logicalType);
        }
    }

    private boolean extractBoolean(RowData rowData, LogicalType logicalType, int index) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return rowData.getBoolean(index);
            case TINYINT:
                return rowData.getByte(index) != 0;
            case SMALLINT:
                return rowData.getShort(index) != 0;
            case INTEGER:
                return rowData.getInt(index) != 0;
            case BIGINT:
                return rowData.getLong(index) != 0L;
            case CHAR:
            case VARCHAR:
                String value = rowData.getString(index) == null ? "" : rowData.getString(index).toString().trim();
                if ("1".equals(value) || "true".equalsIgnoreCase(value)) {
                    return true;
                }
                if ("0".equals(value) || "false".equalsIgnoreCase(value)) {
                    return false;
                }
                throw new IllegalArgumentException("Unsupported boolean string value: " + value);
            default:
                throw new UnsupportedOperationException("Unsupported boolean logical type: " + logicalType);
        }
    }

    private BigInteger extractDecimal(RowData rowData, LogicalType logicalType, ColumnType columnType, int index) {
        switch (logicalType.getTypeRoot()) {
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                DecimalData decimalData = rowData.getDecimal(index, decimalType.getPrecision(), decimalType.getScale());
                return decimalData.toBigDecimal().unscaledValue();
            case CHAR:
            case VARCHAR:
                return new BigDecimal(rowData.getString(index).toString().trim()).unscaledValue();
            default:
                throw new UnsupportedOperationException("Unsupported decimal logical type: " + logicalType + " for ClickHouse type " + columnType.rawTypeName);
        }
    }

    private Number parseStringNumber(String rawValue, ColumnType columnType) {
        String value = rawValue == null ? "" : rawValue.trim();
        switch (columnType.kind) {
            case FLOAT32:
            case FLOAT64:
                return Double.parseDouble(value);
            case INT8:
            case UINT8:
            case INT16:
            case UINT16:
            case INT32:
            case UINT32:
                return Integer.parseInt(value);
            case INT64:
            case UINT64:
                return Long.parseLong(value);
            case BOOL:
                if ("1".equals(value) || "true".equalsIgnoreCase(value)) {
                    return 1;
                }
                if ("0".equals(value) || "false".equalsIgnoreCase(value)) {
                    return 0;
                }
                throw new IllegalArgumentException("Unsupported boolean string value: " + rawValue);
            default:
                throw new UnsupportedOperationException("Unsupported string-to-number conversion for ClickHouse type: " + columnType.rawTypeName);
        }
    }

    private int extractDateDays(RowData rowData, LogicalType logicalType, int index) {
        switch (logicalType.getTypeRoot()) {
            case DATE:
                return rowData.getInt(index);
            case CHAR:
            case VARCHAR:
                return (int) LocalDate.parse(rowData.getString(index).toString().trim(), DATE_FORMATTER).toEpochDay();
            default:
                throw new UnsupportedOperationException("Unsupported date logical type: " + logicalType);
        }
    }

    private TimestampData extractTimestamp(RowData rowData, LogicalType logicalType, int index) {
        switch (logicalType.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                TimestampType timestampType = (TimestampType) logicalType;
                return rowData.getTimestamp(index, timestampType.getPrecision());
            case CHAR:
            case VARCHAR:
                return TimestampData.fromTimestamp(parseTimestamp(rowData.getString(index).toString().trim()));
            default:
                throw new UnsupportedOperationException("Unsupported timestamp logical type: " + logicalType);
        }
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

    private Timestamp parseTimestamp(String rawValue) {
        if (rawValue.matches("^-?\\d+$")) {
            long epoch = Long.parseLong(rawValue);
            if (rawValue.length() <= 10) {
                return Timestamp.from(Instant.ofEpochSecond(epoch));
            }
            return Timestamp.from(Instant.ofEpochMilli(epoch));
        }

        try {
            return Timestamp.valueOf(LocalDateTime.parse(rawValue, DATE_TIME_MILLIS_FORMATTER));
        } catch (DateTimeParseException ignored) {
        }

        try {
            return Timestamp.valueOf(LocalDateTime.parse(rawValue, DATE_TIME_FORMATTER));
        } catch (DateTimeParseException ignored) {
        }

        try {
            return Timestamp.from(LocalDate.parse(rawValue, DATE_FORMATTER).atStartOfDay(DEFAULT_ZONE_ID).toInstant());
        } catch (DateTimeParseException ignored) {
        }

        try {
            return Timestamp.from(Instant.parse(rawValue));
        } catch (DateTimeParseException ignored) {
        }

        throw new IllegalArgumentException("Unsupported timestamp string value: " + rawValue);
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
