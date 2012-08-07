/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.cql.jdbc;

import static org.apache.cassandra.cql.jdbc.Utils.*;
import static org.apache.cassandra.utils.ByteBufferUtil.string;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import org.apache.cassandra.cql.jdbc.TypedColumn.CollectionType;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlMetadata;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * <p>The Supported Data types in CQL are as follows:</p>
 * <table>
 * <tr><th>type</th><th>java type</th><th>description</th></tr>
 * <tr><td>ascii</td><td>String</td><td>ASCII character string</td></tr>
 * <tr><td>bigint</td><td>Long</td><td>64-bit signed long</td></tr>
 * <tr><td>blob</td><td>ByteBuffer</td><td>Arbitrary bytes (no validation)</td></tr>
 * <tr><td>boolean</td><td>Boolean</td><td>true or false</td></tr>
 * <tr><td>counter</td><td>Long</td><td>Counter column (64-bit long)</td></tr>
 * <tr><td>decimal</td><td>BigDecimal</td><td>Variable-precision decimal</td></tr>
 * <tr><td>double</td><td>Double</td><td>64-bit IEEE-754 floating point</td></tr>
 * <tr><td>float</td><td>Float</td><td>32-bit IEEE-754 floating point</td></tr>
 * <tr><td>int</td><td>Integer</td><td>32-bit signed int</td></tr>
 * <tr><td>text</td><td>String</td><td>UTF8 encoded string</td></tr>
 * <tr><td>timestamp</td><td>Date</td><td>A timestamp</td></tr>
 * <tr><td>uuid</td><td>UUID</td><td>Type 1 or type 4 UUID</td></tr>
 * <tr><td>varchar</td><td>String</td><td>UTF8 encoded string</td></tr>
 * <tr><td>varint</td><td>BigInteger</td><td>Arbitrary-precision integer</td></tr>
 * </table>
 *
 */
class CassandraResultSet extends AbstractResultSet implements CassandraResultSetExtras
{
    public static final int DEFAULT_TYPE = ResultSet.TYPE_FORWARD_ONLY;
    public static final int DEFAULT_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;
    public static final int DEFAULT_HOLDABILITY = ResultSet.HOLD_CURSORS_OVER_COMMIT;

    /**
     * The rows iterator.
     */
    private Iterator<CqlRow> rowsIterator;
    

    int rowNumber = 0;
    // the current row key when iterating through results.
    private byte[] curRowKey = null;

    /**
     * The values.
     */
    private List<TypedColumn> values = new ArrayList<TypedColumn>();

    /**
     * The index map.
     */
    private Map<String, Integer> indexMap = new HashMap<String, Integer>();

    private final CResultSetMetaData meta;

    private final CassandraStatement statement;

    private int resultSetType;

    private int fetchDirection;

    private int fetchSize;

    private boolean wasNull;

    private CqlMetadata schema;

    /**
     * no argument constructor.
     */
    CassandraResultSet()
    {
        statement = null;
        meta = new CResultSetMetaData();
    }

    /**
     * Instantiates a new cassandra result set from a CqlResult.
     */
    CassandraResultSet(CassandraStatement statement, CqlResult resultSet) throws SQLException
    {        
        this.statement = statement;
        this.resultSetType = statement.getResultSetType();
        this.fetchDirection = statement.getFetchDirection();
        this.fetchSize = statement.getFetchSize();
        this.schema = resultSet.schema;

        rowsIterator = resultSet.getRowsIterator();
        
        // Initialize to column values from the first row
        // NOTE: that the first call to next() will HARMLESSLY re-write these values for the columns
        // NOTE: the row cursor is not advanced and sits before the first row
        if (hasMoreRows())
        {
            populateColumns();
            // reset the iterator back to the beginning.
            rowsIterator = resultSet.getRowsIterator();
        }
                
        meta = new CResultSetMetaData();
    }
    
    
    private final boolean hasMoreRows()
    {
        return (rowsIterator !=null && rowsIterator.hasNext());
    }

    private final void populateColumns()
    {
        // clear column value tables
        values.clear();
        indexMap.clear();
        
        CqlRow row = rowsIterator.next();
        curRowKey = row.getKey();
        List<Column> cols = row.getColumns();
        
        // loop through the columns 
        for (Column col : cols)
        {
            TypedColumn c = createColumn(col);
            String columnName = c.getNameString();
            values.add(c);
            indexMap.put(columnName, values.size()); // one greater than 0 based index of a list
        }
    }
    
    public boolean absolute(int arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void afterLast() throws SQLException
    {
        if (resultSetType==TYPE_FORWARD_ONLY) throw new SQLNonTransientException(FORWARD_ONLY);
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void beforeFirst() throws SQLException
    {
        if (resultSetType==TYPE_FORWARD_ONLY) throw new SQLNonTransientException(FORWARD_ONLY);
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    private final void checkIndex(int index) throws SQLException
    {
        // 1 <= index <= size()
        if (index < 1 || index > values.size())
            throw new SQLSyntaxErrorException(String.format(MUST_BE_POSITIVE, String.valueOf(index))+" "+values.size());
    }

    private final void checkName(String name) throws SQLException
    {
        if (indexMap.get(name) == null) throw new SQLSyntaxErrorException(String.format(VALID_LABELS, name));
    }

    private final void checkNotClosed() throws SQLException
    {
        if (isClosed()) throw new SQLRecoverableException(WAS_CLOSED_RSLT);
    }

    public void clearWarnings() throws SQLException
    {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    public void close() throws SQLException
    {
        indexMap = null;
        values = null;
    }

    public int findColumn(String name) throws SQLException
    {
        checkNotClosed();
        checkName(name);
        return indexMap.get(name).intValue();
    }

    public boolean first() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }


    public BigDecimal getBigDecimal(int index) throws SQLException
    {
        checkIndex(index);
        return getBigDecimal(values.get(index - 1));
    }

    /** @deprecated */
    public BigDecimal getBigDecimal(int index, int scale) throws SQLException
    {
        checkIndex(index);
        return (getBigDecimal(values.get(index - 1))).setScale(scale);
    }

    public BigDecimal getBigDecimal(String name) throws SQLException
    {
        checkName(name);
        return getBigDecimal(indexMap.get(name).intValue());
    }

    /** @deprecated */
    public BigDecimal getBigDecimal(String name, int scale) throws SQLException
    {
        checkName(name);
        return (getBigDecimal(indexMap.get(name).intValue())).setScale(scale);
    }

    private BigDecimal getBigDecimal(TypedColumn column) throws SQLException
    {
        checkNotClosed();
        Object value = column.getValue();
        wasNull = value == null;

        if (wasNull) return BigDecimal.ZERO;

        if (value instanceof BigDecimal) return (BigDecimal) value;

        if (value instanceof Long) return BigDecimal.valueOf((Long) value);
        
        if (value instanceof Double) return BigDecimal.valueOf((Double) value);

        if (value instanceof BigInteger) return new BigDecimal((BigInteger) value);

        try
        {
            if (value instanceof String) return (new BigDecimal((String) value));
        }
        catch (NumberFormatException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, value.getClass().getSimpleName(), "BigDecimal"));
    }
    public BigInteger getBigInteger(int index) throws SQLException
    {
        checkIndex(index);
        return getBigInteger(values.get(index - 1));
    }

    public BigInteger getBigInteger(String name) throws SQLException
    {
        checkName(name);
        return getBigInteger(indexMap.get(name).intValue());
    }

    private BigInteger getBigInteger(TypedColumn column) throws SQLException
    {
        checkNotClosed();
        Object value = column.getValue();
        wasNull = value == null;

        if (wasNull) return BigInteger.ZERO;

        if (value instanceof BigInteger) return (BigInteger) value;

        if (value instanceof Integer) return BigInteger.valueOf((Integer) value);

        if (value instanceof Long) return BigInteger.valueOf((Long) value);

        try
        {
            if (value instanceof String) return (new BigInteger((String) value));
        }
        catch (NumberFormatException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, value.getClass().getSimpleName(), "BigInteger"));
    }

    public boolean getBoolean(int index) throws SQLException
    {
        checkIndex(index);
        return getBoolean(values.get(index - 1));
    }

    public boolean getBoolean(String name) throws SQLException
    {
        checkName(name);
        return getBoolean(indexMap.get(name).intValue());
    }

    private final Boolean getBoolean(TypedColumn column) throws SQLException
    {
        checkNotClosed();
        Object value = column.getValue();
        wasNull = value == null;

        if (wasNull) return false;

        if (value instanceof Boolean) return (Boolean)value;

        if (value instanceof Integer) return Boolean.valueOf(((Integer) value) == 0 ? false : true);
        
        if (value instanceof Long) return Boolean.valueOf(((Long) value) == 0 ? false : true);

        if (value instanceof BigInteger) return Boolean.valueOf(((BigInteger) value).intValue() == 0 ? false : true);

        if (value instanceof String)
        {
            String str = (String) value;
            if (str.equalsIgnoreCase("true")) return true;
            if (str.equalsIgnoreCase("false")) return false;

            throw new SQLSyntaxErrorException(String.format(NOT_BOOLEAN, str));
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, value.getClass().getSimpleName(), "Boolean"));
    }

    public byte getByte(int index) throws SQLException
    {
        checkIndex(index);
        return getByte(values.get(index - 1));
    }

    public byte getByte(String name) throws SQLException
    {
        checkName(name);
        return getByte(indexMap.get(name).intValue());
    }

    private final Byte getByte(TypedColumn column) throws SQLException
    {
        checkNotClosed();
        Object value = column.getValue();
        wasNull = value == null;

        if (wasNull) return 0;

        if (value instanceof Integer) return ((Integer) value).byteValue();

        if (value instanceof Long) return ((Long) value).byteValue();

        if (value instanceof BigInteger) return ((BigInteger) value).byteValue();

        try
        {
            if (value instanceof String) return (new Byte((String) value));
        }
        catch (NumberFormatException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, value.getClass().getSimpleName(), "Byte"));
    }

    public byte[] getBytes(int index) throws SQLException
    {
        return getBytes(values.get(index - 1));
    }

    public byte[] getBytes(String name) throws SQLException
    {
        return getBytes(indexMap.get(name).intValue());
    }

    private byte[] getBytes(TypedColumn column) throws SQLException
    {
        checkNotClosed();
        ByteBuffer value = (ByteBuffer) column.getRawColumn().value;
        wasNull = value == null;
        return value == null ? null : ByteBufferUtil.clone(value).array();
    }

    public TypedColumn getColumn(int index) throws SQLException
    {
        checkIndex(index);
        checkNotClosed();
        return values.get(index - 1);
    }

    public TypedColumn getColumn(String name) throws SQLException
    {
        checkName(name);
        checkNotClosed();
        return values.get(indexMap.get(name).intValue());
    }

    public int getConcurrency() throws SQLException
    {
        checkNotClosed();
        return statement.getResultSetConcurrency();
    }

    public Date getDate(int index) throws SQLException
    {
        checkIndex(index);
        return getDate(values.get(index - 1));
    }

    public Date getDate(int index, Calendar calendar) throws SQLException
    {
        checkIndex(index);
        // silently ignore the Calendar argument; its a hint we do not need
        return getDate(index);
    }

    public Date getDate(String name) throws SQLException
    {
        checkName(name);
        return getDate(indexMap.get(name).intValue());
    }

    public Date getDate(String name, Calendar calendar) throws SQLException
    {
        checkName(name);
        // silently ignore the Calendar argument; its a hint we do not need
        return getDate(name);
    }

    private Date getDate(TypedColumn column) throws SQLException
    {
        checkNotClosed();
        Object value = column.getValue();
        wasNull = value == null;

        if (wasNull) return null;

        if (value instanceof Long) return new Date((Long) value);

        if (value instanceof java.util.Date) return new Date(((java.util.Date) value).getTime());

        try
        {
            if (value instanceof String) return Date.valueOf((String) value);
        }
        catch (IllegalArgumentException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, value.getClass().getSimpleName(), "SQL Date"));
    }

    public double getDouble(int index) throws SQLException
    {
        checkIndex(index);
        return getDouble(values.get(index - 1));
    }

    public double getDouble(String name) throws SQLException
    {
        checkName(name);
        return getDouble(indexMap.get(name).intValue());
    }

    private final Double getDouble(TypedColumn column) throws SQLException
    {
        checkNotClosed();
        Object value = column.getValue();
        wasNull = value == null;

        if (wasNull) return 0.0;

        if (value instanceof Double) return ((Double) value);

        if (value instanceof Float) return ((Float) value).doubleValue();

        if (value instanceof Integer) return new Double((Integer) value);

        if (value instanceof Long) return new Double((Long) value);

        if (value instanceof BigInteger) return new Double(((BigInteger) value).doubleValue());

        try
        {
            if (value instanceof String) return new Double((String) value);
        }
        catch (NumberFormatException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, value.getClass().getSimpleName(), "Double"));
    }

    public int getFetchDirection() throws SQLException
    {
        checkNotClosed();
        return fetchDirection;
    }

    public int getFetchSize() throws SQLException
    {
        checkNotClosed();
        return fetchSize;
    }

    public float getFloat(int index) throws SQLException
    {
        checkIndex(index);
        return getFloat(values.get(index - 1));
    }

    public float getFloat(String name) throws SQLException
    {
        checkName(name);
        return getFloat(indexMap.get(name).intValue());
    }

    private final Float getFloat(TypedColumn column) throws SQLException
    {
        checkNotClosed();
        Object value = column.getValue();
        wasNull = value == null;

        if (wasNull) return (float) 0.0;

        if (value instanceof Float) return ((Float) value);

        if (value instanceof Double) return ((Double) value).floatValue();

        if (value instanceof Integer) return new Float((Integer) value);

        if (value instanceof Long) return new Float((Long) value);

        if (value instanceof BigInteger) return new Float(((BigInteger) value).floatValue());

        try
        {
            if (value instanceof String) return new Float((String) value);
        }
        catch (NumberFormatException e)
        {
            throw new SQLException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, value.getClass().getSimpleName(), "Float"));
    }

    public int getHoldability() throws SQLException
    {
        checkNotClosed();
        return statement.getResultSetHoldability();
    }

    public int getInt(int index) throws SQLException
    {
        checkIndex(index);
        return getInt(values.get(index - 1));
    }

    public int getInt(String name) throws SQLException
    {
        checkName(name);
        return getInt(indexMap.get(name).intValue());
    }

    private int getInt(TypedColumn column) throws SQLException
    {
        checkNotClosed();
        Object value = column.getValue();
        wasNull = value == null;

        if (wasNull) return 0;

        if (value instanceof Integer) return ((Integer) value);
        
        if (value instanceof Long) return ((Long) value).intValue();

        if (value instanceof BigInteger) return ((BigInteger) value).intValue();

        try
        {
            if (value instanceof String) return (Integer.parseInt((String) value));
        }
        catch (NumberFormatException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, value.getClass().getSimpleName(), "int"));
    }

    public byte[] getKey() throws SQLException
    {
        return curRowKey;
    }

    public long getLong(int index) throws SQLException
    {
        checkIndex(index);
        return getLong(values.get(index - 1));
    }

    public long getLong(String name) throws SQLException
    {
        checkName(name);
        return getLong(indexMap.get(name).intValue());
    }

    private Long getLong(TypedColumn column) throws SQLException
    {
        checkNotClosed();
        Object value = column.getValue();
        wasNull = value == null;

        if (wasNull) return 0L;

        if (value instanceof Long) return (Long) value;

        if (value instanceof Integer) return Long.valueOf((Integer) value);

        if (value instanceof BigInteger) return getBigInteger(column).longValue();

        if (value instanceof Long) return (Long) value;

        try
        {
            if (value instanceof String) return (Long.parseLong((String) value));
        }
        catch (NumberFormatException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, value.getClass().getSimpleName(), "Long"));
    }

    public ResultSetMetaData getMetaData() throws SQLException
    {
        checkNotClosed();
        return meta;
    }

    public Object getObject(int index) throws SQLException
    {
        checkIndex(index);
        return getObject(values.get(index - 1));
    }

    public Object getObject(String name) throws SQLException
    {
        checkName(name);
        return getObject(indexMap.get(name).intValue());
    }


    private Object getObject(TypedColumn column) throws SQLException
    {
        checkNotClosed();
        Object value = column.getValue();
        wasNull = value == null;
        
        if (value instanceof String)
        {
            switch (column.getCollectionType())
            {
                case LIST: break;
                case MAP: break;
                case SET: break;
            }
        }
        return (wasNull) ? null : value;
    }

    public int getRow() throws SQLException
    {
        checkNotClosed();
        return rowNumber;
    }

    public RowId getRowId(int index) throws SQLException
    {
        checkIndex(index);
        return getRowId(values.get(index - 1));
    }

    public RowId getRowId(String name) throws SQLException
    {
        checkName(name);
        return getRowId(indexMap.get(name).intValue());
    }

    private final RowId getRowId(TypedColumn column) throws SQLException
    {
        checkNotClosed();
        ByteBuffer value =  column.getRawColumn().value;
        wasNull = value == null;
        return value == null ? null : new CassandraRowId(value);
    }

    public short getShort(int index) throws SQLException
    {
        checkIndex(index);
        return getShort(values.get(index - 1));
    }

    public short getShort(String name) throws SQLException
    {
        checkName(name);
        return getShort(indexMap.get(name).intValue());
    }

    private final Short getShort(TypedColumn column) throws SQLException
    {
        checkNotClosed();
        Object value = column.getValue();
        wasNull = value == null;

        if (wasNull) return 0;

        if (value instanceof Integer) return ((Integer) value).shortValue();

        if (value instanceof Long) return ((Long) value).shortValue();

        if (value instanceof BigInteger) return ((BigInteger) value).shortValue();

        try
        {
            if (value instanceof String) return (new Short((String) value));
        }
        catch (NumberFormatException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, value.getClass().getSimpleName(), "Short"));
    }

    public Statement getStatement() throws SQLException
    {
        checkNotClosed();
        return statement;
    }

    public String getString(int index) throws SQLException
    {
        checkIndex(index);
        return getString(values.get(index - 1));
    }

    public String getString(String name) throws SQLException
    {
        checkName(name);
        return getString(indexMap.get(name).intValue());
    }

    private String getString(TypedColumn column) throws SQLException
    {
        checkNotClosed();
        Object value = column.getValue();
        wasNull = value == null;
        return (wasNull) ? null : value.toString();
    }

    public Time getTime(int index) throws SQLException
    {
        checkIndex(index);
        return getTime(values.get(index - 1));
    }

    public Time getTime(int index, Calendar calendar) throws SQLException
    {
        checkIndex(index);
        // silently ignore the Calendar argument; its a hint we do not need
        return getTime(index);
    }

    public Time getTime(String name) throws SQLException
    {
        checkName(name);
        return getTime(indexMap.get(name).intValue());
    }

    public Time getTime(String name, Calendar calendar) throws SQLException
    {
        checkName(name);
        // silently ignore the Calendar argument; its a hint we do not need
        return getTime(name);
    }

    private Time getTime(TypedColumn column) throws SQLException
    {
        checkNotClosed();
        Object value = column.getValue();
        wasNull = value == null;

        if (wasNull) return null;

        if (value instanceof Long) return new Time((Long) value);

        if (value instanceof java.util.Date) return new Time(((java.util.Date) value).getTime());

        try
        {
            if (value instanceof String) return Time.valueOf((String) value);
        }
        catch (IllegalArgumentException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, value.getClass().getSimpleName(), "SQL Time"));
    }

    public Timestamp getTimestamp(int index) throws SQLException
    {
        checkIndex(index);
        return getTimestamp(values.get(index - 1));
    }

    public Timestamp getTimestamp(int index, Calendar calendar) throws SQLException
    {
        checkIndex(index);
        // silently ignore the Calendar argument; its a hint we do not need
        return getTimestamp(index);
    }

    public Timestamp getTimestamp(String name) throws SQLException
    {
        checkName(name);
        return getTimestamp(indexMap.get(name).intValue());
    }

    public Timestamp getTimestamp(String name, Calendar calendar) throws SQLException
    {
        checkName(name);
        // silently ignore the Calendar argument; its a hint we do not need
        return getTimestamp(name);
    }

    private Timestamp getTimestamp(TypedColumn column) throws SQLException
    {
        checkNotClosed();
        Object value = column.getValue();
        wasNull = value == null;

        if (wasNull) return null;

        if (value instanceof Long) return new Timestamp((Long) value);

        if (value instanceof java.util.Date) return new Timestamp(((java.util.Date) value).getTime());

        try
        {
            if (value instanceof String) return Timestamp.valueOf((String) value);
        }
        catch (IllegalArgumentException e)
        {
            throw new SQLSyntaxErrorException(e);
        }

        throw new SQLSyntaxErrorException(String.format(NOT_TRANSLATABLE, value.getClass().getSimpleName(), "SQL Timestamp"));
    }

    public int getType() throws SQLException
    {
        checkNotClosed();
        return resultSetType;
    }

    // URL (awaiting some clarifications as to how it is stored in C* ... just a validated Sting in URL format?
    public URL getURL(int arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public URL getURL(String arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    // These Methods are planned to be  implemented soon; but not right now...
    // Each set of methods has a more detailed set of issues that should be considered fully...


    public SQLWarning getWarnings() throws SQLException
    {
        checkNotClosed();
        // the rationale is there are no warnings to return in this implementation...
        return null;
    }


    public boolean isAfterLast() throws SQLException
    {
        checkNotClosed();
        return rowNumber == Integer.MAX_VALUE;
    }

    public boolean isBeforeFirst() throws SQLException
    {
        checkNotClosed();
        return rowNumber == 0;
    }

    public boolean isClosed() throws SQLException
    {
        return values == null;
    }

    public boolean isFirst() throws SQLException
    {
        checkNotClosed();
        return rowNumber == 1;
    }

    public boolean isLast() throws SQLException
    {
        checkNotClosed();
        return !rowsIterator.hasNext();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return CassandraResultSetExtras.class.isAssignableFrom(iface);
    }

    // Navigation between rows within the returned set of rows
    // Need to use a list iterator so next() needs completely re-thought

    public boolean last() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public synchronized boolean next() throws SQLException
    {
        if (hasMoreRows())
        {
            populateColumns();
            rowNumber++;
            return true;
        }
        else
        {
            rowNumber = Integer.MAX_VALUE;
            return false;
        }
    }
    
    private String bbToString(ByteBuffer buffer)
    {
        try
        {
            return string(buffer);
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
        
    }

    private TypedColumn createColumn(Column column)
    {
        assert column != null;
        assert column.name != null;
        
        String nameType = schema.name_types.get(column.name);
        if (nameType==null) nameType = "AsciiType";
        AbstractJdbcType<?> comparator = TypesMap.getTypeForComparator(nameType == null ? schema.default_name_type : nameType);
        String valueType = schema.value_types.get(column.name);
        AbstractJdbcType<?> validator = TypesMap.getTypeForComparator(valueType == null ? schema.default_value_type : valueType);
        CollectionType type = CollectionType.NOT_COLLECTION;
        if (validator == null)
        {
            if (valueType.equals("ListType") ||valueType.equals("SetType") || valueType.equals("MapType"))
            {
                System.out.println(bbToString(column.value));
                validator = JdbcAscii.instance;
                if (valueType.equals("ListType")) type = CollectionType.LIST;
                else if (valueType.equals("SetType")) type = CollectionType.SET;
                else if (valueType.equals("MapType")) type = CollectionType.MAP;
            }
        }
        
        return new TypedColumn(column, comparator, validator);
    }

    public boolean previous() throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public boolean relative(int arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void setFetchDirection(int direction) throws SQLException
    {
        checkNotClosed();

        if (direction == FETCH_FORWARD || direction == FETCH_REVERSE || direction == FETCH_UNKNOWN)
        {
            if ((getType() == TYPE_FORWARD_ONLY) && (direction != FETCH_FORWARD))
                throw new SQLSyntaxErrorException("attempt to set an illegal direction : " + direction);
            fetchDirection = direction;
        }
        throw new SQLSyntaxErrorException(String.format(BAD_FETCH_DIR, direction));
    }

    public void setFetchSize(int size) throws SQLException
    {
        checkNotClosed();
        if (size < 0) throw new SQLException(String.format(BAD_FETCH_SIZE, size));
        fetchSize = size;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        if (iface.equals(CassandraResultSetExtras.class)) return (T) this;

        throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    public boolean wasNull() throws SQLException
    {
        return wasNull;
    }

    /**
     * RSMD implementation.  The metadata returned refers to the column
     * values, not the column names.
     */
    class CResultSetMetaData implements ResultSetMetaData
    {
        /**
         * return the Cassandra Cluster Name as the Catalog
         */
        public String getCatalogName(int column) throws SQLException
        {
            checkIndex(column);
            return statement.connection.cluster;
        }

        public String getColumnClassName(int column) throws SQLException
        {
            checkIndex(column);
            return values.get(column - 1).getValueType().getType().getName();
        }

        public int getColumnCount() throws SQLException
        {
            return values.size();
        }

        public int getColumnDisplaySize(int column) throws SQLException
        {
            checkIndex(column);
            return values.get(column - 1).getValueString().length();
        }

        public String getColumnLabel(int column) throws SQLException
        {
            checkIndex(column);
            return getColumnName(column);
        }

        public String getColumnName(int column) throws SQLException
        {
            checkIndex(column);
            return values.get(column - 1).getNameString();
        }

        public int getColumnType(int column) throws SQLException
        {
            checkIndex(column);
            return values.get(column - 1).getValueType().getJdbcType();
        }

        /**
         * Spec says "database specific type name"; for Cassandra this means the AbstractType.
         */
        public String getColumnTypeName(int column) throws SQLException
        {
            checkIndex(column);
            return values.get(column - 1).getValueType().getClass().getSimpleName();
        }

        public int getPrecision(int column) throws SQLException
        {
            checkIndex(column);
            TypedColumn col = values.get(column - 1);
            return col.getValueType().getPrecision(col.getValue());
        }

        public int getScale(int column) throws SQLException
        {
            checkIndex(column);
            TypedColumn tc = values.get(column - 1);
            return tc.getValueType().getScale(tc.getValue());
        }

        /**
         * return the DEFAULT current Keyspace as the Schema Name
         */
        public String getSchemaName(int column) throws SQLException
        {
            checkIndex(column);
            return statement.connection.currentKeyspace;
        }

        public String getTableName(int column) throws SQLException
        {
            throw new SQLFeatureNotSupportedException();
        }

        public boolean isAutoIncrement(int column) throws SQLException
        {
            checkIndex(column);
            return values.get(column - 1).getValueType() instanceof JdbcCounterColumn; // todo: check Value is correct.
        }

        public boolean isCaseSensitive(int column) throws SQLException
        {
            checkIndex(column);
            TypedColumn tc = values.get(column - 1);
            return tc.getValueType().isCaseSensitive();
        }

        public boolean isCurrency(int column) throws SQLException
        {
            checkIndex(column);
            TypedColumn tc = values.get(column - 1);
            return tc.getValueType().isCurrency();
        }

        public boolean isDefinitelyWritable(int column) throws SQLException
        {
            checkIndex(column);
            return isWritable(column);
        }

        /**
         * absence is the equivalent of null in Cassandra
         */
        public int isNullable(int column) throws SQLException
        {
            checkIndex(column);
            return ResultSetMetaData.columnNullable;
        }

        public boolean isReadOnly(int column) throws SQLException
        {
            checkIndex(column);
            return column == 0;
        }

        public boolean isSearchable(int column) throws SQLException
        {
            checkIndex(column);
            return false;
        }

        public boolean isSigned(int column) throws SQLException
        {
            checkIndex(column);
            TypedColumn tc = values.get(column - 1);
            return tc.getValueType().isSigned();
        }

        public boolean isWrapperFor(Class<?> iface) throws SQLException
        {
            return false;
        }

        public boolean isWritable(int column) throws SQLException
        {
            checkIndex(column);
            return column > 0;
        }

        public <T> T unwrap(Class<T> iface) throws SQLException
        {
            throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
        }
    }
}
