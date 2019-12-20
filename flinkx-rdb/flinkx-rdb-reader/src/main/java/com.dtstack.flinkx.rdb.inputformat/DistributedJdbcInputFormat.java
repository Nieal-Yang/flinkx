/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.rdb.inputformat;

import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.rdb.DataSource;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.datareader.DistributedIncrementConfig;
import com.dtstack.flinkx.rdb.datareader.QuerySqlBuilder;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.rdb.util.DBUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * InputFormat for reading data from multiple database and generate Rows.
 *
 * @author jiangbo
 * @Company: www.dtstack.com
 */
public class DistributedJdbcInputFormat extends RichInputFormat {

    protected static final long serialVersionUID = 1L;

    protected DatabaseInterface databaseInterface;

    protected int numPartitions;

    protected String driverName;

    protected boolean hasNext;

    protected int columnCount;

    protected int resultSetType;

    protected int resultSetConcurrency;

    protected List<String> descColumnTypeList;

    protected List<DataSource> sourceList;

    protected transient int sourceIndex;

    protected transient Connection currentConn;

    protected transient Statement currentStatement;

    protected transient ResultSet currentResultSet;

    protected transient Row currentRecord;

    protected String username;

    protected String password;

    protected String splitKey;

    protected String where;

    protected List<MetaColumn> metaColumns;

    protected TypeConverterInterface typeConverter;

    protected int fetchSize;

    protected int queryTimeOut;

    protected DistributedIncrementConfig incrementConfig;

    public DistributedJdbcInputFormat() {
        resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public void configure(Configuration configuration) {
        // null
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        try {
            ClassUtil.forName(driverName, getClass().getClassLoader());
            sourceList = ((DistributedJdbcInputSplit) inputSplit).getSourceList();
        } catch (Exception e) {
            throw new IllegalArgumentException("open() failed." + e.getMessage(), e);
        }

        LOG.info("JdbcInputFormat[{}}]open: end", jobName);
    }

    protected void openNextSource() throws SQLException {
        DataSource currentSource = sourceList.get(sourceIndex);
        currentConn = DBUtil.getConnection(currentSource.getJdbcUrl(), currentSource.getUserName(), currentSource.getPassword());
        currentConn.setAutoCommit(false);
        String queryTemplate = new QuerySqlBuilder(databaseInterface, currentSource.getTable(), metaColumns, splitKey,
                where, currentSource.isSplitByKey(), incrementConfig.isIncrement(), false).buildSql();
        currentStatement = currentConn.createStatement(resultSetType, resultSetConcurrency);

        if (currentSource.isSplitByKey()) {
            String n = currentSource.getParameterValues()[0].toString();
            String m = currentSource.getParameterValues()[1].toString();
            queryTemplate = queryTemplate.replace("${N}", n).replace("${M}", m);

            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Executing '%s' with parameters %s", queryTemplate,
                        Arrays.deepToString(currentSource.getParameterValues())));
            }
        }

        if (incrementConfig.isIncrement()) {
            queryTemplate = buildIncrementSql(
                    incrementConfig.getColumnType(),
                    queryTemplate,
                    incrementConfig.getColumnName(),
                    incrementConfig.getStartLocation(),
                    incrementConfig.getEndLocation(),
                    true
            );
        }

        currentStatement.setFetchSize(fetchSize);
        currentStatement.setQueryTimeout(queryTimeOut);
        currentResultSet = currentStatement.executeQuery(queryTemplate);
        columnCount = currentResultSet.getMetaData().getColumnCount();

        if (descColumnTypeList == null) {
            descColumnTypeList = DBUtil.analyzeTable(currentSource.getJdbcUrl(), currentSource.getUserName(),
                    currentSource.getPassword(), databaseInterface, currentSource.getTable(), metaColumns);
        }

        LOG.info("open source: {} ,table: {}", currentSource.getJdbcUrl(), currentSource.getTable());
    }

    public String buildIncrementSql(String increColType, String queryTemplate, String increCol,
                                    String startLocation, String endLocation, boolean useMaxFunc) {
        String incrementFilter = buildIncrementFilter(increColType,
                increCol,
                startLocation,
                endLocation,
                useMaxFunc);
        if (StringUtils.isNotEmpty(incrementFilter)) {
            incrementFilter = " and " + incrementFilter;
        }

        return queryTemplate.replace(DBUtil.INCREMENT_FILTER_PLACEHOLDER, incrementFilter);
    }

    protected String buildIncrementFilter(String incrementColType, String incrementCol, String startLocation, String endLocation, boolean useMaxFunc) {
        LOG.info("buildIncrementFilter, incrementColType = {}, incrementCol = {}, startLocation = {}, , useMaxFunc = {}", incrementColType, incrementCol, startLocation, useMaxFunc);
        StringBuilder filter = new StringBuilder(128);
        incrementCol = databaseInterface.quoteColumn(incrementCol);

        String startFilter = buildStartLocationSql(incrementColType, incrementCol, startLocation, useMaxFunc);
        if (org.apache.commons.lang.StringUtils.isNotEmpty(startFilter)) {
            filter.append(startFilter);
        }

        String endFilter = buildEndLocationSql(incrementColType, incrementCol, endLocation);
        if (org.apache.commons.lang.StringUtils.isNotEmpty(endFilter)) {
            if (filter.length() > 0) {
                filter.append(" and ").append(endFilter);
            } else {
                filter.append(endFilter);
            }
        }

        return filter.toString();
    }

    /**
     * 构建起始位置sql
     *
     * @param incrementColType 增量字段类型
     * @param incrementCol     增量字段名称
     * @param startLocation    开始位置
     * @param useMaxFunc       是否保存结束位置数据
     * @return
     */
    protected String buildStartLocationSql(String incrementColType, String incrementCol, String startLocation, boolean useMaxFunc) {
        if (org.apache.commons.lang.StringUtils.isEmpty(startLocation) || DBUtil.NULL_STRING.equalsIgnoreCase(startLocation)) {
            return null;
        }

        String operator = useMaxFunc ? " >= " : " > ";

        return getLocationSql(incrementColType, incrementCol, startLocation, operator);
    }

    /**
     * 构建结束位置sql
     *
     * @param incrementColType 增量字段类型
     * @param incrementCol     增量字段名称
     * @param endLocation      结束位置
     * @return
     */
    public String buildEndLocationSql(String incrementColType, String incrementCol, String endLocation) {
        if (org.apache.commons.lang.StringUtils.isEmpty(endLocation) || DBUtil.NULL_STRING.equalsIgnoreCase(endLocation)) {
            return null;
        }

        return getLocationSql(incrementColType, incrementCol, endLocation, " < ");
    }

    /**
     * 构建边界位置sql
     *
     * @param incrementColType 增量字段类型
     * @param incrementCol     增量字段名称
     * @param location         边界位置(起始/结束)
     * @param operator         判断符( >, >=,  <)
     * @return
     */
    protected String getLocationSql(String incrementColType, String incrementCol, String location, String operator) {
        String endTimeStr;
        String endLocationSql;
        if (ColumnType.isTimeType(incrementColType)) {
            endTimeStr = getTimeStr(Long.parseLong(location));
            endLocationSql = incrementCol + operator + endTimeStr;
        } else if (ColumnType.isNumberType(incrementColType)) {
            endLocationSql = incrementCol + operator + location;
        } else {
            endTimeStr = String.format("'%s'", location);
            endLocationSql = incrementCol + operator + endTimeStr;
        }

        return endLocationSql;
    }

    /**
     * 构建时间边界字符串
     *
     * @param location 边界位置(起始/结束)
     * @return
     */
    private String getTimeStr(Long location) {
        String timeStr;
        Timestamp ts = new Timestamp(DBUtil.getMillis(location));
        ts.setNanos(DBUtil.getNanos(location));
        timeStr = DBUtil.getNanosTimeStr(ts.toString());
        timeStr = timeStr.substring(0, 26);
        timeStr = String.format("'%s'", timeStr);

        return timeStr;
    }

    protected boolean readNextRecord() throws IOException {
        try {
            if (currentConn == null) {
                openNextSource();
            }

            hasNext = currentResultSet.next();
            if (hasNext) {
                currentRecord = new Row(columnCount);
                if (!"*".equals(metaColumns.get(0).getName())) {
                    for (int i = 0; i < columnCount; i++) {
                        Object val = currentRecord.getField(i);
                        if (val == null && metaColumns.get(i).getValue() != null) {
                            val = metaColumns.get(i).getValue();
                        }

                        if (val instanceof String) {
                            val = StringUtil.string2col(String.valueOf(val), metaColumns.get(i).getType(), metaColumns.get(i).getTimeFormat());
                            currentRecord.setField(i, val);
                        }
                    }
                }
            } else {
                if (sourceIndex + 1 < sourceList.size()) {
                    closeCurrentSource();
                    sourceIndex++;
                    return readNextRecord();
                }
            }

            return !hasNext;
        } catch (SQLException se) {
            throw new IOException("Couldn't read data - " + se.getMessage(), se);
        } catch (Exception npe) {
            throw new IOException("Couldn't access resultSet", npe);
        }
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        return currentRecord;
    }

    protected void closeCurrentSource() {
        try {
            DBUtil.closeDBResources(currentResultSet, currentStatement, currentConn, true);
            currentConn = null;
            currentStatement = null;
            currentResultSet = null;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void closeInternal() throws IOException {

    }

    @Override
    public InputSplit[] createInputSplits(int minPart) throws IOException {
        DistributedJdbcInputSplit[] inputSplits = new DistributedJdbcInputSplit[numPartitions];

        if (splitKey != null && splitKey.length() > 0) {
            Object[][] parmeter = DBUtil.getParameterValues(numPartitions);
            for (int j = 0; j < numPartitions; j++) {
                DistributedJdbcInputSplit split = new DistributedJdbcInputSplit(j, numPartitions);
                List<DataSource> sourceCopy = deepCopyList(sourceList);
                for (int i = 0; i < sourceCopy.size(); i++) {
                    sourceCopy.get(i).setSplitByKey(true);
                    sourceCopy.get(i).setParameterValues(parmeter[j]);
                }
                split.setSourceList(sourceCopy);
                inputSplits[j] = split;
            }
        } else {
            int partNum = sourceList.size() / numPartitions;
            if (partNum == 0) {
                for (int i = 0; i < sourceList.size(); i++) {
                    DistributedJdbcInputSplit split = new DistributedJdbcInputSplit(i, numPartitions);
                    split.setSourceList(Arrays.asList(sourceList.get(i)));
                    inputSplits[i] = split;
                }
            } else {
                for (int j = 0; j < numPartitions; j++) {
                    DistributedJdbcInputSplit split = new DistributedJdbcInputSplit(j, numPartitions);
                    split.setSourceList(new ArrayList<>(sourceList.subList(j * partNum, (j + 1) * partNum)));
                    inputSplits[j] = split;
                }

                if (partNum * numPartitions < sourceList.size()) {
                    int base = partNum * numPartitions;
                    int size = sourceList.size() - base;
                    for (int i = 0; i < size; i++) {
                        DistributedJdbcInputSplit split = inputSplits[i];
                        split.getSourceList().add(sourceList.get(i + base));
                    }
                }
            }
        }

        return inputSplits;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return readNextRecord();
    }

    public <T> List<T> deepCopyList(List<T> src) throws IOException {
        try {
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(byteOut);
            out.writeObject(src);

            ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
            ObjectInputStream in = new ObjectInputStream(byteIn);
            List<T> dest = (List<T>) in.readObject();

            return dest;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}