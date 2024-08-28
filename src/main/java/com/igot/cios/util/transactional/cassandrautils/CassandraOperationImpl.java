package com.igot.cios.util.transactional.cassandrautils;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Builder;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;


import com.igot.cios.util.ApiResponse;
import com.igot.cios.util.Constants;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.Map.Entry;


/**
 * @author Mahesh RV
 * @author Ruksana
 */
@Component
public class CassandraOperationImpl implements CassandraOperation {

    private Logger logger = LoggerFactory.getLogger(getClass().getName());

    @Autowired
    CassandraConnectionManager connectionManager;

    private Select processQuery(String keyspaceName, String tableName, Map<String, Object> propertyMap,
                                List<String> fields) {
        Select selectQuery = null;

        Builder selectBuilder;
        if (CollectionUtils.isNotEmpty(fields)) {
            String[] dbFields = fields.toArray(new String[fields.size()]);
            selectBuilder = QueryBuilder.select(dbFields);
        } else {
            selectBuilder = QueryBuilder.select().all();
        }
        selectQuery = selectBuilder.from(keyspaceName, tableName);
        if (MapUtils.isNotEmpty(propertyMap)) {
            Where selectWhere = selectQuery.where();
            for (Entry<String, Object> entry : propertyMap.entrySet()) {
                if (entry.getValue() instanceof List) {
                    List<Object> list = (List) entry.getValue();
                    if (null != list) {
                        Object[] propertyValues = list.toArray(new Object[list.size()]);
                        Clause clause = QueryBuilder.in(entry.getKey(), propertyValues);
                        selectWhere.and(clause);

                    }
                } else {

                    Clause clause = QueryBuilder.eq(entry.getKey(), entry.getValue());
                    selectWhere.and(clause);

                }
                selectQuery.allowFiltering();
            }
        }
        return selectQuery;
    }

    private Select processQueryWithoutFiltering(String keyspaceName, String tableName, Map<String, Object> propertyMap,
                                                List<String> fields) {
        Select selectQuery = null;
        Builder selectBuilder;
        if (CollectionUtils.isNotEmpty(fields)) {
            String[] dbFields = fields.toArray(new String[fields.size()]);
            selectBuilder = QueryBuilder.select(dbFields);
        } else {
            selectBuilder = QueryBuilder.select().all();
        }
        selectQuery = selectBuilder.from(keyspaceName, tableName);
        if (MapUtils.isNotEmpty(propertyMap)) {
            Where selectWhere = selectQuery.where();
            for (Entry<String, Object> entry : propertyMap.entrySet()) {
                if (entry.getValue() instanceof List) {
                    List<Object> list = (List) entry.getValue();
                    if (null != list) {
                        Object[] propertyValues = list.toArray(new Object[list.size()]);
                        Clause clause = QueryBuilder.in(entry.getKey(), propertyValues);
                        selectWhere.and(clause);
                    }
                } else {
                    Clause clause = QueryBuilder.eq(entry.getKey(), entry.getValue());
                    selectWhere.and(clause);
                }
            }
        }
        return selectQuery;
    }

    @Override
    public Object insertRecord(String keyspaceName, String tableName, Map<String, Object> request) {
        ApiResponse response = new ApiResponse();
        String query = CassandraUtil.getPreparedStatement(keyspaceName, tableName, request);
        try {
            PreparedStatement statement = connectionManager.getSession(keyspaceName).prepare(query);
            BoundStatement boundStatement = new BoundStatement(statement);
            Iterator<Object> iterator = request.values().iterator();
            Object[] array = new Object[request.keySet().size()];
            int i = 0;
            while (iterator.hasNext()) {
                array[i++] = iterator.next();
            }
            connectionManager.getSession(keyspaceName).execute(boundStatement.bind(array));
            response.put(Constants.RESPONSE, Constants.SUCCESS);
        } catch (Exception e) {
            String errMsg = String.format("Exception occurred while inserting record to %s %s", tableName, e.getMessage());
            logger.error(errMsg);
            response.put(Constants.RESPONSE, Constants.FAILED);
            response.put(Constants.ERROR_MESSAGE, errMsg);
        }
        return response;
    }

    @Override
    public List<Map<String, Object>> getRecordsByPropertiesWithoutFiltering(String keyspaceName, String tableName, Map<String, Object> propertyMap, List<String> fields, Integer limit) {
        Select selectQuery = null;
        List<Map<String, Object>> response = new ArrayList<>();
        try {
            selectQuery = processQueryWithoutFiltering(keyspaceName, tableName, propertyMap, fields);
            if (limit != null) {
                selectQuery = selectQuery.limit(limit);
            }
            ResultSet results = connectionManager.getSession(keyspaceName).execute(selectQuery);
            response = CassandraUtil.createResponse(results);

        } catch (Exception e) {
            logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
        }
        return response;
    }

    @Override
    public Map<String, Object> updateRecord(String keyspaceName, String tableName, Map<String, Object> updateAttributes,
        Map<String, Object> compositeKey) {
        Map<String, Object> response = new HashMap<>();
        Statement updateQuery = null;
        try {
            Session session = connectionManager.getSession(keyspaceName);
            Update update = QueryBuilder.update(keyspaceName, tableName);
            Assignments assignments = update.with();
            Update.Where where = update.where();
            updateAttributes.entrySet().stream().forEach(x -> {
                assignments.and(QueryBuilder.set(x.getKey(), x.getValue()));
            });
            compositeKey.entrySet().stream().forEach(x -> {
                where.and(QueryBuilder.eq(x.getKey(), x.getValue()));
            });
            updateQuery = where;
            session.execute(updateQuery);
            response.put(Constants.RESPONSE, Constants.SUCCESS);
        } catch (Exception e) {
            String errMsg = String.format("Exception occurred while updating record to %s %s", tableName, e.getMessage());
            logger.error(errMsg);
            response.put(Constants.RESPONSE, Constants.FAILED);
            response.put(Constants.ERROR_MESSAGE, errMsg);
            throw e;
        }
        return response;
    }

    @Override
    public List<Map<String, Object>> getRecordsByProperties(String keyspaceName, String tableName,
                                                            Map<String, Object> propertyMap, List<String> fields) {
        Select selectQuery = null;
        List<Map<String, Object>> response = new ArrayList<>();
        try {
            selectQuery = processQuery(keyspaceName, tableName, propertyMap, fields);
            ResultSet results = connectionManager.getSession(keyspaceName).execute(selectQuery);
            response = CassandraUtil.createResponse(results);

        } catch (Exception e) {
            logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
        }
        return response;
    }
}
