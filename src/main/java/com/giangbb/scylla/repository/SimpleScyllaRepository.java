package com.giangbb.scylla.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;
import com.giangbb.scylla.core.ScyllaTemplate;
import com.giangbb.scylla.core.convert.MappingScyllaConverter;
import com.giangbb.scylla.core.convert.ScyllaColumnType;
import com.giangbb.scylla.core.cql.RowMapperResultSetExtractor;
import com.giangbb.scylla.core.mapping.MapId;
import com.giangbb.scylla.core.mapping.ScyllaPersistentProperty;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Created by Giangbb on 04/03/2024
 */
public class SimpleScyllaRepository<T> implements ScyllaRepository<T> {
    protected final Log logger = LogFactory.getLog(getClass());

    public static final String bindMarker_TTL = "ttl";

    protected final Class<T> tClass;
    protected final MappingScyllaConverter scyllaConverter;
    protected final ScyllaTemplate scyllaTemplate;

    protected final ScyllaEntityHelperImpl<T> scyllaEntityHelperImpl;


    private PreparedStatement saveStatement;
    private PreparedStatement saveWithTtlStatement;
    private PreparedStatement saveIfExistsStatement;
    private PreparedStatement selectByPrimaryKeyStatement;
    private PreparedStatement selectByPartitionKeyStatement;
    private PreparedStatement countAllStatement;
    private PreparedStatement countByPartitionKeyStatement;
    private PreparedStatement findAllStatement;
    private PreparedStatement deleteStatement;
    private PreparedStatement deleteAllStatement;


    public SimpleScyllaRepository(Class<T> tClass, ScyllaTemplate scyllaTemplate) {
        Assert.notNull(tClass, "T Class must not be null");
        Assert.notNull(scyllaTemplate, "ScyllaTemplate must not be null");
        this.tClass = tClass;
        this.scyllaTemplate = scyllaTemplate;
        this.scyllaConverter = (MappingScyllaConverter) scyllaTemplate.getScyllaConverter();
        this.scyllaEntityHelperImpl = new ScyllaEntityHelperImpl<T>(tClass, scyllaTemplate.getCqlSession(), scyllaConverter);
        this.initStatement();
    }

    protected CqlSession getCqlSession() {
        return this.scyllaTemplate.getCqlSession();
    }

    public ScyllaEntityHelperImpl<T> getScyllaEntityHelperImpl() {
        return this.scyllaEntityHelperImpl;
    }



    private void initStatement() {
        SimpleStatement saveStatement_simple = this.scyllaEntityHelperImpl.insert().build();
        this.saveStatement = this.prepare(saveStatement_simple);

        SimpleStatement saveWithTtlStatement_simple = this.scyllaEntityHelperImpl.insert().usingTtl(QueryBuilder.bindMarker(bindMarker_TTL)).build();
        this.saveWithTtlStatement = this.prepare(saveWithTtlStatement_simple);

        //update by primkey
        SimpleStatement saveIfExistsStatement_simple = SimpleStatement.newInstance((this.scyllaEntityHelperImpl.updateByPrimaryKey()).ifExists().asCql());
        this.saveIfExistsStatement = this.prepare(saveIfExistsStatement_simple);

        SimpleStatement findAllStatement_simple = this.scyllaEntityHelperImpl.selectStart().build();
        this.findAllStatement = this.prepare(findAllStatement_simple);

        //delete by primkey
        SimpleStatement deleteStatement_simple = this.scyllaEntityHelperImpl.deleteByPrimaryKey().build();
        this.deleteStatement = this.prepare(deleteStatement_simple);


        SimpleStatement deleteAllStatement_simple = this.scyllaEntityHelperImpl.deleteAll().build();
        this.deleteAllStatement = this.prepare(deleteAllStatement_simple);

        SimpleStatement selectByPrimaryKeyStatement_simple = this.scyllaEntityHelperImpl.selectByPrimaryKey().build();
        this.selectByPrimaryKeyStatement = this.prepare(selectByPrimaryKeyStatement_simple);

        SimpleStatement selectByPartitionKeyStatement_simple = this.scyllaEntityHelperImpl.selectByPartitionKey().build();
        this.selectByPartitionKeyStatement = this.prepare(selectByPartitionKeyStatement_simple);

        SimpleStatement countAllStatement_simple = this.scyllaEntityHelperImpl.selectCountStart().build();
        this.countAllStatement = this.prepare(countAllStatement_simple);

        SimpleStatement countByPartitionKeyStatement_simple = this.scyllaEntityHelperImpl.selectCountByPartitionKey().build();
        this.countByPartitionKeyStatement = this.prepare(countByPartitionKeyStatement_simple);

    }

    protected PreparedStatement prepare(SimpleStatement simpleStatement){
        return this.getCqlSession().prepare(simpleStatement);
    }

    protected <U> UdtValue marshallUDTValue(String columnName, U u){
        ScyllaPersistentProperty property = this.findScyllaPersistentProperty(columnName, u);
        if (property == null){
            throw new IllegalArgumentException("not found Property: " + columnName+  " for Entity: " + this.scyllaEntityHelperImpl.getPersistentEntity().getTableName());
        }

        ScyllaColumnType columnType = this.scyllaConverter.resolve(property);
        if (!columnType.isUserDefinedType()){
            throw new IllegalArgumentException("not found UDT Property: " + columnName+  " for Entity: " + this.scyllaEntityHelperImpl.getPersistentEntity().getTableName());
        }

        UdtValue udtValue = ((UserDefinedType) columnType.getDataType()).newValue();
        this.scyllaConverter.write(u, udtValue, this.scyllaConverter.getMappingContext().getRequiredPersistentEntity(u.getClass()));
        return udtValue;
    }


    protected <U> TupleValue marshallTupleValue(String columnName, U u){
        ScyllaPersistentProperty property = this.findScyllaPersistentProperty(columnName, u);
        if (property == null){
            throw new IllegalArgumentException("not found Property: " + columnName+  " for Entity: " + this.scyllaEntityHelperImpl.getPersistentEntity().getTableName());
        }

        ScyllaColumnType columnType = this.scyllaConverter.resolve(property);
        if (!columnType.isTupleType()){
            throw new IllegalArgumentException("not found Tuple Property: " + columnName+  " for Entity: " + this.scyllaEntityHelperImpl.getPersistentEntity().getTableName());
        }

        TupleValue tupleValue = ((TupleType) columnType.getDataType()).newValue();
        this.scyllaConverter.write(u, tupleValue, this.scyllaConverter.getMappingContext().getRequiredPersistentEntity(u.getClass()));
        return tupleValue;
    }

    protected <U> ScyllaPersistentProperty findScyllaPersistentProperty(String columnName, U u){
        CqlIdentifier colName = CqlIdentifier.fromCql(columnName);
        for (ScyllaPersistentProperty scyllaPersistentProperty : this.scyllaEntityHelperImpl.getPersistentEntity()) {
            if (scyllaPersistentProperty.getColumnName().equals(colName) && scyllaPersistentProperty.getType().equals(u.getClass())) {
                return scyllaPersistentProperty;
            }
        }
        return null;
    }


    @Override
    public void save(T t) {
        this.execute(this.bindSaveStatement(t));
    }

    @Override
    public CompletionStage<Void> saveAsync(T t) {
        try {
            return this.executeAsyncAndMapToVoid(this.bindSaveStatement(t));
        } catch (Exception e) {
            return CompletableFutures.failedFuture(e);
        }
    }

    private BoundStatement bindSaveStatement(T t){
        Map<CqlIdentifier, Object> object = new LinkedHashMap<>();
        this.scyllaConverter.write(t, object, this.scyllaEntityHelperImpl.getPersistentEntity());
        Object[] values = object.values().toArray();
//        logger.info("save entity - {} \nvalues: {} \nobj: {}", this.saveStatement.getQuery(), values, object);
        return this.saveStatement.bind(values);
    }


    /**
     * @param t - entity.
     * @param ttl - time to live in second.
     */
    @Override
    public void saveWithTtl(T t, int ttl) {
        this.execute(this.bindSaveWithTtlStatement(t, ttl));
    }

    /**
     * @param t - entity.
     * @param ttl - time to live in second.
     */
    @Override
    public CompletionStage<Void> saveWithTtlAsync(T t, int ttl) {
        try {
            return this.executeAsyncAndMapToVoid(this.bindSaveWithTtlStatement(t, ttl));
        } catch (Exception e) {
            return CompletableFutures.failedFuture(e);
        }
    }

    /**
     * @param t - entity.
     * @param ttl - time to live in second.
     */
    private BoundStatement bindSaveWithTtlStatement(T t, int ttl){
        Map<CqlIdentifier, Object> object = new LinkedHashMap<>();
        this.scyllaConverter.write(t, object, this.scyllaEntityHelperImpl.getPersistentEntity());

        object.put(CqlIdentifier.fromCql(bindMarker_TTL), ttl);

        Object[] values = object.values().toArray();
//        logger.info("saveWithTtl entity - {} - {}", this.saveWithTtlStatement.getQuery(), values);
        return this.saveWithTtlStatement.bind(values);
    }


    @Override
    public boolean saveIfExists(T t) {
        return this.executeAndMapWasAppliedToBoolean(this.bindSaveIfExistsStatement(t));
    }

    @Override
    public CompletionStage<Boolean> saveIfExistsAsync(T t) {
        try {
            return this.executeAsyncAndMapWasAppliedToBoolean(this.bindSaveIfExistsStatement(t));
        } catch (Exception e) {
            return CompletableFutures.failedFuture(e);
        }
    }

    private BoundStatement bindSaveIfExistsStatement(T t){
        Map<CqlIdentifier, Object> object = new LinkedHashMap<>();
        this.scyllaConverter.write(t, object, this.scyllaEntityHelperImpl.getPersistentEntity());

        for (ScyllaPersistentProperty key : this.scyllaEntityHelperImpl.getPrimaryKeys()) {
            //move primkey to tail
            if (object.containsKey(key.getColumnName())){
                Object value = object.get(key.getColumnName());
                object.remove(key.getColumnName());
                object.put(key.getColumnName(), value);
            }
        }
        Object[] values = object.values().toArray();
//        logger.info("saveIfExistsAsync entity - {} - {}", this.saveIfExistsStatement.getQuery(), values);
        return this.saveIfExistsStatement.bind(values);
    }


    @Override
    public T findByPrimaryKey(Map<CqlIdentifier, Object> primaryKey) {
        return this.executeAndMapToSingleEntity(this.bindSelectByPrimaryKeyStatement(primaryKey));
    }

    @Override
    public CompletionStage<T> findByPrimaryKeyAsync(Map<CqlIdentifier, Object> primaryKey) {
        try {
            return this.executeAsyncAndMapToSingleEntity(this.bindSelectByPrimaryKeyStatement(primaryKey));
        } catch (Exception e) {
            return CompletableFutures.failedFuture(e);
        }
    }

    private BoundStatement bindSelectByPrimaryKeyStatement(Map<CqlIdentifier, Object> primaryKey){
        Map<CqlIdentifier, Object> object = new LinkedHashMap<>();
        this.scyllaEntityHelperImpl.getPrimaryKeys().forEach(scyllaPersistentProperty -> {
            CqlIdentifier columnName = scyllaPersistentProperty.getColumnName();
            Object value = primaryKey.get(columnName);
            if (value == null){
                throw new IllegalArgumentException("not value for key: " + columnName.toString());
            }
            object.put(columnName, value);
        });

        Object[] values = object.values().toArray();
//        logger.info("Select By PrimaryKey entity - {} - {}", this.selectByPrimaryKeyStatement.getQuery(), values);
        return this.selectByPrimaryKeyStatement.bind(values);
    }


    @Override
    public T findByPrimaryKey(T t) {
        return this.executeAndMapToSingleEntity(this.bindSelectByPrimaryKeyStatement(t));
    }

    @Override
    public CompletionStage<T> findByPrimaryKeyAsync(T t) {
        try {
            return this.executeAsyncAndMapToSingleEntity(this.bindSelectByPrimaryKeyStatement(t));
        } catch (Exception e) {
            return CompletableFutures.failedFuture(e);
        }
    }

    private BoundStatement bindSelectByPrimaryKeyStatement(T t){
        Object entityPrimaryKey = scyllaConverter.extractId(t, this.scyllaEntityHelperImpl.getPersistentEntity());
        if  (entityPrimaryKey instanceof MapId) {
            MapId entityPrimaryKeyMap = (MapId)entityPrimaryKey;

            Map<CqlIdentifier, Object> object = new LinkedHashMap<>();
            this.scyllaEntityHelperImpl.getPrimaryKeys().forEach(scyllaPersistentProperty -> {
                CqlIdentifier columnName = scyllaPersistentProperty.getColumnName();
                Object value =  entityPrimaryKeyMap.get(scyllaPersistentProperty.getField().getName());
                if (value == null){
                    throw new IllegalArgumentException("not value for key: " + columnName.toString());
                }
                object.put(columnName, value);
            });

            Object[] values = object.values().toArray();
//            logger.info("Select By PrimaryKey MapId entity - {} - {} - map object {}", this.selectByPrimaryKeyStatement.getQuery(), values, object);
            return this.selectByPrimaryKeyStatement.bind(values);
        }else{
//            logger.info("Select By PrimaryKey object entity - {} - {}", this.selectByPrimaryKeyStatement.getQuery(), entityPrimaryKey);
            return this.selectByPrimaryKeyStatement.bind(entityPrimaryKey);
        }
    }


    @Override
    public List<T> findByPartitionKey(Map<CqlIdentifier, Object> pKeys) {
        return this.executeAndMapToListEntity(bindSelectByPartitionKeyStatement(pKeys));
    }

    @Override
    public PagingIterable<T> findByPartitionKeyPagingIterable(Map<CqlIdentifier, Object> pKeys) {
        return this.executeAndMapToEntityIterable(bindSelectByPartitionKeyStatement(pKeys));
    }

    @Override
    public CompletionStage<MappedAsyncPagingIterable<T>> findByPartitionKeyAsync(Map<CqlIdentifier, Object> pKeys) {
        try {
            return this.executeAsyncAndMapToEntityIterable(bindSelectByPartitionKeyStatement(pKeys));
        } catch (Exception e) {
            return CompletableFutures.failedFuture(e);
        }
    }

    private BoundStatement bindSelectByPartitionKeyStatement(Map<CqlIdentifier, Object> pKeys){
        Map<CqlIdentifier, Object> object = new LinkedHashMap<>();
        this.scyllaEntityHelperImpl.getpKeys().forEach(scyllaPersistentProperty -> {
            CqlIdentifier columnName = scyllaPersistentProperty.getColumnName();
            Object value = pKeys.get(columnName);
            if (value == null){
                throw new IllegalArgumentException("not value for key: " + columnName.toString());
            }
            object.put(columnName, value);
        });

        Object[] values = object.values().toArray();
//        logger.info("Select By PartitionKey entity - {} - {}", this.selectByPartitionKeyStatement.getQuery(), values);
        return this.selectByPartitionKeyStatement.bind(values);
    }




    @Override
    public List<T> findByPartitionKey(T t) {
        return this.executeAndMapToListEntity(bindSelectByPartitionKeyStatement(t));
    }

    @Override
    public PagingIterable<T> findByPartitionKeyPagingIterable(T t) {
        return this.executeAndMapToEntityIterable(bindSelectByPartitionKeyStatement(t));
    }

    @Override
    public CompletionStage<MappedAsyncPagingIterable<T>> findByPartitionKeyAsync(T t) {
        try {
            return this.executeAsyncAndMapToEntityIterable(bindSelectByPartitionKeyStatement(t));
        } catch (Exception e) {
            return CompletableFutures.failedFuture(e);
        }
    }

    private BoundStatement bindSelectByPartitionKeyStatement(T t){
        Object entityPrimaryKey = scyllaConverter.extractId(t, this.scyllaEntityHelperImpl.getPersistentEntity());
        if  (entityPrimaryKey instanceof MapId) {
            MapId entityPrimaryKeyMap = (MapId)entityPrimaryKey;

            Map<CqlIdentifier, Object> object = new LinkedHashMap<>();
            this.scyllaEntityHelperImpl.getpKeys().forEach(scyllaPersistentProperty -> {
                CqlIdentifier columnName = scyllaPersistentProperty.getColumnName();
                Object value =  entityPrimaryKeyMap.get(scyllaPersistentProperty.getField().getName());
                if (value == null){
                    throw new IllegalArgumentException("not value for key: " + columnName.toString());
                }
                object.put(columnName, value);
            });

            Object[] values = object.values().toArray();
//            logger.info("Select By PartitionKey MapId entity - {} - {} - map object {}", this.selectByPartitionKeyStatement.getQuery(), values, object);
            return this.selectByPartitionKeyStatement.bind(values);
        }else{
//            logger.info("Select By PartitionKey object entity - {} - {}", this.selectByPartitionKeyStatement.getQuery(), entityPrimaryKey);
            return this.selectByPartitionKeyStatement.bind(entityPrimaryKey);
        }
    }


    @Override
    public List<T> findAll() {
        BoundStatementBuilder boundStatementBuilder = this.findAllStatement.boundStatementBuilder();
        BoundStatement boundStatement = boundStatementBuilder.build();
        return this.executeAndMapToListEntity(boundStatement);
    }

    @Override
    public PagingIterable<T> findAllPagingIterable() {
        BoundStatementBuilder boundStatementBuilder = this.findAllStatement.boundStatementBuilder();
        BoundStatement boundStatement = boundStatementBuilder.build();
        return this.executeAndMapToEntityIterable(boundStatement);
    }


    @Override
    public CompletionStage<MappedAsyncPagingIterable<T>> findAllAsync() {
        try {
            BoundStatementBuilder boundStatementBuilder = this.findAllStatement.boundStatementBuilder();
            BoundStatement boundStatement = boundStatementBuilder.build();
            return this.executeAsyncAndMapToEntityIterable(boundStatement);
        } catch (Exception e) {
            return CompletableFutures.failedFuture(e);
        }
    }

    @Override
    public void delete(T t) {
        this.execute(this.bindDeleteStatement(t));
    }

    @Override
    public CompletionStage<Void> deleteAsync(T t) {
        try {
            return this.executeAsyncAndMapToVoid(this.bindDeleteStatement(t));
        } catch (Exception e) {
            return CompletableFutures.failedFuture(e);
        }
    }

    private BoundStatement bindDeleteStatement(T t){
        Map<CqlIdentifier, Object> object = new LinkedHashMap<>();
        this.scyllaConverter.write(t, object, this.scyllaEntityHelperImpl.getPersistentEntity());

        List<CqlIdentifier> pmkeys = this.scyllaEntityHelperImpl.getPrimaryKeys().stream().map(ScyllaPersistentProperty::getColumnName).toList();

        Iterator<Map.Entry<CqlIdentifier, Object>> iterator = object.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<CqlIdentifier, Object> entry = iterator.next();
            CqlIdentifier key = entry.getKey();

            // replace "someCondition" with your actual condition
            if (!pmkeys.contains(key)) {
                iterator.remove();
            }
        }

        Object[] values = object.values().toArray();
//        logger.info("deleteStatement entity - {} - {}", this.deleteStatement.getQuery(), values);
        return this.deleteStatement.bind(values);
    }


    @Override
    public void deleteAll() {
        this.execute(this.deleteAllStatement.bind());
    }

    @Override
    public CompletionStage<Void> deleteAllAsync() {
        try {
            return this.executeAsyncAndMapToVoid(this.deleteAllStatement.bind());
        } catch (Exception e) {
            return CompletableFutures.failedFuture(e);
        }
    }

    @Override
    public long countAll() {
        return this.executeAndMapFirstColumnToLong(this.countAllStatement.bind());
    }

    @Override
    public CompletionStage<Long> countAllAsync() {
        try {
            return this.executeAsyncAndMapFirstColumnToLong(this.countAllStatement.bind());
        } catch (Exception e) {
            return CompletableFutures.failedFuture(e);
        }
    }

    @Override
    public long countByPartitionKey(Map<CqlIdentifier, Object> pKeys) {
        return this.executeAndMapFirstColumnToLong(bindCountByPartitionKeyStatement(pKeys));
    }

    @Override
    public CompletionStage<Long> countByPartitionKeyAsync(Map<CqlIdentifier, Object> pKeys) {
        try {
            return this.executeAsyncAndMapFirstColumnToLong(bindCountByPartitionKeyStatement(pKeys));
        } catch (Exception e) {
            return CompletableFutures.failedFuture(e);
        }
    }

    private BoundStatement bindCountByPartitionKeyStatement(Map<CqlIdentifier, Object> pKeys){
        Map<CqlIdentifier, Object> object = new LinkedHashMap<>();
        this.scyllaEntityHelperImpl.getpKeys().forEach(scyllaPersistentProperty -> {
            CqlIdentifier columnName = scyllaPersistentProperty.getColumnName();
            Object value = pKeys.get(columnName);
            if (value == null){
                throw new IllegalArgumentException("not value for key: " + columnName.toString());
            }
            object.put(columnName, value);
        });

        Object[] values = object.values().toArray();
//        logger.info("Count By PartitionKey entity - {} - {}", this.countByPartitionKeyStatement.getQuery(), values);
        return this.countByPartitionKeyStatement.bind(values);
    }





    @Override
    public long countByPartitionKey(T t) {
        return this.executeAndMapFirstColumnToLong(this.bindCountByPartitionKeyStatement(t));
    }

    @Override
    public CompletionStage<Long> countByPartitionKeyAsync(T t) {
        try {
            return this.executeAsyncAndMapFirstColumnToLong(this.bindCountByPartitionKeyStatement(t));
        } catch (Exception e) {
            return CompletableFutures.failedFuture(e);
        }
    }

    private BoundStatement bindCountByPartitionKeyStatement(T t){
        Object entityPrimaryKey = scyllaConverter.extractId(t, this.scyllaEntityHelperImpl.getPersistentEntity());
        if  (entityPrimaryKey instanceof MapId) {
            MapId entityPrimaryKeyMap = (MapId)entityPrimaryKey;

            Map<CqlIdentifier, Object> object = new LinkedHashMap<>();
            this.scyllaEntityHelperImpl.getpKeys().forEach(scyllaPersistentProperty -> {
                CqlIdentifier columnName = scyllaPersistentProperty.getColumnName();
                Object value =  entityPrimaryKeyMap.get(scyllaPersistentProperty.getField().getName());
                if (value == null){
                    throw new IllegalArgumentException("not value for key: " + columnName.toString());
                }
                object.put(columnName, value);
            });

            Object[] values = object.values().toArray();
//            logger.info("Count By PartitionKey MapId entity - {} - {} - map object {}", this.countByPartitionKeyStatement.getQuery(), values, object);
            return this.countByPartitionKeyStatement.bind(values);
        }else{
//            logger.info("Count By PartitionKey object entity - {} - {}", this.countByPartitionKeyStatement.getQuery(), entityPrimaryKey);
            return this.countByPartitionKeyStatement.bind(entityPrimaryKey);
        }
    }








    //region Execution
    protected Function<Row, T> getSingleRowMapper() {
        return this.scyllaTemplate.getSingleRowMapper(tClass, this.scyllaEntityHelperImpl.getTableId());
    }


    protected RowMapperResultSetExtractor<T> getRowMapperResultSetExtractor(){
        return this.scyllaTemplate.getRowMapperResultSetExtractor(tClass, this.scyllaEntityHelperImpl.getTableId());
    }

    public ResultSet execute(Statement<?> statement){
        return this.scyllaTemplate.execute(statement);
    }

    public boolean executeAndMapWasAppliedToBoolean(Statement<?> statement){
        return this.scyllaTemplate.executeAndMapWasAppliedToBoolean(statement);
    }

    public long executeAndMapFirstColumnToLong(Statement<?> statement){
        return this.scyllaTemplate.executeAndMapFirstColumnToLong(statement);
    }

    protected Row executeAndExtractFirstRow(Statement<?> statement){
        return this.scyllaTemplate.executeAndExtractFirstRow(statement);
    }

    protected  T executeAndMapToSingleEntity(Statement<?> statement){
        return this.scyllaTemplate.executeAndMapToSingleEntity(statement, this.getSingleRowMapper());
    }



    protected Optional<T> executeAndMapToOptionalEntity(Statement<?> statement){
        return this.scyllaTemplate.executeAndMapToOptionalEntity(statement, this.getSingleRowMapper());
    }


    protected List<T> executeAndMapToListEntity(Statement<?> statement){
        return this.scyllaTemplate.executeAndMapToListEntity(statement, this.getRowMapperResultSetExtractor());
    }


    protected PagingIterable<T> executeAndMapToEntityIterable(Statement<?> statement){
        return this.scyllaTemplate.executeAndMapToEntityIterable(statement, this.getSingleRowMapper());
    }


    protected Stream<T> executeAndMapToEntityStream(Statement<?> statement){
        return this.scyllaTemplate.executeAndMapToEntityStream(statement, this.getSingleRowMapper());
    }

    protected CompletableFuture<AsyncResultSet> executeAsync(Statement<?> statement){
        return this.scyllaTemplate.executeAsync(statement);
    }


    protected CompletableFuture<Void> executeAsyncAndMapToVoid(Statement<?> statement){
        return this.scyllaTemplate.executeAsyncAndMapToVoid(statement);
    }

    protected CompletableFuture<Boolean> executeAsyncAndMapWasAppliedToBoolean(Statement<?> statement){
        return this.scyllaTemplate.executeAsyncAndMapWasAppliedToBoolean(statement);
    }

    protected CompletableFuture<Long> executeAsyncAndMapFirstColumnToLong(Statement<?> statement){
        return this.scyllaTemplate.executeAsyncAndMapFirstColumnToLong(statement);
    }

    protected CompletableFuture<Row> executeAsyncAndExtractFirstRow(Statement<?> statement){
        return this.scyllaTemplate.executeAsyncAndExtractFirstRow(statement);
    }

    protected CompletableFuture<T> executeAsyncAndMapToSingleEntity(Statement<?> statement){
        return this.scyllaTemplate.executeAsyncAndMapToSingleEntity(statement, this.getSingleRowMapper());
    }

    protected CompletableFuture<Optional<T>> executeAsyncAndMapToOptionalEntity(Statement<?> statement){
        return this.scyllaTemplate.executeAsyncAndMapToOptionalEntity(statement, this.getSingleRowMapper());
    }

    protected CompletableFuture<MappedAsyncPagingIterable<T>> executeAsyncAndMapToEntityIterable(Statement<?> statement){
        return this.scyllaTemplate.executeAsyncAndMapToEntityIterable(statement, this.getSingleRowMapper());
    }

    protected CompletableFuture<Stream<T>> executeAsyncAndMapToEntityStream(Statement<?> statement){
        return this.scyllaTemplate.executeAsyncAndMapToEntityStream(statement, this.getSingleRowMapper());
    }
    //endregion
}
