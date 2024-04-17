package com.giangbb.scylla.core;

import com.datastax.oss.driver.api.core.*;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.mapper.MapperException;
import com.datastax.oss.driver.internal.core.cql.ResultSets;
import org.springframework.data.projection.EntityProjection;
import org.springframework.util.Assert;
import com.giangbb.scylla.config.SessionFactoryFactoryBean;
import com.giangbb.scylla.core.convert.ScyllaConverter;
import com.giangbb.scylla.core.cql.ResultSetExtractor;
import com.giangbb.scylla.core.cql.RowMapper;
import com.giangbb.scylla.core.cql.RowMapperResultSetExtractor;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by Giangbb on 12/04/2024
 */
public class ScyllaTemplate implements ScyllaOperations{
    private static final CqlIdentifier APPLIED = CqlIdentifier.fromInternal("[applied]");

    private final CqlSession cqlSession;
    private final ScyllaConverter scyllaConverter;
    protected final EntityOperations entityOperations;

    public ScyllaTemplate(SessionFactoryFactoryBean scyllaSessionFactory) {
        Assert.notNull(scyllaSessionFactory, "SessionFactoryFactoryBean must not be null");
        this.cqlSession = scyllaSessionFactory.getSession();
        this.scyllaConverter = scyllaSessionFactory.getConverter();
        this.entityOperations = new EntityOperations(scyllaConverter);
    }

    public CqlSession getCqlSession() {
        return cqlSession;
    }

    public ScyllaConverter getScyllaConverter() {
        return scyllaConverter;
    }

    public <EntityT> RowMapperResultSetExtractor<EntityT> getRowMapperResultSetExtractor(Class<EntityT> tClass, CqlIdentifier tableName) {
        EntityProjection<EntityT, ?> projection = entityOperations.introspectProjection(tClass, tClass);
        Function<Row, EntityT> mapper = getMapper(projection, tableName);
        RowMapperResultSetExtractor<EntityT> resultSetExtractor = newResultSetExtractor((row, rowNum) -> mapper.apply(row));
        return resultSetExtractor;
    }

    /**
     * Constructs a new instance of the {@link ResultSetExtractor} adapting the given {@link RowMapper}.
     *
     * @param rowMapper {@link RowMapper} to adapt as a {@link ResultSetExtractor}.
     * @return a {@link ResultSetExtractor} implementation adapting an instance of the {@link RowMapper}.
     * @see ResultSetExtractor
     * @see RowMapper
     * @see RowMapperResultSetExtractor
     */
    protected <T> RowMapperResultSetExtractor<T> newResultSetExtractor(RowMapper<T> rowMapper) {
        return new RowMapperResultSetExtractor<>(rowMapper);
    }

    public <EntityT> Function<Row, EntityT> getSingleRowMapper(Class<EntityT> tClass, CqlIdentifier tableName) {
        Function<Row, EntityT> mapper = getMapper(EntityProjection.nonProjecting(tClass), tableName);
        return mapper;
    }

    private <T> Function<Row, T> getMapper(EntityProjection<T, ?> projection, CqlIdentifier tableName) {
        Class<T> targetType = projection.getMappedType().getType();
        return row -> {
            T result = getScyllaConverter().project(projection, row);
            return result;
        };
    }




    @Override
    public ResultSet execute(Statement<?> statement) {
        return this.cqlSession.execute(statement);
    }

    @Override
    public boolean executeAndMapWasAppliedToBoolean(Statement<?> statement) {
        ResultSet rs = this.execute(statement);
        return rs.wasApplied();
    }

    @Override
    public long executeAndMapFirstColumnToLong(Statement<?> statement) {
        Row row = this.executeAndExtractFirstRow(statement);
        return this.extractCount(row);
    }

    @Override
    public long extractCount(Row row) {
        if (row == null) {
            throw new MapperException("Expected the query to return at least one row (return type long is intended for COUNT queries)");
        } else if (row.getColumnDefinitions().size() != 0 && row.getColumnDefinitions().get(0).getType().equals(DataTypes.BIGINT)) {
            return row.getLong(0);
        } else {
            throw new MapperException("Expected the query to return a column with CQL type BIGINT in first position (return type long is intended for COUNT queries)");
        }
    }

    @Override
    public Row executeAndExtractFirstRow(Statement<?> statement) {
        return (Row)this.execute(statement).one();
    }

    @Override
    public <EntityT> EntityT executeAndMapToSingleEntity(Statement<?> statement, Function<Row, EntityT> mapper) {
        ResultSet rs = this.execute(statement);
        return this.asEntity((Row)rs.one(), mapper);
    }

    private <EntityT> EntityT asEntity(Row row, Function<Row, EntityT> mapper) {
        return row != null && (row.getColumnDefinitions().size() != 1 || !row.getColumnDefinitions().get(0).getName().equals(APPLIED)) ? mapper.apply(row) : null;
    }

    @Override
    public <EntityT> Optional<EntityT> executeAndMapToOptionalEntity(Statement<?> statement, Function<Row, EntityT> mapper) {
        return Optional.ofNullable(this.executeAndMapToSingleEntity(statement, mapper));
    }

    @Override
    public <EntityT> List<EntityT> executeAndMapToListEntity(Statement<?> statement, RowMapperResultSetExtractor<EntityT> resultSetExtractor) {
        ResultSet resultSet = this.execute(statement);
        return resultSetExtractor.extractData(resultSet);
    }

    @Override
    public <EntityT> PagingIterable<EntityT> executeAndMapToEntityIterable(Statement<?> statement, Function<Row, EntityT> mapper) {
        return this.execute(statement).map(mapper::apply);
    }

    @Override
    public <EntityT> Stream<EntityT> executeAndMapToEntityStream(Statement<?> statement, Function<Row, EntityT> mapper) {
        return StreamSupport.stream(this.execute(statement).map(mapper::apply).spliterator(), false);
    }

    @Override
    public CompletableFuture<AsyncResultSet> executeAsync(Statement<?> statement) {
        CompletionStage<AsyncResultSet> stage = cqlSession.executeAsync(statement);
        return stage.toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> executeAsyncAndMapToVoid(Statement<?> statement) {
        return this.executeAsync(statement).thenApply((rs) -> null);
    }

    @Override
    public CompletableFuture<Boolean> executeAsyncAndMapWasAppliedToBoolean(Statement<?> statement) {
        return this.executeAsync(statement).thenApply(AsyncResultSet::wasApplied);
    }

    @Override
    public CompletableFuture<Long> executeAsyncAndMapFirstColumnToLong(Statement<?> statement) {
        return this.executeAsyncAndExtractFirstRow(statement).thenApply(this::extractCount);
    }

    @Override
    public CompletableFuture<Row> executeAsyncAndExtractFirstRow(Statement<?> statement) {
        return this.executeAsync(statement).thenApply(AsyncPagingIterable::one);
    }

    @Override
    public <EntityT> CompletableFuture<EntityT> executeAsyncAndMapToSingleEntity(Statement<?> statement, Function<Row, EntityT> mapper) {
        return this.executeAsync(statement).thenApply((rs) -> this.asEntity((Row)rs.one(), mapper));
    }

    @Override
    public <EntityT> CompletableFuture<Optional<EntityT>> executeAsyncAndMapToOptionalEntity(Statement<?> statement, Function<Row, EntityT> mapper) {
        return this.executeAsync(statement).thenApply((rs) -> Optional.ofNullable(this.asEntity((Row)rs.one(), mapper)));
    }

    @Override
    public <EntityT> CompletableFuture<MappedAsyncPagingIterable<EntityT>> executeAsyncAndMapToEntityIterable(Statement<?> statement, Function<Row, EntityT> mapper) {
        return this.executeAsync(statement).thenApply((rs) -> rs.map(mapper::apply));
    }

    @Override
    public <EntityT> CompletableFuture<Stream<EntityT>> executeAsyncAndMapToEntityStream(Statement<?> statement, Function<Row, EntityT> mapper) {
        return this.executeAsync(statement).thenApply(ResultSets::newInstance).thenApply((rs) -> {
            Objects.requireNonNull(mapper);
            return StreamSupport.stream(rs.map(mapper::apply).spliterator(), false);
        });
    }



}
