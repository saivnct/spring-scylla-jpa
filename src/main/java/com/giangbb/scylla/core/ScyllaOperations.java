package com.giangbb.scylla.core;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.giangbb.scylla.core.cql.RowMapperResultSetExtractor;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Created by Giangbb on 12/04/2024
 */
public interface ScyllaOperations {

    ResultSet execute(Statement<?> statement);

    boolean executeAndMapWasAppliedToBoolean(Statement<?> statement);

    long executeAndMapFirstColumnToLong(Statement<?> statement);

    long extractCount(Row row);

    Row executeAndExtractFirstRow(Statement<?> statement);

    <EntityT> EntityT executeAndMapToSingleEntity(Statement<?> statement, Function<Row, EntityT> mapper);

    <EntityT> Optional<EntityT> executeAndMapToOptionalEntity(Statement<?> statement, Function<Row, EntityT> mapper);

    <EntityT> List<EntityT> executeAndMapToListEntity(Statement<?> statement, RowMapperResultSetExtractor<EntityT> resultSetExtractor);

    <EntityT> PagingIterable<EntityT> executeAndMapToEntityIterable(Statement<?> statement, Function<Row, EntityT> mapper);

    <EntityT> Stream<EntityT> executeAndMapToEntityStream(Statement<?> statement, Function<Row, EntityT> mapper);

    CompletableFuture<AsyncResultSet> executeAsync(Statement<?> statement);

    CompletableFuture<Void> executeAsyncAndMapToVoid(Statement<?> statement);

    CompletableFuture<Boolean> executeAsyncAndMapWasAppliedToBoolean(Statement<?> statement);

    CompletableFuture<Long> executeAsyncAndMapFirstColumnToLong(Statement<?> statement);

    CompletableFuture<Row> executeAsyncAndExtractFirstRow(Statement<?> statement);

    <EntityT> CompletableFuture<EntityT> executeAsyncAndMapToSingleEntity(Statement<?> statement, Function<Row, EntityT> mapper);

    <EntityT> CompletableFuture<Optional<EntityT>> executeAsyncAndMapToOptionalEntity(Statement<?> statement, Function<Row, EntityT> mapper);

    <EntityT> CompletableFuture<MappedAsyncPagingIterable<EntityT>> executeAsyncAndMapToEntityIterable(Statement<?> statement, Function<Row, EntityT> mapper);

    <EntityT> CompletableFuture<Stream<EntityT>> executeAsyncAndMapToEntityStream(Statement<?> statement, Function<Row, EntityT> mapper);

}
