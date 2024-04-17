package com.giangbb.scylla.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * Created by giangbb on 09/04/2024
 */
public interface ScyllaRepository<T> {

    T findByPrimaryKey(Map<CqlIdentifier, Object> primaryKey);
    CompletionStage<T> findByPrimaryKeyAsync(Map<CqlIdentifier, Object> primaryKey);
    T findByPrimaryKey(T t);
    CompletionStage<T> findByPrimaryKeyAsync(T t);


    List<T> findByPartitionKey(Map<CqlIdentifier, Object> pKeys);
    PagingIterable<T> findByPartitionKeyPagingIterable(Map<CqlIdentifier, Object> pKeys);
    CompletionStage<MappedAsyncPagingIterable<T>> findByPartitionKeyAsync(Map<CqlIdentifier, Object> pKeys);

    List<T> findByPartitionKey(T t);
    PagingIterable<T> findByPartitionKeyPagingIterable(T t);
    CompletionStage<MappedAsyncPagingIterable<T>> findByPartitionKeyAsync(T t);

    void save(T t);

    CompletionStage<Void> saveAsync(T t);

    void saveWithTtl(T t, int ttl);

    CompletionStage<Void> saveWithTtlAsync(T t, int ttl);

    boolean saveIfExists(T t);

    CompletionStage<Boolean> saveIfExistsAsync(T t);

    List<T> findAll();

    PagingIterable<T> findAllPagingIterable();

    CompletionStage<MappedAsyncPagingIterable<T>> findAllAsync();

    void delete(T t);

    CompletionStage<Void> deleteAsync(T t);

    void deleteAll();

    CompletionStage<Void> deleteAllAsync();

    public long countAll();

    CompletionStage<Long> countAllAsync();


    public long countByPartitionKey(Map<CqlIdentifier, Object> pKeys);

    CompletionStage<Long> countByPartitionKeyAsync(Map<CqlIdentifier, Object> pKeys);

    public long countByPartitionKey(T t);

    CompletionStage<Long> countByPartitionKeyAsync(T t);


}
