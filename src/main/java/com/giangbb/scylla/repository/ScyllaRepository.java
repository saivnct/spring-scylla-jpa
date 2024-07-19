package com.giangbb.scylla.repository;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
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

    void saveAll(List<T> tList);
    void saveAll(List<T> tList, ConsistencyLevel consistencyLevel);

    CompletionStage<Void> saveAllAsync(List<T> tList);
    CompletionStage<Void> saveAllAsync(List<T> tList, ConsistencyLevel consistencyLevel);



    void save(T t);
    void save(T t, ConsistencyLevel consistencyLevel);

    CompletionStage<Void> saveAsync(T t);
    CompletionStage<Void> saveAsync(T t, ConsistencyLevel consistencyLevel);

    void saveWithTtl(T t, int ttl);
    void saveWithTtl(T t, int ttl, ConsistencyLevel consistencyLevel);

    CompletionStage<Void> saveWithTtlAsync(T t, int ttl);
    CompletionStage<Void> saveWithTtlAsync(T t, int ttl, ConsistencyLevel consistencyLevel);

    boolean saveIfExists(T t);
    boolean saveIfExists(T t, ConsistencyLevel consistencyLevel);

    CompletionStage<Boolean> saveIfExistsAsync(T t);
    CompletionStage<Boolean> saveIfExistsAsync(T t, ConsistencyLevel consistencyLevel);

    List<T> findAll();

    PagingIterable<T> findAllPagingIterable();

    CompletionStage<MappedAsyncPagingIterable<T>> findAllAsync();

    void delete(T t);
    void delete(T t, ConsistencyLevel consistencyLevel);

    CompletionStage<Void> deleteAsync(T t);
    CompletionStage<Void> deleteAsync(T t, ConsistencyLevel consistencyLevel);

    void deleteAll();

    CompletionStage<Void> deleteAllAsync();

    public long countAll();

    CompletionStage<Long> countAllAsync();


    public long countByPartitionKey(Map<CqlIdentifier, Object> pKeys);

    CompletionStage<Long> countByPartitionKeyAsync(Map<CqlIdentifier, Object> pKeys);

    public long countByPartitionKey(T t);

    CompletionStage<Long> countByPartitionKeyAsync(T t);


}
