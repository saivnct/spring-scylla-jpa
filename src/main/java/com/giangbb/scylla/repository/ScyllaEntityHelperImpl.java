package com.giangbb.scylla.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.mapper.MapperException;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.querybuilder.truncate.Truncate;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.internal.querybuilder.update.DefaultUpdate;
import com.giangbb.scylla.core.convert.ScyllaConverter;
import com.giangbb.scylla.core.mapping.BasicScyllaPersistentEntity;
import com.giangbb.scylla.core.mapping.ScyllaPersistentProperty;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Created by Giangbb on 10/04/2024
 */
public class ScyllaEntityHelperImpl<T> implements ScyllaEntityHelper<T> {

    private Class<T> tClass;
    private final CqlIdentifier keyspaceId;

    private final BasicScyllaPersistentEntity<T> persistentEntity;
    private final CqlIdentifier tableId;

    private final List<ScyllaPersistentProperty> pKeys;
    private final List<ScyllaPersistentProperty> cKeys;

    protected ScyllaEntityHelperImpl(Class<T> tClass, CqlSession cqlSession, ScyllaConverter scyllaConverter) {
        Assert.notNull(tClass, "T Class must not be null");
        Assert.notNull(cqlSession, "CqlSession must not be null");
        this.tClass = tClass;

        BasicScyllaPersistentEntity<T> persistentEntity = (BasicScyllaPersistentEntity<T>) scyllaConverter.getMappingContext().getRequiredPersistentEntity(tClass);
        Assert.notNull(persistentEntity, "Cannot get required PersistentEntity for " + tClass.getName());
        this.persistentEntity = persistentEntity;


        this.keyspaceId = cqlSession.getKeyspace().orElse(null);
        Assert.notNull(keyspaceId, "KeyspaceId must not be null");

        this.tableId = persistentEntity.getTableName();

        this.pKeys = new ArrayList<ScyllaPersistentProperty>();
        this.cKeys = new ArrayList<ScyllaPersistentProperty>();

        this.persistentEntity.forEach(property -> {
            if (property.isPartitionKeyColumn()) {
                this.pKeys.add(property);
            }else if (property.isClusterKeyColumn()) {
                this.cKeys.add(property);
            }
        });
        Assert.notEmpty(pKeys, "Partition Key must not be empty");
    }


    private void throwIfKeyspaceMissing() {
        if (this.getKeyspaceId() == null) {
            throw new MapperException("Missing keyspace");
        }
    }


    @Override
    public BasicScyllaPersistentEntity<T> getPersistentEntity() {
        return persistentEntity;
    }

    @Override
    public List<ScyllaPersistentProperty> getpKeys() {
        return pKeys;
    }

    @Override
    public List<ScyllaPersistentProperty> getcKeys() {
        return cKeys;
    }

    @Override
    public List<ScyllaPersistentProperty> getPrimaryKeys() {
        List<ScyllaPersistentProperty> primaryKeys = new ArrayList<>();
        primaryKeys.addAll(pKeys);
        primaryKeys.addAll(cKeys);

        return primaryKeys;
    }

    @Override
    public Class<T> getEntityClass() {
        return this.tClass;
    }

    @Override
    public CqlIdentifier getKeyspaceId() {
        return this.keyspaceId;
    }

    @Override
    public CqlIdentifier getTableId() {
        return this.tableId;
    }


    public Select selectCountStart() {
        throwIfKeyspaceMissing();

        SelectFrom selectFrom = (keyspaceId == null)
                ? QueryBuilder.selectFrom(tableId)
                : QueryBuilder.selectFrom(keyspaceId, tableId);

        Select select = selectFrom.countAll();

        return select;
    }

    public Select selectCountByPartitionKey() {
        Select select = selectCountStart();
        for (ScyllaPersistentProperty property : this.pKeys) {
            CqlIdentifier columnName = Objects.requireNonNull(property.getColumnName());
            select = select.whereColumn(columnName).isEqualTo(QueryBuilder.bindMarker(columnName));
        }
        return select;
    }



    @Override
    public Select selectStart() {
        throwIfKeyspaceMissing();

        SelectFrom selectFrom = (keyspaceId == null)
                ? QueryBuilder.selectFrom(tableId)
                : QueryBuilder.selectFrom(keyspaceId, tableId);

        Select select = null;

        for (ScyllaPersistentProperty property : this.persistentEntity) {
            CqlIdentifier columnName = Objects.requireNonNull(property.getColumnName());

            if (select == null) {
                select = selectFrom
                        .column(columnName);
            } else{
                select = select.column(columnName);
            }
        }

        return select;
    }

    @Override
    public Select selectByPrimaryKey() {
        Select select = selectStart();
        for (ScyllaPersistentProperty property : this.getPrimaryKeys()) {
            CqlIdentifier columnName = Objects.requireNonNull(property.getColumnName());
            select = select.whereColumn(columnName).isEqualTo(QueryBuilder.bindMarker(columnName));
        }

        return select;
    }



    @Override
    public Select selectByPartitionKey() {
        Select select = selectStart();
        for (ScyllaPersistentProperty property : this.pKeys) {
            CqlIdentifier columnName = Objects.requireNonNull(property.getColumnName());
            select = select.whereColumn(columnName).isEqualTo(QueryBuilder.bindMarker(columnName));
        }

        return select;
    }

    public DeleteSelection deleteStart() {
        throwIfKeyspaceMissing();

        return (keyspaceId == null)
                ? QueryBuilder.deleteFrom(tableId)
                : QueryBuilder.deleteFrom(keyspaceId, tableId);
    }

    @Override
    public Delete deleteByPrimaryKey() {
        DeleteSelection deleteSelection = deleteStart();

        Delete delete = null;

        for (ScyllaPersistentProperty property : this.getPrimaryKeys()) {
            CqlIdentifier columnName = Objects.requireNonNull(property.getColumnName());
            if (delete == null) {
                delete = deleteSelection
                        .whereColumn(columnName).isEqualTo(QueryBuilder.bindMarker(columnName));
            } else{
                delete = delete
                        .whereColumn(columnName).isEqualTo(QueryBuilder.bindMarker(columnName));
            }
        }
        return delete;
    }


    @Override
    public Truncate deleteAll() {
        throwIfKeyspaceMissing();

        return (keyspaceId == null)
                ? QueryBuilder.truncate(tableId)
                : QueryBuilder.truncate(keyspaceId, tableId);
    }


    @Override
    public RegularInsert insert() {
        throwIfKeyspaceMissing();
        InsertInto insertInto = (keyspaceId == null)
                ? QueryBuilder.insertInto(tableId)
                : QueryBuilder.insertInto(keyspaceId, tableId);

        RegularInsert insert = null;
        for (ScyllaPersistentProperty property : this.persistentEntity) {
            CqlIdentifier columnName = Objects.requireNonNull(property.getColumnName());

            if (insert == null) {
                insert = insertInto
                        .value(columnName, QueryBuilder.bindMarker(columnName));
            } else{
                insert = insert.value(columnName, QueryBuilder.bindMarker(columnName));
            }
        }

        return insert;
    }

    @Override
    public DefaultUpdate updateStart() {
        throwIfKeyspaceMissing();

        UpdateStart updateStart = (keyspaceId == null)
                ? QueryBuilder.update(tableId)
                : QueryBuilder.update(keyspaceId, tableId);

        DefaultUpdate update = null;
        for (ScyllaPersistentProperty property : this.persistentEntity) {
            if (property.isPartitionKeyColumn() || property.isClusterKeyColumn()) {
               continue;
            }
            CqlIdentifier columnName = Objects.requireNonNull(property.getColumnName());

            if (update == null) {
                update = (DefaultUpdate)updateStart
                        .setColumn(columnName, QueryBuilder.bindMarker(columnName));
            } else{
                update = (DefaultUpdate)update.setColumn(columnName, QueryBuilder.bindMarker(columnName));
            }
        }

        return update;
    }

    @Override
    public DefaultUpdate updateByPrimaryKey() {
        DefaultUpdate update = updateStart();

        for (ScyllaPersistentProperty property : this.getPrimaryKeys()) {
            CqlIdentifier columnName = Objects.requireNonNull(property.getColumnName());
            update = (DefaultUpdate)update
                    .where(Relation.column(columnName).isEqualTo(QueryBuilder.bindMarker(columnName)));
        }

        return update;
    }



}
