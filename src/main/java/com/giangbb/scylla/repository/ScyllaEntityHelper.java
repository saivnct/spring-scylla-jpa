package com.giangbb.scylla.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.truncate.Truncate;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.giangbb.scylla.core.mapping.BasicScyllaPersistentEntity;
import com.giangbb.scylla.core.mapping.ScyllaPersistentProperty;

import java.util.List;

/**
 * Created by Giangbb on 11/04/2024
 */
public interface ScyllaEntityHelper<T> {

    BasicScyllaPersistentEntity<T> getPersistentEntity();

    List<ScyllaPersistentProperty> getpKeys();

    List<ScyllaPersistentProperty> getcKeys();

    List<ScyllaPersistentProperty> getPrimaryKeys();

    Class<T> getEntityClass();

    CqlIdentifier getKeyspaceId();

    CqlIdentifier getTableId();

    RegularInsert insert();

    Update updateStart();

    Update updateByPrimaryKey();

    Select selectByPrimaryKey();

    Select selectByPartitionKey();

    Select selectStart();

    DeleteSelection deleteStart();

    Delete deleteByPrimaryKey();

    Truncate deleteAll();








}
