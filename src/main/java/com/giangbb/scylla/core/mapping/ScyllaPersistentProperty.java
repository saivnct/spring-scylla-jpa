/*
 * Copyright 2013-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.giangbb.scylla.core.mapping;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import org.springframework.context.ApplicationContextAware;
import com.giangbb.scylla.core.cql.Ordering;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedType;

/**
 * Scylla specific {@link PersistentProperty} extension.
 *
 * @author Giangbb
 */
public interface ScyllaPersistentProperty
		extends PersistentProperty<ScyllaPersistentProperty>, ApplicationContextAware {

	/**
	 * If this property is mapped with a single column, set the column name to the given {@link CqlIdentifier}. If this
	 * property is not mapped by a single column, throws {@link IllegalStateException}. If the given column name is null,
	 * {@link IllegalArgumentException} is thrown.
	 *
	 * @param columnName must not be {@literal null}.
	 */
	void setColumnName(CqlIdentifier columnName);

	/**
	 * The name of the single column to which the property is persisted.
	 */
	@Nullable
	CqlIdentifier getColumnName();

	/**
	 * The name of the single column to which the property is persisted.
	 *
	 * @throws IllegalStateException if the required column name is not available.
	 */
	default CqlIdentifier getRequiredColumnName() {

		CqlIdentifier columnName = getColumnName();

		Assert.state(columnName != null, () -> String
				.format("No column name available for this persistent property [%1$s.%2$s]", getOwner().getName(), getName()));

		return columnName;
	}


	/**
	 * Return whether the property has an explicitly configured column name. Eg. via {@link CqlName#value()},
	 *
	 * @return {@literal true} if a configured column name is present and non empty.
	 */
	boolean hasExplicitColumnName();

	/**
	 * The name of the element ordinal to which the property is persisted when the owning type is a mapped tuple.
	 */
	@Nullable
	Integer getOrdinal();

	/**
	 * The required element ordinal to which the property is persisted when the owning type is a mapped tuple.
	 *
	 * @throws IllegalStateException if the required ordinal is not available.
	 */
	default int getRequiredOrdinal() {

		Integer ordinal = getOrdinal();

		Assert.state(ordinal != null,
			() -> String.format("No ordinal available for this persistent property [%1$s.%2$s]",
				getOwner().getName(), getName()));

		return ordinal;
	}

	/**
	 * Determines whether this {@link ScyllaPersistentProperty} is persisted (mapped) to an element ordinal when the
	 * owning type is a mapped tuple.
	 *
	 * @return a boolean value indicating whether this {@link ScyllaPersistentProperty} is persisted (mapped) to an
	 *         element ordinal when the owning type is a mapped tuple.
	 * @see #getOrdinal()
	 */
	default boolean hasOrdinal() {
		return getOrdinal() != null;
	}

	/**
	 * The ordering (ascending or descending) for the column. Valid only for primary key columns; returns null for
	 * non-primary key columns.
	 */
	@Nullable
	Ordering getClusteringKeyOrdering();

	/**
	 * Whether the property is a cluster key column.
	 */
	boolean isClusterKeyColumn();

	/**
	 * Returns whether the property is a {@link java.util.Map}.
	 *
	 * @return a boolean indicating whether this property type is a {@link java.util.Map}.
	 */
	boolean isMapLike();

	/**
	 * Whether the property is a partition key column.
	 */
	boolean isPartitionKeyColumn();

	/**
	 * Whether the property is a partition key column or a cluster key column
	 *
	 * @see #isPartitionKeyColumn()
	 * @see #isClusterKeyColumn()
	 */
	boolean isPrimaryKeyColumn();

	/**
	 * Whether the property maps to a static column.
	 *
	 */
	boolean isStaticColumn();


	/**
	 * Find an {@link AnnotatedType} by {@code annotationType} derived from the property type. Annotated type is looked up
	 * by introspecting property field/accessors. Collection/Map-like types are introspected for type annotations within
	 * type arguments.
	 *
	 * @param annotationType must not be {@literal null}.
	 * @return the annotated type or {@literal null}.
	 */
	@Nullable
	AnnotatedType findAnnotatedType(Class<? extends Annotation> annotationType);

}
