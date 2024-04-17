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
package com.giangbb.scylla.core.convert;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.giangbb.scylla.core.mapping.ScyllaMappingContext;
import com.giangbb.scylla.core.mapping.ScyllaPersistentEntity;
import com.giangbb.scylla.core.mapping.ScyllaPersistentProperty;
import com.giangbb.scylla.core.mapping.MapId;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.EntityConverter;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Central Scylla specific converter interface from Object to Row.
 *
 * @author Giangbb
 */
public interface ScyllaConverter
		extends EntityConverter<ScyllaPersistentEntity<?>, ScyllaPersistentProperty, Object, Object> {

	/**
	 * Returns the {@link ProjectionFactory} for this converter.
	 *
	 * @return will never be {@literal null}.
	 */
	ProjectionFactory getProjectionFactory();

	/**
	 * Returns the {@link CustomConversions} for this converter.
	 *
	 * @return will never be {@literal null}.
	 */
	CustomConversions getCustomConversions();

	/**
	 * Returns the {@link CodecRegistry} registered in the {@link ScyllaConverter}.
	 *
	 * @return the {@link CodecRegistry}.
	 */
	CodecRegistry getCodecRegistry();

	@Override
	ScyllaMappingContext getMappingContext();

	/**
	 * Returns the {@link ColumnTypeResolver} to resolve {@link ColumnType} for properties, {@link TypeInformation}, and
	 * {@code values}.
	 *
	 * @return the {@link ColumnTypeResolver}
	 */
	ColumnTypeResolver getColumnTypeResolver();

	/**
	 * Apply a projection to {@link Row} and return the projection return type {@code R}.
	 * {@link EntityProjection#isProjection() Non-projecting} descriptors fall back to {@link #read(Class, Object) regular
	 * object materialization}.
	 *
	 * @param descriptor the projection descriptor, must not be {@literal null}.
	 * @param row must not be {@literal null}.
	 * @param <R>
	 * @return a new instance of the projection return type {@code R}.
	 */
	<R> R project(EntityProjection<R, ?> descriptor, Row row);

	/**
	 * Returns the Id for an entity. It can return:
	 *
	 * @param object must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the id value or {@literal null}, if the id is not set.
	 */
	@Nullable
	Object getId(Object object, ScyllaPersistentEntity<?> entity);

	/**
	 * Converts the given object into a value Scylla will be able to store natively in a column.
	 *
	 * @param value {@link Object} to convert; must not be {@literal null}.
	 * @return the result of the conversion.
	 */
	Object convertToColumnType(Object value);

	/**
	 * Converts the given object into a value Scylla will be able to store natively in a column.
	 *
	 * @param value {@link Object} to convert; must not be {@literal null}.
	 * @param typeInformation {@link TypeInformation} used to describe the object type; must not be {@literal null}.
	 * @return the result of the conversion.
	 */
	default Object convertToColumnType(Object value, TypeInformation<?> typeInformation) {

		Assert.notNull(value, "Value must not be null");
		Assert.notNull(typeInformation, "TypeInformation must not be null");

		return convertToColumnType(value, getColumnTypeResolver().resolve(typeInformation));
	}

	/**
	 * Converts the given object into a value Scylla will be able to store natively in a column.
	 *
	 * @param value {@link Object} to convert; must not be {@literal null}.
	 * @param typeDescriptor {@link ColumnType} used to describe the object type; must not be {@literal null}.
	 * @return the result of the conversion.
	 */
	Object convertToColumnType(Object value, ColumnType typeDescriptor);

	/**
	 * Converts and writes a {@code source} object into a {@code sink} using the given {@link ScyllaPersistentEntity}.
	 *
	 * @param source the source, must not be {@literal null}.
	 * @param sink must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 */
	void write(Object source, Object sink, ScyllaPersistentEntity<?> entity);

}
