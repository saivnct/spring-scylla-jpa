/*
 * Copyright 2023-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.giangbb.scylla.core.convert;

import com.datastax.oss.driver.api.core.cql.Row;
import com.giangbb.scylla.core.mapping.ScyllaPersistentProperty;
import org.springframework.data.convert.ValueConversionContext;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * {@link ValueConversionContext} that allows to delegate read/write to an underlying {@link ScyllaConverter}.
 *
 * @author Giangbb
 */
public class ScyllaConversionContext implements ValueConversionContext<ScyllaPersistentProperty> {

	private final PropertyValueProvider<ScyllaPersistentProperty> accessor;
	private final ScyllaPersistentProperty persistentProperty;
	private final ScyllaConverter scyllaConverter;

	@Nullable private final SpELContext spELContext;

	public ScyllaConversionContext(PropertyValueProvider<ScyllaPersistentProperty> accessor,
								   ScyllaPersistentProperty persistentProperty, ScyllaConverter ScyllaConverter) {
		this(accessor, persistentProperty, ScyllaConverter, null);
	}

	public ScyllaConversionContext(PropertyValueProvider<ScyllaPersistentProperty> accessor,
								   ScyllaPersistentProperty persistentProperty, ScyllaConverter ScyllaConverter,
								   @Nullable SpELContext spELContext) {

		this.accessor = accessor;
		this.persistentProperty = persistentProperty;
		this.scyllaConverter = ScyllaConverter;
		this.spELContext = spELContext;
	}

	@Override
	public ScyllaPersistentProperty getProperty() {
		return persistentProperty;
	}

	@Nullable
	public Object getValue(String propertyPath) {
		return accessor.getPropertyValue(persistentProperty.getOwner().getRequiredPersistentProperty(propertyPath));
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T write(@Nullable Object value, TypeInformation<T> target) {
		return (T) scyllaConverter.convertToColumnType(value, target);
	}

	@Override
	public <T> T read(@Nullable Object value, TypeInformation<T> target) {
		return value instanceof Row row ? scyllaConverter.read(target.getType(), row)
				: ValueConversionContext.super.read(value, target);
	}

	@Nullable
	public SpELContext getSpELContext() {
		return spELContext;
	}
}
