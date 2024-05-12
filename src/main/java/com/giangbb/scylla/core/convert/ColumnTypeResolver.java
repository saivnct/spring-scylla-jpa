/*
 * Copyright 2020-2024 the original author or authors.
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

import com.giangbb.scylla.core.mapping.ScyllaPersistentProperty;
import com.giangbb.scylla.core.mapping.ScyllaType;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import com.giangbb.scylla.core.mapping.ScyllaSimpleTypeHolder;

/**
 * Resolves {@link ColumnType} for properties, {@link TypeInformation}, and {@code values}.
 *
 * @author Giangbb
 */
public interface ColumnTypeResolver {

	/**
	 * Resolve a {@link ScyllaColumnType} from a {@link ScyllaPersistentProperty}. Considers
	 * {@link ScyllaType}-annotated properties.
	 *
	 * @param property must not be {@literal null}.
	 * @return
	 * @see ScyllaType
	 * @see ScyllaPersistentProperty
	 * @throws org.springframework.dao.InvalidDataAccessApiUsageException
	 */
	default ScyllaColumnType resolve(ScyllaPersistentProperty property) {

		Assert.notNull(property, "Property must not be null");

		if (property.isAnnotationPresent(ScyllaType.class)) {
			return resolve(property.getRequiredAnnotation(ScyllaType.class));
		}

		return resolve(property.getTypeInformation());
	}

	/**
	 * Resolve a {@link ScyllaColumnType} from {@link TypeInformation}. Considers potentially registered custom
	 * converters and simple type rules.
	 *
	 * @param typeInformation must not be {@literal null}.
	 * @return
	 * @see ScyllaSimpleTypeHolder
	 * @see ScyllaCustomConversions
	 * @throws org.springframework.dao.InvalidDataAccessApiUsageException
	 */
	ScyllaColumnType resolve(TypeInformation<?> typeInformation);

	/**
	 * Resolve a {@link ScyllaColumnType} from a {@link ScyllaType} annotation.
	 *
	 * @param annotation must not be {@literal null}.
	 * @return
	 * @see ScyllaType
	 * @see ScyllaPersistentProperty
	 * @throws org.springframework.dao.InvalidDataAccessApiUsageException
	 */
	ScyllaColumnType resolve(ScyllaType annotation);

	/**
	 * Resolve a {@link ColumnType} from a {@code value}. Considers potentially registered custom converters and simple
	 * type rules.
	 *
	 * @param value
	 * @return
	 * @see ScyllaSimpleTypeHolder
	 * @see ScyllaCustomConversions
	 */
	ColumnType resolve(@Nullable Object value);
}
