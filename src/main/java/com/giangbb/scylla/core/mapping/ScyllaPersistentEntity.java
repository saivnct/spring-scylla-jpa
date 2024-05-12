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
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import org.springframework.data.mapping.Parameter;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * Scylla specific {@link PersistentEntity} abstraction.
 *
 * @author Giangbb
 */
public interface ScyllaPersistentEntity<T> extends PersistentEntity<T, ScyllaPersistentProperty> {

	/**
	 * Retrieve a {@link ScyllaPersistentProperty} from a {@link Parameter persistence creator (constructor/factory
	 * method) parameter}. Parameters are either derived by name or synthesized if their name does not map to a existing
	 * property.
	 *
	 * @param parameter the parameter to create a property from. Parameters without a name return no ({@literal null})
	 *          parameter.
	 * @return the property, synthetic property or {@literal null}, if the parameter is unnamed.
	 */
	@Nullable
	ScyllaPersistentProperty getProperty(Parameter<?, ScyllaPersistentProperty> parameter);

	/**
	 * Returns the table name to which the entity shall be persisted.
	 */
	CqlIdentifier getTableName();

	/**
	 * Return whether the property has an explicitly configured naming Strategy. Eg. via {@link NamingStrategy#convention()},
	 *
	 * @return {@literal true} if a configured naming Strategy is present and non empty.
	 */
	default boolean hasExplicitNamingStrategy(){
		NamingStrategy namingStrategy = findAnnotation(NamingStrategy.class);
		return namingStrategy != null && !ObjectUtils.isEmpty(namingStrategy.convention());
	}

	/**
	 * The name of the element ordinal to which the property is persisted when the owning type is a mapped tuple.
	 */
	@Nullable
	default NamingConvention getExplicitNamingStrategy(){
		if (hasExplicitNamingStrategy()){
			NamingStrategy namingStrategy = findAnnotation(NamingStrategy.class);
			return namingStrategy.convention()[0];
		}

		return null;
	};

	/**
	 * Sets the CQL table name.
	 *
	 * @param tableName must not be {@literal null}.
	 */
	void setTableName(CqlIdentifier tableName);

	/**
	 * @return {@literal true} if the type is a mapped tuple type.
	 * @see Tuple
	 */
	boolean isTupleType();

	/**
	 * @return {@literal true} if the type is a mapped user defined type.
	 * @see UserDefinedType
	 */
	boolean isUserDefinedType();

}
