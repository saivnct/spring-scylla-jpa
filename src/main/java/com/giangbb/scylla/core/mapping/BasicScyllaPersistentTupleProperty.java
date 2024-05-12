/*
 * Copyright 2018-2024 the original author or authors.
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
package com.giangbb.scylla.core.mapping;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.giangbb.scylla.core.cql.Ordering;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Scylla Tuple specific {@link ScyllaPersistentProperty} implementation.
 *
 * @author Giangbb
 * @see Element
 */
public class BasicScyllaPersistentTupleProperty extends BasicScyllaPersistentProperty {

	private final @Nullable Integer ordinal;

	/**
	 * Create a new {@link BasicScyllaPersistentTupleProperty}.
	 *
	 * @param property the actual {@link Property} in the domain entity corresponding to this persistent entity.
	 * @param owner the containing object or {@link ScyllaPersistentEntity} of this persistent property.
	 * @param simpleTypeHolder mapping of Java [simple|wrapper] types to Scylla data types.
	 */
	public BasicScyllaPersistentTupleProperty(Property property, ScyllaPersistentEntity<?> owner,
											  SimpleTypeHolder simpleTypeHolder) {

		super(property, owner, simpleTypeHolder);

		this.ordinal = findOrdinal();
	}

	@Nullable
	private Integer findOrdinal() {

		if (isTransient()) {
			return null;
		}

		int ordinal;

		try {
			ordinal = getRequiredAnnotation(Element.class).value();
		} catch (IllegalStateException cause) {
			throw new MappingException(
					String.format("Missing @Element annotation in mapped tuple type for property [%s] in entity [%s]", getName(),
							getOwner().getName()),
					cause);
		}

		Assert.isTrue(ordinal >= 0,
				() -> String.format("Element ordinal must be greater or equal to zero for property [%s] in entity [%s]",
						getName(),
						getOwner().getName()));

		return ordinal;
	}

	@Override
	public CqlIdentifier getColumnName() {
		return null;
	}

	@Nullable
	@Override
	public Integer getOrdinal() {
		return this.ordinal;
	}

	@Override
	@Nullable
	public Ordering getClusteringKeyOrdering() {
		return null;
	}

	@Override
	public boolean isClusterKeyColumn() {
		return false;
	}

	@Override
	public boolean isPartitionKeyColumn() {
		return false;
	}

	@Override
	public boolean isPrimaryKeyColumn() {
		return false;
	}

	@Override
	public boolean isStaticColumn() {
		return false;
	}

	@Override
	public void setColumnName(CqlIdentifier columnName) {
		throw new UnsupportedOperationException("Cannot set a column name on a property representing a tuple element");
	}
}
