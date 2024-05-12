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

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.TypeInformation;

import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Default {@link ScyllaColumnType} implementation.
 *
 * @author Giangbb
 */
class DefaultScyllaColumnType extends DefaultColumnType implements ScyllaColumnType {

	private final Lazy<DataType> dataType;

	DefaultScyllaColumnType(Class<?> type, DataType dataType, ColumnType... parameters) {
		this(TypeInformation.of(type), dataType, parameters);
	}

	DefaultScyllaColumnType(TypeInformation<?> typeInformation, Supplier<DataType> dataType,
							ColumnType... parameters) {
		super(typeInformation, parameters);

		this.dataType = Lazy.of(dataType);
	}

	DefaultScyllaColumnType(TypeInformation<?> typeInformation, DataType dataType, ColumnType... parameters) {
		super(typeInformation, parameters);

		this.dataType = Lazy.of(dataType);
	}

	public DataType getDataType() {
		return dataType.get();
	}

	@Override
	public String toString() {

		StringBuilder builder = new StringBuilder();

		if (isTupleType()) {
			builder.append("Tuple: ");
		}

		if (isUserDefinedType()) {
			builder.append("UDT: ").append(((UserDefinedType) getDataType()).getName());
		}

		builder.append(getType().getName()).append(" [").append(getDataType()).append("]");

		if (getParameters().isEmpty()) {
			return builder.toString();
		}

		builder.append("<").append(getParameters().stream().map(Object::toString).collect(Collectors.toList())).append(">");

		return builder.toString();
	}
}
