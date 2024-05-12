/*
 * Copyright 2016-2024 the original author or authors.
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

import org.springframework.data.util.TypeInformation;

/**
 * {@link org.springframework.data.mapping.PersistentEntity} for a mapped user-defined type (UDT). A mapped UDT consists
 * of a set of fields. Each field requires a data type that can be either a simple Scylla type or an UDT.
 *
 * @author Giangbb
 * @see UserDefinedType
 */
public class ScyllaUserTypePersistentEntity<T> extends BasicScyllaPersistentEntity<T> {

	/**
	 * Create a new {@link ScyllaUserTypePersistentEntity}.
	 *
	 * @param typeInformation must not be {@literal null}.
	 * @param verifier must not be {@literal null}.
	 */
	public ScyllaUserTypePersistentEntity(TypeInformation<T> typeInformation,
										  ScyllaPersistentEntityMetadataVerifier verifier) {

		super(typeInformation, verifier);
	}

	@Override
	public boolean isUserDefinedType() {
		return true;
	}
}
