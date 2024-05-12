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
package com.giangbb.scylla.core.cql.generator;

import com.giangbb.scylla.core.cql.keyspace.UserTypeNameSpecification;
import org.springframework.util.Assert;

/**
 * Abstract class to support User type CQL generation.
 *
 * @author Giangbb
 * @param <T> specification type
 * @see UserTypeNameSpecification
 */
public abstract class UserTypeNameCqlGenerator<T extends UserTypeNameSpecification> {

	private final UserTypeNameSpecification specification;

	/**
	 * Create a new {@link UserTypeNameCqlGenerator}.
	 *
	 * @param specification must not be {@literal null}.
	 */
	public UserTypeNameCqlGenerator(UserTypeNameSpecification specification) {

		Assert.notNull(specification, "UserTypeNameSpecification must not be null");

		this.specification = specification;
	}

	@SuppressWarnings("unchecked")
	public T getSpecification() {
		return (T) specification;
	}

	/**
	 * Convenient synonymous method of {@link #getSpecification()}.
	 */
	protected T spec() {
		return getSpecification();
	}

	public String toCql() {
		return toCql(new StringBuilder()).toString();
	}

	public abstract StringBuilder toCql(StringBuilder cql);
}
