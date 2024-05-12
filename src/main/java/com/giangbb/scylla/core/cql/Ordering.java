/*
 * Copyright 2013-2024 the original author or authors.
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
package com.giangbb.scylla.core.cql;

/**
 * Enum for Scylla primary key column ordering.
 *
 * @author Giangbb
 */
public enum Ordering {

	/**
	 * Ascending Scylla column ordering.
	 */
	ASCENDING("ASC"),

	/**
	 * Descending Scylla column ordering.
	 */
	DESCENDING("DESC");

	private String cql;

	Ordering(String cql) {
		this.cql = cql;
	}

	/**
	 * Returns the CQL keyword of this {@link Ordering}.
	 */
	public String cql() {
		return cql;
	}
}
