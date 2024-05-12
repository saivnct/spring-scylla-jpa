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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.util.function.Supplier;

/**
 * Default implementation of {@link UserTypeResolver} that resolves a {@link UserDefinedType} by its name from
 * {@link Metadata}.
 *
 * @author Giangbb
 */
public class SimpleUserTypeResolver implements UserTypeResolver {

	private final Supplier<Metadata> metadataSupplier;

	private final CqlIdentifier keyspaceName;

	/**
	 * Create a new {@link SimpleUserTypeResolver}.
	 *
	 * @param session must not be {@literal null}.
	 */
	public SimpleUserTypeResolver(CqlSession session) {

		Assert.notNull(session, "Session must not be null");

		this.metadataSupplier = session::getMetadata;
		this.keyspaceName = session.getKeyspace().orElse(CqlIdentifier.fromCql("system"));
	}

	/**
	 * Create a new {@link SimpleUserTypeResolver}.
	 *
	 * @param session must not be {@literal null}.
	 * @param keyspaceName must not be {@literal null}.
	 */
	public SimpleUserTypeResolver(CqlSession session, CqlIdentifier keyspaceName) {

		Assert.notNull(session, "Session must not be null");
		Assert.notNull(keyspaceName, "Keyspace must not be null");

		this.metadataSupplier = session::getMetadata;
		this.keyspaceName = keyspaceName;
	}

	/**
	 * Create a new {@link SimpleUserTypeResolver}.
	 *
	 * @param metadataSupplier must not be {@literal null}.
	 * @param keyspaceName must not be {@literal null}.
	 */
	public SimpleUserTypeResolver(Supplier<Metadata> metadataSupplier, CqlIdentifier keyspaceName) {

		Assert.notNull(metadataSupplier, "Metadata supplier must not be null");
		Assert.notNull(keyspaceName, "Keyspace must not be null");

		this.metadataSupplier = metadataSupplier;
		this.keyspaceName = keyspaceName;
	}

	@Nullable
	@Override
	public UserDefinedType resolveType(CqlIdentifier typeName) {
		return metadataSupplier.get().getKeyspace(keyspaceName) //
				.flatMap(it -> it.getUserDefinedType(typeName)) //
				.orElse(null);
	}
}
