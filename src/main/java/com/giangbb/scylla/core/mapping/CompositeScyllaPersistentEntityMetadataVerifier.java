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

import org.springframework.data.mapping.MappingException;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.Collection;

/**
 * Composite {@link ScyllaPersistentEntityMetadataVerifier} to verify persistent entities and primary key classes.
 *
 * @author Giangbb
 * @see BasicScyllaPersistentEntityMetadataVerifier
 */
public class CompositeScyllaPersistentEntityMetadataVerifier implements ScyllaPersistentEntityMetadataVerifier {

	private Collection<ScyllaPersistentEntityMetadataVerifier> verifiers;

	/**
	 * Create a new {@link CompositeScyllaPersistentEntityMetadataVerifier} using default entity and primary key
	 * verifiers.
	 *
	 * @see BasicScyllaPersistentEntityMetadataVerifier
	 */
	public CompositeScyllaPersistentEntityMetadataVerifier() {
		this(Arrays.asList(new BasicScyllaPersistentEntityMetadataVerifier()));
	}

	/**
	 * Create a new {@link CompositeScyllaPersistentEntityMetadataVerifier} for the given {@code verifiers}
	 *
	 * @param verifiers must not be {@literal null}.
	 */
	private CompositeScyllaPersistentEntityMetadataVerifier(
			Collection<ScyllaPersistentEntityMetadataVerifier> verifiers) {

		Assert.notNull(verifiers, "Verifiers must not be null");

		this.verifiers = verifiers;
	}

	@Override
	public void verify(ScyllaPersistentEntity<?> entity) throws MappingException {
		verifiers.forEach(verifier -> verifier.verify(entity));
	}
}
