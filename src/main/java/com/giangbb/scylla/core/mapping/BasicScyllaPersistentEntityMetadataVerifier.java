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

import org.springframework.data.mapping.MappingException;

import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation for Scylla Persistent Entity Verification. Ensures that annotated
 * {@link ScyllaPersistentEntity entities} will map properly to a Scylla Table.
 *
 * @author Giangbb
 * @see Table
 */
public class BasicScyllaPersistentEntityMetadataVerifier implements ScyllaPersistentEntityMetadataVerifier {

	@Override
	public void verify(ScyllaPersistentEntity<?> entity) throws MappingException {

		if (entity.getType().isInterface() || !entity.isAnnotationPresent(Table.class)) {
			return;
		}

		List<MappingException> exceptions = new ArrayList<>();

		List<ScyllaPersistentProperty> partitionKeyColumns = new ArrayList<>();
		List<ScyllaPersistentProperty> primaryKeyColumns = new ArrayList<>();

		// @Indexed not allowed on type level
		if (entity.isAnnotationPresent(Indexed.class)) {
			exceptions.add(new MappingException("@Indexed cannot be used on entity classes"));
		}

		// Parse entity properties
		entity.forEach(property -> {
			if (property.isClusterKeyColumn()) {
				primaryKeyColumns.add(property);
			} else if (property.isPartitionKeyColumn()) {
				partitionKeyColumns.add(property);
				primaryKeyColumns.add(property);
			}
		});

		/*
		 * Perform rules verification on Table/Persistent
		 */

		if (primaryKeyColumns.isEmpty()) {
			exceptions
					.add(new MappingException(String.format("@%s types must have Primary Key", Table.class.getSimpleName())));

			fail(entity, exceptions);
		}

		// We have no Partition Keys & only Clustering Key Column(s); ensure at least one is of type PARTITIONED
		if (partitionKeyColumns.isEmpty()) {
			exceptions
					.add(new MappingException(String.format("@%s types must have Partition Key", Table.class.getSimpleName())));
		}

		// Determine whether or not to throw Exception based on errors found
		if (!exceptions.isEmpty()) {
			fail(entity, exceptions);
		}
	}

	private static void fail(ScyllaPersistentEntity<?> entity, List<MappingException> exceptions) {
		throw new VerifierMappingExceptions(entity, exceptions);
	}
}
