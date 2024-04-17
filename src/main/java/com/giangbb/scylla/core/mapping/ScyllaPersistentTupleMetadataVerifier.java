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

import org.springframework.data.mapping.MappingException;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Verifier for {@link ScyllaPersistentEntity tuple entities}. Validates for a proper annotated domain classes to
 * ensure the meta-model is suitable for {@link com.datastax.driver.core.TupleValue} mapping.
 *
 * @author Giangbb
 */
enum ScyllaPersistentTupleMetadataVerifier implements ScyllaPersistentEntityMetadataVerifier {

	INSTANCE;

	public void verify(ScyllaPersistentEntity<?> entity) throws MappingException {

		if (entity.getType().isInterface() || !entity.isAnnotationPresent(Tuple.class)) {
			return;
		}

		Set<Integer> ordinals = new TreeSet<>();

		for (ScyllaPersistentProperty tupleProperty : entity) {

			if (tupleProperty.isTransient()) {
				continue;
			}

			if (!ordinals.add(tupleProperty.getOrdinal())) {
				throw new MappingException(
						String.format("Duplicate ordinal [%d] in entity [%s]", tupleProperty.getOrdinal(), entity.getName()));
			}
		}

		if (ordinals.isEmpty()) {
			throw new MappingException(String.format(
					"Mapped tuple contains no persistent elements annotated with @Element in entity [%s]", entity.getName()));
		}

		List<Integer> missingMappings = IntStream.range(0, ordinals.size()).boxed().collect(Collectors.toList());

		missingMappings.removeAll(ordinals);

		if (!missingMappings.isEmpty()) {
			throw new MappingException(String.format("Mapped tuple has no ordinal mapping in entity [%s] for ordinal(s): %s",
					entity.getName(), StringUtils.collectionToDelimitedString(missingMappings, ", ")));
		}
	}
}
