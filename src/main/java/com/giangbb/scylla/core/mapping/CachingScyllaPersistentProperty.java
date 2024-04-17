/*
 * Copyright 2021-2024 the original author or authors.
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

import com.giangbb.scylla.core.cql.Ordering;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.lang.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedType;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link BasicScyllaPersistentProperty} that pre-computes primary key and embedded flags.
 *
 * @author Giangbb
 */
public class CachingScyllaPersistentProperty extends BasicScyllaPersistentProperty {

	private final @Nullable Ordering clusteringKeyOrdering;

	private final boolean isClusterKeyColumn;
	private final boolean isPartitionKeyColumn;
	private final boolean isPrimaryKeyColumn;
	private final boolean isStaticColumn;
	private final Map<Class<? extends Annotation>, Optional<AnnotatedType>> findAnnotatedTypeCache = new ConcurrentHashMap<>();

	public CachingScyllaPersistentProperty(Property property, ScyllaPersistentEntity<?> owner,
										   SimpleTypeHolder simpleTypeHolder) {
		super(property, owner, simpleTypeHolder);

		clusteringKeyOrdering = super.getClusteringKeyOrdering();
		isClusterKeyColumn = super.isClusterKeyColumn();
		isPartitionKeyColumn = super.isPartitionKeyColumn();
		isPrimaryKeyColumn = super.isPrimaryKeyColumn();
		isStaticColumn = super.isStaticColumn();
	}


	@Nullable
	@Override
	public Ordering getClusteringKeyOrdering() {
		return clusteringKeyOrdering;
	}

	@Override
	public boolean isClusterKeyColumn() {
		return isClusterKeyColumn;
	}

	@Override
	public boolean isPartitionKeyColumn() {
		return isPartitionKeyColumn;
	}

	@Override
	public boolean isPrimaryKeyColumn() {
		return isPrimaryKeyColumn;
	}

	@Override
	public boolean isStaticColumn() {
		return isStaticColumn;
	}

	@Override
	public AnnotatedType findAnnotatedType(Class<? extends Annotation> annotationType) {
		return findAnnotatedTypeCache
				.computeIfAbsent(annotationType, key -> Optional.ofNullable(super.findAnnotatedType(key))).orElse(null);
	}
}
