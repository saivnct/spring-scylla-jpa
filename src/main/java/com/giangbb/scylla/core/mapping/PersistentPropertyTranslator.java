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

import org.springframework.data.util.Predicates;
import org.springframework.lang.Nullable;

import java.util.function.Predicate;

/**
 * Utility to translate a {@link ScyllaPersistentProperty} into a corresponding property from a different
 * {@link ScyllaPersistentEntity} by looking it up by name.
 * <p>
 * Mainly used within the framework.
 *
 * @author Giangbb
 */
public class PersistentPropertyTranslator {

	/**
	 * Translate a {@link ScyllaPersistentProperty} into a corresponding property from a different
	 * {@link ScyllaPersistentEntity}.
	 *
	 * @param property must not be {@literal null}.
	 * @return the translated property. Can be the original {@code property}.
	 */
	public ScyllaPersistentProperty translate(ScyllaPersistentProperty property) {
		return property;
	}

	/**
	 * Create a new {@link PersistentPropertyTranslator}.
	 *
	 * @param targetEntity must not be {@literal null}.
	 * @return the property translator to use.
	 */
	public static PersistentPropertyTranslator create(@Nullable ScyllaPersistentEntity<?> targetEntity) {
		return create(targetEntity, Predicates.isTrue());
	}

	/**
	 * Create a new {@link PersistentPropertyTranslator} accepting a {@link Predicate filter predicate} whether the
	 * translation should happen at all.
	 *
	 * @param targetEntity must not be {@literal null}.
	 * @param translationFilter must not be {@literal null}.
	 * @return the property translator to use.
	 */
	public static PersistentPropertyTranslator create(@Nullable ScyllaPersistentEntity<?> targetEntity,
			Predicate<ScyllaPersistentProperty> translationFilter) {
		return targetEntity != null ? new EntityPropertyTranslator(targetEntity, translationFilter)
				: new PersistentPropertyTranslator();
	}

	private static class EntityPropertyTranslator extends PersistentPropertyTranslator {

		private final ScyllaPersistentEntity<?> targetEntity;
		private final Predicate<ScyllaPersistentProperty> translationFilter;

		EntityPropertyTranslator(ScyllaPersistentEntity<?> targetEntity,
                                 Predicate<ScyllaPersistentProperty> translationFilter) {
			this.targetEntity = targetEntity;
			this.translationFilter = translationFilter;
		}

		@Override
		public ScyllaPersistentProperty translate(ScyllaPersistentProperty property) {

			if (!translationFilter.test(property)) {
				return property;
			}

			ScyllaPersistentProperty targetProperty = targetEntity.getPersistentProperty(property.getName());
			return targetProperty != null ? targetProperty : property;
		}
	}

}
