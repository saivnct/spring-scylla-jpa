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
package com.giangbb.scylla.core.convert;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.giangbb.scylla.core.mapping.ScyllaPersistentProperty;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link ScyllaValueProvider} to read property values from a {@link Row}.
 *
 * @author Giangbb
 */
public class RowValueProvider implements ScyllaValueProvider {

	private final RowReader reader;

	private final SpELExpressionEvaluator evaluator;

	/**
	 * Create a new {@link RowValueProvider} with the given {@link Row}, {@link CodecRegistry} and
	 * {@link SpELExpressionEvaluator}.
	 *
	 * @param source must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 */
	public RowValueProvider(Row source, SpELExpressionEvaluator evaluator) {

		Assert.notNull(source, "Source Row must not be null");
		Assert.notNull(evaluator, "SpELExpressionEvaluator must not be null");

		this.reader = new RowReader(source);
		this.evaluator = evaluator;
	}

	@Nullable
	@Override
	@SuppressWarnings("unchecked")
	public <T> T getPropertyValue(ScyllaPersistentProperty property) {

		String spelExpression = property.getSpelExpression();

		return spelExpression != null ? this.evaluator.evaluate(spelExpression)
				: (T) this.reader.get(property.getRequiredColumnName());
	}

	@Override
	public boolean hasProperty(ScyllaPersistentProperty property) {

		Assert.notNull(property, "ScyllaPersistentProperty must not be null");

		return this.reader.contains(property.getRequiredColumnName());
	}

	@Override
	public Object getSource() {
		return this.reader.getRow();
	}
}
