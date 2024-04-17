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
package com.giangbb.scylla.core.convert;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.giangbb.scylla.core.mapping.ScyllaPersistentProperty;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link ScyllaValueProvider} to read property values from a {@link TupleValue}.
 *
 * @author Giangbb
 */
public class TupleValueProvider implements ScyllaValueProvider {

	private final CodecRegistry codecRegistry;

	private final SpELExpressionEvaluator evaluator;

	private final TupleValue tupleValue;

	/**
	 * Create a new {@link TupleValueProvider} with the given {@link TupleValue} and {@link SpELExpressionEvaluator}.
	 *
	 * @param tupleValue must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 */
	public TupleValueProvider(TupleValue tupleValue, SpELExpressionEvaluator evaluator) {

		Assert.notNull(tupleValue, "TupleValue must not be null");
		Assert.notNull(evaluator, "SpELExpressionEvaluator must not be null");

		this.tupleValue = tupleValue;
		this.codecRegistry = tupleValue.codecRegistry();
		this.evaluator = evaluator;
	}

	@Nullable
	@Override
	public <T> T getPropertyValue(ScyllaPersistentProperty property) {

		String spelExpression = property.getSpelExpression();

		if (spelExpression != null) {
			return evaluator.evaluate(spelExpression);
		}

		int ordinal = property.getRequiredOrdinal();
		DataType elementType = tupleValue.getType().getComponentTypes().get(ordinal);

		return tupleValue.get(ordinal, codecRegistry.codecFor(elementType));
	}

	@Override
	public boolean hasProperty(ScyllaPersistentProperty property) {
		return this.tupleValue.getType().getComponentTypes().size() >= property.getRequiredOrdinal();
	}

	@Override
	public Object getSource() {
		return this.tupleValue;
	}
}
