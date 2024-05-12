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
package com.giangbb.scylla.core.convert;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.giangbb.scylla.core.mapping.ScyllaPersistentProperty;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link ScyllaValueProvider} to read property values from a {@link UdtValue}.
 *
 * @author Giangbb
 */
public class UdtValueProvider implements ScyllaValueProvider {

	private final UdtValue udtValue;

	private final SpELExpressionEvaluator evaluator;

	/**
	 * Create a new {@link UdtValueProvider} with the given {@link UdtValue} and {@link SpELExpressionEvaluator}.
	 *
	 * @param udtValue must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 */
	public UdtValueProvider(UdtValue udtValue, SpELExpressionEvaluator evaluator) {

		Assert.notNull(udtValue, "UDTValue must not be null");
		Assert.notNull(evaluator, "SpELExpressionEvaluator must not be null");

		this.udtValue = udtValue;
		this.evaluator = evaluator;
	}

	@Nullable
	public <T> T getPropertyValue(ScyllaPersistentProperty property) {

		String spelExpression = property.getSpelExpression();

		if (spelExpression != null) {
			return this.evaluator.evaluate(spelExpression);
		}

		return (T) this.udtValue.getObject(property.getRequiredColumnName());
	}

	@Override
	public boolean hasProperty(ScyllaPersistentProperty property) {
		return this.udtValue.getType().contains(property.getRequiredColumnName());
	}

	@Override
	public Object getSource() {
		return this.udtValue;
	}
}
