/*
 * Copyright 2017-2024 the original author or authors.
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

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.core.convert.converter.GenericConverter.ConvertiblePair;
import com.giangbb.scylla.core.mapping.ScyllaPersistentProperty;
import com.giangbb.scylla.core.mapping.ScyllaSimpleTypeHolder;
import org.springframework.data.convert.ConverterBuilder;
import org.springframework.data.convert.Jsr310Converters;
import org.springframework.data.convert.PropertyValueConversions;
import org.springframework.data.convert.PropertyValueConverter;
import org.springframework.data.convert.PropertyValueConverterFactory;
import org.springframework.data.convert.PropertyValueConverterRegistrar;
import org.springframework.data.convert.SimplePropertyValueConversions;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.util.Assert;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Value object to capture custom conversion. {@link ScyllaCustomConversions} also act as factory for
 * {@link SimpleTypeHolder}
 *
 * @author Giangbb
 * @see org.springframework.data.convert.CustomConversions
 * @see SimpleTypeHolder
 */
public class ScyllaCustomConversions extends org.springframework.data.convert.CustomConversions {

	private static final List<Object> STORE_CONVERTERS;

	private static final StoreConversions STORE_CONVERSIONS;

	static {

		List<Object> converters = new ArrayList<>();

		converters.addAll(ScyllaConverters.getConvertersToRegister());
		converters.addAll(ScyllaJsr310Converters.getConvertersToRegister());

		STORE_CONVERTERS = Collections.unmodifiableList(converters);
		STORE_CONVERSIONS = StoreConversions.of(ScyllaSimpleTypeHolder.HOLDER, STORE_CONVERTERS);
	}

	/**
	 * Create a new {@link ScyllaCustomConversions} instance registering the given converters.
	 *
	 * @param converters must not be {@literal null}.
	 */
	public ScyllaCustomConversions(List<?> converters) {
		super(new ScyllaConverterConfiguration(converters));
	}

	/**
	 * Create a new {@link ScyllaCustomConversions} given {@link ScyllaConverterConfigurationAdapter}.
	 *
	 * @param conversionConfiguration must not be {@literal null}.
	 */
	protected ScyllaCustomConversions(ScyllaConverterConfigurationAdapter conversionConfiguration) {
		super(conversionConfiguration.createConverterConfiguration());
	}

	/**
	 * Functional style {@link org.springframework.data.convert.CustomConversions} creation giving users a convenient way
	 * of configuring store specific capabilities by providing deferred hooks to what will be configured when creating the
	 * {@link org.springframework.data.convert.CustomConversions#CustomConversions(ConverterConfiguration) instance}.
	 *
	 * @param configurer must not be {@literal null}.
	 */
	public static ScyllaCustomConversions create(Consumer<ScyllaConverterConfigurationAdapter> configurer) {

		ScyllaConverterConfigurationAdapter adapter = new ScyllaConverterConfigurationAdapter();
		configurer.accept(adapter);

		return new ScyllaCustomConversions(adapter);
	}

	/**
	 * Scylla-specific extension to {@link ConverterConfiguration}.
	 * This extension avoids {@link Converter} registrations that enforce date mapping to {@link Date} from JSR-310.
	 */
	static class ScyllaConverterConfiguration extends ConverterConfiguration {

		ScyllaConverterConfiguration(List<?> converters) {
			super(STORE_CONVERSIONS, converters, getConverterFilter());

		}

		ScyllaConverterConfiguration(List<?> userConverters, PropertyValueConversions propertyValueConversions) {
			super(STORE_CONVERSIONS, userConverters, getConverterFilter(), propertyValueConversions);
		}

		static Predicate<ConvertiblePair> getConverterFilter() {

			return convertiblePair -> !(Jsr310Converters.supports(convertiblePair.getSourceType())
					&& Date.class.isAssignableFrom(convertiblePair.getTargetType()));
		}
	}

	/**
	 * {@link ScyllaConverterConfigurationAdapter} encapsulates creation of
	 * {@link ConverterConfiguration} with Scylla specifics.
	 *
	 */
	public static class ScyllaConverterConfigurationAdapter {

		private final List<Object> customConverters = new ArrayList<>();

		private final PropertyValueConversions internalValueConversion = PropertyValueConversions.simple(it -> {});
		private PropertyValueConversions propertyValueConversions = internalValueConversion;

		/**
		 * Create a {@link ScyllaConverterConfigurationAdapter} using the provided {@code converters} and our own codecs
		 * for JSR-310 types.
		 *
		 * @param converters must not be {@literal null}.
		 * @return
		 */
		public static ScyllaConverterConfigurationAdapter from(List<?> converters) {

			Assert.notNull(converters, "Converters must not be null");

			ScyllaConverterConfigurationAdapter adapter = new ScyllaConverterConfigurationAdapter();
			adapter.registerConverters(converters);

			return adapter;
		}

		/**
		 * Add a custom {@link Converter} implementation.
		 *
		 * @param converter must not be {@literal null}.
		 * @return this.
		 */
		public ScyllaConverterConfigurationAdapter registerConverter(Converter<?, ?> converter) {

			Assert.notNull(converter, "Converter must not be null");

			customConverters.add(converter);
			return this;
		}

		/**
		 * Add a custom {@link ConverterFactory} implementation.
		 *
		 * @param converterFactory must not be {@literal null}.
		 * @return this.
		 */
		public ScyllaConverterConfigurationAdapter registerConverterFactory(ConverterFactory<?, ?> converterFactory) {

			Assert.notNull(converterFactory, "ConverterFactory must not be null");

			customConverters.add(converterFactory);
			return this;
		}

		/**
		 * Add {@link Converter converters}, {@link ConverterFactory factories}, {@link ConverterBuilder.ConverterAware
		 * converter-aware objects}, and {@link GenericConverter generic converters}.
		 *
		 * @param converters must not be {@literal null} nor contain {@literal null} values.
		 * @return this.
		 */
		public ScyllaConverterConfigurationAdapter registerConverters(Object... converters) {
			return registerConverters(Arrays.asList(converters));
		}

		/**
		 * Add {@link Converter converters}, {@link ConverterFactory factories}, {@link ConverterBuilder.ConverterAware
		 * converter-aware objects}, and {@link GenericConverter generic converters}.
		 *
		 * @param converters must not be {@literal null} nor contain {@literal null} values.
		 * @return this.
		 */
		public ScyllaConverterConfigurationAdapter registerConverters(Collection<?> converters) {

			Assert.notNull(converters, "Converters must not be null");
			Assert.noNullElements(converters, "Converters must not be null nor contain null values");

			customConverters.addAll(converters);
			return this;
		}

		/**
		 * Add a custom/default {@link PropertyValueConverterFactory} implementation used to serve
		 * {@link PropertyValueConverter}.
		 *
		 * @param converterFactory must not be {@literal null}.
		 * @return this.
		 */
		public ScyllaConverterConfigurationAdapter registerPropertyValueConverterFactory(
				PropertyValueConverterFactory converterFactory) {

			Assert.state(valueConversions() instanceof SimplePropertyValueConversions,
					"Configured PropertyValueConversions does not allow setting custom ConverterRegistry");

			((SimplePropertyValueConversions) valueConversions()).setConverterFactory(converterFactory);
			return this;
		}

		/**
		 * Gateway to register property specific converters.
		 *
		 * @param configurationAdapter must not be {@literal null}.
		 * @return this.
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public ScyllaConverterConfigurationAdapter configurePropertyConversions(
				Consumer<PropertyValueConverterRegistrar<ScyllaPersistentProperty>> configurationAdapter) {

			Assert.state(valueConversions() instanceof SimplePropertyValueConversions,
					"Configured PropertyValueConversions does not allow setting custom ConverterRegistry");

			PropertyValueConverterRegistrar propertyValueConverterRegistrar = new PropertyValueConverterRegistrar();
			configurationAdapter.accept(propertyValueConverterRegistrar);

			((SimplePropertyValueConversions) valueConversions())
					.setValueConverterRegistry(propertyValueConverterRegistrar.buildRegistry());
			return this;
		}

		/**
		 * Optionally set the {@link PropertyValueConversions} to be applied during mapping.
		 * <p>
		 * Use this method if {@link #configurePropertyConversions(Consumer)} and
		 * {@link #registerPropertyValueConverterFactory(PropertyValueConverterFactory)} are not sufficient.
		 *
		 * @param valueConversions must not be {@literal null}.
		 * @return this.
		 */
		public ScyllaConverterConfigurationAdapter withPropertyValueConversions(
				PropertyValueConversions valueConversions) {

			Assert.notNull(valueConversions, "PropertyValueConversions must not be null");

			this.propertyValueConversions = valueConversions;
			return this;
		}

		PropertyValueConversions valueConversions() {

			if (this.propertyValueConversions == null) {
				this.propertyValueConversions = internalValueConversion;
			}

			return this.propertyValueConversions;
		}

		ScyllaConverterConfiguration createConverterConfiguration() {

			if (hasDefaultPropertyValueConversions()
					&& propertyValueConversions instanceof SimplePropertyValueConversions svc) {
				svc.init();
			}

			return new ScyllaConverterConfiguration(this.customConverters, this.propertyValueConversions);
		}

		private boolean hasDefaultPropertyValueConversions() {
			return propertyValueConversions == internalValueConversion;
		}
	}
}
