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

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.lang.NonNull;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static java.time.ZoneId.systemDefault;

/**
 * Helper class to register JodaTime specific {@link Converter} implementations in case the library is present on the
 * classpath.
 *
 * @author Giangbb
 */
public abstract class ScyllaJsr310Converters {

	private ScyllaJsr310Converters() {}

	/**
	 * Returns the converters to be registered. Will only return converters in case we're running on Java 8.
	 *
	 * @return
	 */
	public static Collection<Converter<?, ?>> getConvertersToRegister() {

		List<Converter<?, ?>> converters = new ArrayList<>();

		converters.add(MillisOfDayToLocalTimeConverter.INSTANCE);
		converters.add(LocalTimeToMillisOfDayConverter.INSTANCE);

		converters.add(DateToInstantConverter.INSTANCE);
		converters.add(InstantToDateConverter.INSTANCE);
		converters.add(LocalDateTimeToInstantConverter.INSTANCE);

		return converters;
	}

	/**
	 * Simple singleton to convert {@link Long}s to their {@link LocalTime} representation.
	 *
	 */
	@ReadingConverter
	public enum MillisOfDayToLocalTimeConverter implements Converter<Long, LocalTime> {

		INSTANCE;

		@Override
		public LocalTime convert(Long source) {
			return LocalTime.ofNanoOfDay(source);
		}
	}

	/**
	 * Simple singleton to convert {@link LocalTime}s to their {@link Long} representation.
	 *
	 */
	@ReadingConverter
	public enum LocalTimeToMillisOfDayConverter implements Converter<LocalTime, Long> {

		INSTANCE;

		@Override
		public Long convert(LocalTime source) {
			return source.getLong(ChronoField.NANO_OF_DAY);
		}
	}

	/**
	 * Simple singleton to convert {@link Date}s to their Scylla {@link Instant} representation for the CQL Timestamp
	 * type.
	 *
	 */
	@WritingConverter
	public enum DateToInstantConverter implements Converter<Date, Instant> {

		INSTANCE;

		@Override
		public Instant convert(Date source) {
			return source.toInstant();
		}
	}

	@ReadingConverter
	public enum InstantToDateConverter implements Converter<Instant, Date> {

		INSTANCE;

		@NonNull
		@Override
		public Date convert(Instant source) {
			return Date.from(source);
		}
	}

	/**
	 * Converter from {@link LocalDateTime} to {@link Instant}.
	 *
	 */
	@WritingConverter
	enum LocalDateTimeToInstantConverter implements Converter<LocalDateTime, Instant> {

		INSTANCE;

		@Override
		public Instant convert(LocalDateTime source) {
			return source.atZone(systemDefault()).toInstant();
		}
	}
}
