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
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.stream.Collectors;

/**
 * Aggregator of multiple {@link MappingException} for convenience when verifying persistent entities. This allows the
 * framework to communicate all verification errors to the user of the framework, rather than one at a time.
 *
 */
@SuppressWarnings("serial")
public class VerifierMappingExceptions extends MappingException {

	private final Collection<MappingException> exceptions;

	private final String className;

	/**
	 * Create a new {@link VerifierMappingExceptions} for the given {@code entity} and message.
	 *
	 * @param entity must not be {@literal null}.
	 * @param exceptions must not be {@literal null}.
	 */
	public VerifierMappingExceptions(ScyllaPersistentEntity<?> entity, Collection<MappingException> exceptions) {

		super(String.format("Mapping Exceptions for %s", entity.getName()));

		Assert.notNull(entity, "ScyllaPersistentEntity must not be null");

		this.exceptions = Collections.unmodifiableCollection(new LinkedList<>(exceptions));
		this.className = entity.getType().getName();

		this.exceptions.forEach(this::addSuppressed);
	}

	/**
	 * Create a new {@link VerifierMappingExceptions} for the given {@code entity} and message.
	 *
	 * @param entity must not be {@literal null}.
	 * @param message
	 */
	public VerifierMappingExceptions(ScyllaPersistentEntity<?> entity, String message) {

		super(message);

		Assert.notNull(entity, "ScyllaPersistentEntity must not be null");

		this.exceptions = Collections.emptyList();
		this.className = entity.getType().getName();
	}

	/**
	 * Returns a list of the {@link MappingException}s aggregated within.
	 *
	 * @return collection of {@link MappingException}.
	 */
	public Collection<MappingException> getMappingExceptions() {
		return exceptions;
	}

	/**
	 * Returns a list of the {@link MappingException} messages aggregated within.
	 *
	 * @return collection of messages.
	 */
	public Collection<String> getMessages() {
		return exceptions.stream().map(Throwable::getMessage).collect(Collectors.toList());
	}

	@Override
	public String getMessage() {

		StringBuilder builder = new StringBuilder(className).append(":\n");

		exceptions.forEach(e -> builder.append(" - ").append(e.getMessage()).append("\n"));

		return builder.toString();
	}
}
