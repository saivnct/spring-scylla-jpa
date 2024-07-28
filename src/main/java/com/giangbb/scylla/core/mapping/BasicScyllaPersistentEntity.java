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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryAccessor;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.Parameter;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * Scylla specific {@link BasicPersistentEntity} implementation that adds Scylla specific metadata.
 *
 * @author Giangbb
 */
public class BasicScyllaPersistentEntity<T> extends BasicPersistentEntity<T, ScyllaPersistentProperty>
		implements ScyllaPersistentEntity<T>, ApplicationContextAware {

	private static final ScyllaPersistentEntityMetadataVerifier DEFAULT_VERIFIER = new CompositeScyllaPersistentEntityMetadataVerifier();

	private final CqlIdentifierGenerator namingAccessor = new CqlIdentifierGenerator();

	private ScyllaPersistentEntityMetadataVerifier verifier = DEFAULT_VERIFIER;

	private CqlIdentifier tableName;

	private @Nullable StandardEvaluationContext spelContext;

	private final Map<Parameter<?, ScyllaPersistentProperty>, ScyllaPersistentProperty> constructorProperties = new ConcurrentHashMap<>();

	/**
	 * Create a new {@link BasicScyllaPersistentEntity} given {@link TypeInformation}.
	 *
	 * @param typeInformation must not be {@literal null}.
	 */
	public BasicScyllaPersistentEntity(TypeInformation<T> typeInformation) {
		this(typeInformation, DEFAULT_VERIFIER);
	}

	/**
	 * Create a new {@link BasicScyllaPersistentEntity} with the given {@link TypeInformation}. Will default the table
	 * name to the entity's simple type name.
	 *
	 * @param typeInformation must not be {@literal null}.
	 * @param verifier must not be {@literal null}.
	 */
	public BasicScyllaPersistentEntity(TypeInformation<T> typeInformation,
									   ScyllaPersistentEntityMetadataVerifier verifier) {

		super(typeInformation, ScyllaPersistentPropertyComparator.INSTANCE);

		setVerifier(verifier);
	}

	/**
	 * Create a new {@link BasicScyllaPersistentEntity} with the given {@link TypeInformation}. Will default the table
	 * name to the entity's simple type name.
	 *
	 * @param typeInformation must not be {@literal null}.
	 * @param verifier must not be {@literal null}.
	 * @param comparator must not be {@literal null}.
	 */
	protected BasicScyllaPersistentEntity(TypeInformation<T> typeInformation,
										  ScyllaPersistentEntityMetadataVerifier verifier, Comparator<ScyllaPersistentProperty> comparator) {

		super(typeInformation, comparator);

		setVerifier(verifier);
	}

	protected CqlIdentifier determineTableName() {
		return determineTableName(NamingGenerator::getCQLName, findAnnotation(CqlName.class));
	}

	CqlIdentifier determineTableName(
			BiFunction<NamingConvention, ScyllaPersistentEntity<?>, String> defaultNameGenerator,
			@Nullable CqlName cqlName) {

		if (cqlName != null) {
			return this.namingAccessor.generate(cqlName.value(), false, defaultNameGenerator, this, this.spelContext);
		}

		return this.namingAccessor.generate(null, false, defaultNameGenerator, this, this.spelContext);
	}

	@Override
	public void addAssociation(Association<ScyllaPersistentProperty> association) {
		throw new UnsupportedScyllaOperationException("Scylla does not support associations");
	}

	@Override
	public void doWithAssociations(AssociationHandler<ScyllaPersistentProperty> handler) {}


	@Override
	public void verify() throws MappingException {

		super.verify();

		this.verifier.verify(this);

		if (this.tableName == null) {
			setTableName(determineTableName());
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {

		Assert.notNull(context, "ApplicationContext must not be null");

		spelContext = new StandardEvaluationContext();
		spelContext.addPropertyAccessor(new BeanFactoryAccessor());
		spelContext.setBeanResolver(new BeanFactoryResolver(context));
		spelContext.setRootObject(context);
	}

	@Override
	public void setTableName(CqlIdentifier tableName) {

		Assert.notNull(tableName, "CqlIdentifier must not be null");

		this.tableName = tableName;
	}

	/**
	 * Set the {@link NamingConvention} to use.
	 *
	 * @param namingStrategy must not be {@literal null}.
	 */
	public void setNamingStrategy(NamingConvention namingStrategy) {
		this.namingAccessor.setNamingStrategy(namingStrategy);
	}

	@Override
	public CqlIdentifier getTableName() {
		return Optional.ofNullable(this.tableName).orElseGet(this::determineTableName);
	}

	/**
	 * @param verifier The verifier to set.
	 */
	public void setVerifier(ScyllaPersistentEntityMetadataVerifier verifier) {
		this.verifier = verifier;
	}

	/**
	 * @return the verifier.
	 */
	@SuppressWarnings("unused")
	public ScyllaPersistentEntityMetadataVerifier getVerifier() {
		return this.verifier;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public boolean isUserDefinedType() {
		return false;
	}

	@Override
	public ScyllaPersistentProperty getProperty(Parameter<?, ScyllaPersistentProperty> parameter) {

		if (parameter.getName() == null) {
			return null;
		}

		MergedAnnotations annotations = parameter.getAnnotations();
		if (annotations.isPresent(StaticColumn.class) || annotations.isPresent(Element.class)) {

			return constructorProperties.computeIfAbsent(parameter, it -> {

				ScyllaPersistentProperty property = getPersistentProperty(it.getName());
				return new AnnotatedScyllaConstructorProperty(
						property == null ? new ScyllaConstructorProperty(it, this) : property, it.getAnnotations());
			});
		}

		return getPersistentProperty(parameter.getName());
	}
}
