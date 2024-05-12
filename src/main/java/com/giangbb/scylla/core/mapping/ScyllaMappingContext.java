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
package com.giangbb.scylla.core.mapping;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.Optionals;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;
import com.giangbb.scylla.core.convert.ScyllaCustomConversions;
import com.giangbb.scylla.core.convert.MappingScyllaConverter;
import com.giangbb.scylla.core.convert.ScyllaConverter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.StreamSupport;

/**
 * Default implementation of a {@link MappingContext} for Scylla using {@link ScyllaPersistentEntity} and
 * {@link ScyllaPersistentProperty} as primary abstractions.
 *
 * @author Giangbb
 */
public class ScyllaMappingContext
		extends AbstractMappingContext<BasicScyllaPersistentEntity<?>, ScyllaPersistentProperty>
		implements ApplicationContextAware, BeanClassLoaderAware {

	private @Nullable ApplicationContext applicationContext;

	private ScyllaPersistentEntityMetadataVerifier verifier = new CompositeScyllaPersistentEntityMetadataVerifier();

	private @Nullable ClassLoader beanClassLoader;

	private CodecRegistry codecRegistry = CodecRegistry.DEFAULT;

	private CustomConversions customConversions = new ScyllaCustomConversions(Collections.emptyList());

	private Mapping mapping = new Mapping();

	private @Nullable NamingConvention namingStrategy;

	private @Nullable UserTypeResolver userTypeResolver;

	// caches
	private final Map<CqlIdentifier, Set<ScyllaPersistentEntity<?>>> entitySetsByTableName = new ConcurrentHashMap<>();

	private final Set<BasicScyllaPersistentEntity<?>> tableEntities = ConcurrentHashMap.newKeySet();

	private final Set<BasicScyllaPersistentEntity<?>> userDefinedTypes = ConcurrentHashMap.newKeySet();

	/**
	 * Create a new {@link ScyllaMappingContext}.
	 */
	public ScyllaMappingContext() {
		setSimpleTypeHolder(customConversions.getSimpleTypeHolder());
	}

	/**
	 * Create a new {@link ScyllaMappingContext} given {@link UserTypeResolver}.
	 *
	 * @param userTypeResolver must not be {@literal null}.
	 */
	public ScyllaMappingContext(UserTypeResolver userTypeResolver) {
		setUserTypeResolver(userTypeResolver);
		setSimpleTypeHolder(customConversions.getSimpleTypeHolder());
	}

	@Override
	public void initialize() {

		super.initialize();

		processMappingOverrides();
	}

	@SuppressWarnings("all")
	private void processMappingOverrides() {

		this.mapping.getEntityMappings().stream().filter(Objects::nonNull).forEach(entityMapping -> {

			Class<?> entityClass = getEntityClass(entityMapping.getEntityClassName());

			ScyllaPersistentEntity<?> entity = getRequiredPersistentEntity(entityClass);

			String entityTableName = entityMapping.getTableName();

			if (StringUtils.hasText(entityTableName)) {
				entity.setTableName(
						CqlIdentifierGenerator.createIdentifier(entityTableName, Boolean.valueOf(entityMapping.getForceQuote())));
			}

			processMappingOverrides(entity, entityMapping);
		});
	}

	private Class<?> getEntityClass(String entityClassName) {

		try {
			return ClassUtils.forName(entityClassName, this.beanClassLoader);
		} catch (ClassNotFoundException cause) {
			throw new IllegalStateException(String.format("Unknown persistent entity type name [%s]", entityClassName),
					cause);
		}
	}

	private static void processMappingOverrides(ScyllaPersistentEntity<?> entity, EntityMapping entityMapping) {

		entityMapping.getPropertyMappings()
				.forEach((key, propertyMapping) -> processMappingOverride(entity, propertyMapping));
	}

	private static void processMappingOverride(ScyllaPersistentEntity<?> entity, PropertyMapping mapping) {

		ScyllaPersistentProperty property = entity.getRequiredPersistentProperty(mapping.getPropertyName());

		boolean forceQuote = Boolean.parseBoolean(mapping.getForceQuote());

		if (StringUtils.hasText(mapping.getColumnName())) {
			property.setColumnName(CqlIdentifierGenerator.createIdentifier(mapping.getColumnName(), forceQuote));
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public void setBeanClassLoader(ClassLoader beanClassLoader) {
		this.beanClassLoader = beanClassLoader;
	}

	/**
	 * Sets the {@link CustomConversions}.
	 *
	 * @param customConversions must not be {@literal null}.
	 */
	public void setCustomConversions(CustomConversions customConversions) {

		Assert.notNull(customConversions, "CustomConversions must not be null");

		this.customConversions = customConversions;
	}


	public CustomConversions getCustomConversions() {
		return customConversions;
	}

	/**
	 * Sets the {@link Mapping}.
	 *
	 * @param mapping must not be {@literal null}.
	 */
	public void setMapping(Mapping mapping) {

		Assert.notNull(mapping, "Mapping must not be null");

		this.mapping = mapping;
	}

	/**
	 * Returns only {@link Table} entities.
	 *
	 */
	public Collection<BasicScyllaPersistentEntity<?>> getTableEntities() {
		return Collections.unmodifiableCollection(this.tableEntities);
	}

	/**
	 * Returns only those entities representing a user defined type.
	 *
	 */
	public Collection<ScyllaPersistentEntity<?>> getUserDefinedTypeEntities() {
		return Collections.unmodifiableSet(this.userDefinedTypes);
	}

	/**
	 * Sets the {@link CodecRegistry}.
	 *
	 * @param codecRegistry must not be {@literal null}.
	 */
	public void setCodecRegistry(CodecRegistry codecRegistry) {

		Assert.notNull(codecRegistry, "CodecRegistry must not be null");

		this.codecRegistry = codecRegistry;
	}


	public CodecRegistry getCodecRegistry() {
		return this.codecRegistry;
	}

	/**
	 * Set the {@link NamingConvention} to use.
	 *
	 * @param namingStrategy must not be {@literal null}.
	 */
	public void setNamingStrategy(NamingConvention namingStrategy) {

		Assert.notNull(namingStrategy, "NamingStrategy must not be null");

		this.namingStrategy = namingStrategy;
	}


	/**
	 * Sets the {@link UserTypeResolver}.
	 *
	 * @param userTypeResolver must not be {@literal null}.
	 */
	@Deprecated
	public void setUserTypeResolver(UserTypeResolver userTypeResolver) {

		Assert.notNull(userTypeResolver, "UserTypeResolver must not be null");

		this.userTypeResolver = userTypeResolver;
	}

	/**
	 * @deprecated since 3.0. Retrieve {@link UserTypeResolver} directly from
	 *             {@link ScyllaConverter}.
	 */
	@Nullable
	@Deprecated
	public UserTypeResolver getUserTypeResolver() {
		return this.userTypeResolver;
	}

	/**
	 * @param verifier The verifier to set.
	 */
	public void setVerifier(ScyllaPersistentEntityMetadataVerifier verifier) {
		this.verifier = verifier;
	}

	/**
	 * @return Returns the verifier.
	 */
	public ScyllaPersistentEntityMetadataVerifier getVerifier() {
		return this.verifier;
	}

	@Override
	protected Optional<BasicScyllaPersistentEntity<?>> addPersistentEntity(TypeInformation<?> typeInformation) {
		// Prevent conversion types created as ScyllaPersistentEntity
		Optional<BasicScyllaPersistentEntity<?>> optional = shouldCreatePersistentEntityFor(typeInformation)
				? super.addPersistentEntity(typeInformation)
				: Optional.empty();

		optional.ifPresent(entity -> {

			if (entity.isUserDefinedType()) {
				this.userDefinedTypes.add(entity);
			}
			// now do some caching of the entity

			Set<ScyllaPersistentEntity<?>> entities = this.entitySetsByTableName.computeIfAbsent(entity.getTableName(),
					cqlIdentifier -> ConcurrentHashMap.newKeySet());

			entities.add(entity);

			if (!entity.isUserDefinedType() && !entity.isTupleType() && entity.isAnnotationPresent(Table.class)) {
				this.tableEntities.add(entity);
			}
		});

		return optional;
	}

	@Override
	protected boolean shouldCreatePersistentEntityFor(TypeInformation<?> typeInfo) {
		return !this.customConversions.hasCustomWriteTarget(typeInfo.getType())
				&& super.shouldCreatePersistentEntityFor(typeInfo);
	}

	@Override
	protected <T> BasicScyllaPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {


		BasicScyllaPersistentEntity<T> entity = isUserDefinedType(typeInformation)
				? new ScyllaUserTypePersistentEntity<>(typeInformation, getVerifier())
				: isTuple(typeInformation) ? new BasicScyllaPersistentTupleEntity<>(typeInformation)
						: new BasicScyllaPersistentEntity<>(typeInformation, getVerifier());

		if (entity.hasExplicitNamingStrategy()){
			entity.setNamingStrategy(entity.getExplicitNamingStrategy());
		}else if (this.namingStrategy != null) {
			entity.setNamingStrategy(this.namingStrategy);
		}

		Optional.ofNullable(this.applicationContext).ifPresent(entity::setApplicationContext);
		return entity;
	}

	private boolean isTuple(TypeInformation<?> typeInformation) {
		return AnnotatedElementUtils.hasAnnotation(typeInformation.getType(), Tuple.class);
	}

	private boolean isUserDefinedType(TypeInformation<?> typeInformation) {
		return AnnotatedElementUtils.hasAnnotation(typeInformation.getType(), UserDefinedType.class);
	}

	@Override
	protected ScyllaPersistentProperty createPersistentProperty(Property property,
																BasicScyllaPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {

		BasicScyllaPersistentProperty persistentProperty = owner.isTupleType()
				? new BasicScyllaPersistentTupleProperty(property, owner, simpleTypeHolder)
				: new CachingScyllaPersistentProperty(property, owner, simpleTypeHolder);

		if (owner.hasExplicitNamingStrategy()){
			persistentProperty.setNamingStrategy(owner.getExplicitNamingStrategy());
		}else if (this.namingStrategy != null) {
			persistentProperty.setNamingStrategy(this.namingStrategy);
		}

		Optional.ofNullable(this.applicationContext).ifPresent(persistentProperty::setApplicationContext);

		return persistentProperty;
	}

	/**
	 * Returns whether this mapping context has any entities mapped to the given table.
	 *
	 * @param name must not be {@literal null}.
	 * @return {@literal true} is this {@literal TableMetadata} is used by a mapping.
	 */
	public boolean usesTable(CqlIdentifier name) {

		Assert.notNull(name, "Table name must not be null");

		return this.entitySetsByTableName.containsKey(name);
	}

	/**
	 * Returns whether this mapping context has any entities using the given user type.
	 *
	 * @param name must not be {@literal null}.
	 * @return {@literal true} is this {@literal UserType} is used.
	 */
	public boolean usesUserType(CqlIdentifier name) {

		Assert.notNull(name, "User type name must not be null");

		return hasMappedUserType(name) || hasReferencedUserType(name);
	}

	private boolean hasMappedUserType(CqlIdentifier identifier) {
		return this.userDefinedTypes.stream().map(ScyllaPersistentEntity::getTableName).anyMatch(identifier::equals);
	}

	private boolean hasReferencedUserType(CqlIdentifier identifier) {

		return getPersistentEntities().stream().flatMap(entity -> StreamSupport.stream(entity.spliterator(), false))
				.flatMap(it -> Optionals.toStream(Optional.ofNullable(it.findAnnotation(ScyllaType.class))))
				.map(ScyllaType::userTypeName).filter(StringUtils::hasText).map(CqlIdentifier::fromCql)
				.anyMatch(identifier::equals);
	}
}
