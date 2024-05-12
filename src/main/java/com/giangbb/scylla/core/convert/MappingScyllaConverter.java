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
package com.giangbb.scylla.core.convert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.*;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.*;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.util.Predicates;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import com.giangbb.scylla.core.mapping.*;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * {@link ScyllaConverter} that uses a {@link MappingContext} to do sophisticated mapping of domain objects to
 * {@link Row}.
 *
 * @author Giangbb
 */
public class MappingScyllaConverter extends AbstractScyllaConverter
		implements ApplicationContextAware, BeanClassLoaderAware {

	private final Log log = LogFactory.getLog(getClass());

	private final ScyllaMappingContext mappingContext;

	private CodecRegistry codecRegistry;

	private @Nullable UserTypeResolver userTypeResolver;

	private @Nullable ClassLoader beanClassLoader;

	private SpELContext spELContext;

	private final DefaultColumnTypeResolver scyllaTypeResolver;
	private final SpelAwareProxyProjectionFactory projectionFactory = new SpelAwareProxyProjectionFactory();

	/**
	 * Create a new {@link MappingScyllaConverter} with a {@link ScyllaMappingContext}.
	 */
	public MappingScyllaConverter() {
		this(newDefaultMappingContext(new ScyllaCustomConversions(Collections.emptyList())));
	}

	/**
	 * Create a new {@link MappingScyllaConverter} with the given {@link ScyllaMappingContext}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 */
	public MappingScyllaConverter(ScyllaMappingContext mappingContext) {

		super(newConversionService());

		Assert.notNull(mappingContext, "ScyllaMappingContext must not be null");

		UserTypeResolver userTypeResolver = userTypeName -> getUserTypeResolver().resolveType(userTypeName);

		this.mappingContext = mappingContext;
		this.setCodecRegistry(mappingContext.getCodecRegistry());
		this.setCustomConversions(mappingContext.getCustomConversions());
		this.scyllaTypeResolver = new DefaultColumnTypeResolver(mappingContext, userTypeResolver, this::getCodecRegistry,
				this::getCustomConversions);
		this.spELContext = new SpELContext(RowReaderPropertyAccessor.INSTANCE);
	}

	public ScyllaColumnType resolve(ScyllaPersistentProperty property) {
		return scyllaTypeResolver.resolve(property);
	}

	/**
	 * Constructs a new instance of {@link ConversionContext} with various converters to convert different Scylla value
	 * types.
	 *
	 * @return the {@link ConversionContext}.
	 * @see ConversionContext
	 */
	protected ConversionContext getConversionContext() {

		return new ConversionContext(getCustomConversions(), this::doReadRow, this::doReadTupleValue, this::doReadUdtValue,
				this::readCollectionOrArray, this::readMap, this::getPotentiallyConvertedSimpleRead);
	}

	private static ConversionService newConversionService() {
		return new DefaultConversionService();
	}

	/**
	 * Constructs a new instance of a {@link MappingContext} for Scylla.
	 *
	 * @param conversions {@link ScyllaCustomConversions} object encapsulating complex type conversion logic.
	 * @return a new {@link ScyllaMappingContext}.
	 * @see ScyllaMappingContext
	 * @see MappingContext
	 */
	private static ScyllaMappingContext newDefaultMappingContext(ScyllaCustomConversions conversions) {

		ScyllaMappingContext mappingContext = new ScyllaMappingContext();

		mappingContext.setCustomConversions(conversions);
		mappingContext.afterPropertiesSet();

		return mappingContext;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.spELContext = new SpELContext(this.spELContext, applicationContext);
		this.projectionFactory.setBeanFactory(applicationContext);
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
		this.projectionFactory.setBeanClassLoader(classLoader);
	}

	private TypeCodec<Object> getCodec(ScyllaPersistentProperty property) {
		return getCodecRegistry().codecFor(scyllaTypeResolver.resolve(property).getDataType());
	}

	@Override
	public ProjectionFactory getProjectionFactory() {
		return projectionFactory;
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

	/**
	 * Returns the configured {@link CodecRegistry}.
	 *
	 * @return the configured {@link CodecRegistry}.
	 */
	@Override
	public CodecRegistry getCodecRegistry() {
		return this.codecRegistry != null ? this.codecRegistry : getMappingContext().getCodecRegistry();
	}

	/**
	 * Sets the {@link UserTypeResolver}.
	 *
	 * @param userTypeResolver must not be {@literal null}.
	 */
	public void setUserTypeResolver(UserTypeResolver userTypeResolver) {

		Assert.notNull(userTypeResolver, "UserTypeResolver must not be null");

		this.userTypeResolver = userTypeResolver;
	}

	/**
	 * Returns the configured {@link UserTypeResolver}.
	 *
	 * @return the configured {@link UserTypeResolver}.
	 */
	public UserTypeResolver getUserTypeResolver() {

		return this.userTypeResolver != null ? this.userTypeResolver : getMappingContext().getUserTypeResolver();
	}

	@Override
	public ScyllaMappingContext getMappingContext() {
		return this.mappingContext;
	}

	@Override
	public ColumnTypeResolver getColumnTypeResolver() {
		return this.scyllaTypeResolver;
	}

	/**
	 * Create a new {@link ConvertingPropertyAccessor} for the given {@link Object source} and
	 * {@link ScyllaPersistentEntity entity}.
	 *
	 * @param source {@link Object} containing the property values to access; must not be {@literal null}.
	 * @param entity {@link ScyllaPersistentEntity} for the source; must not be {@literal null}.
	 * @return a new {@link ConvertingPropertyAccessor} for the given {@link Object source} and
	 *         {@link ScyllaPersistentEntity entity}.
	 * @see ScyllaPersistentEntity
	 */
	@SuppressWarnings("unchecked")
	private <S> ConvertingPropertyAccessor<S> newConvertingPropertyAccessor(S source,
			ScyllaPersistentEntity<?> entity) {
		return new ConvertingPropertyAccessor<>(entity.getPropertyAccessor(source), getConversionService());
	}

	private <S> ScyllaPersistentEntityParameterValueProvider newParameterValueProvider(ConversionContext context,
																					   ScyllaPersistentEntity<S> entity, ScyllaValueProvider valueProvider) {

		return new ScyllaPersistentEntityParameterValueProvider(entity, valueProvider, context, null);
	}

	@SuppressWarnings("unchecked")
	private <T> Class<T> transformClassToBeanClassLoaderClass(Class<T> entity) {

		try {
			return (Class<T>) ClassUtils.forName(entity.getName(), this.beanClassLoader);
		} catch (ClassNotFoundException | LinkageError ignore) {
			return entity;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <R> R project(EntityProjection<R, ?> projection, Row row) {

		if (!projection.isProjection()) {

			TypeInformation<?> typeToRead = projection.getMappedType().getType().isInterface() ? projection.getDomainType()
					: projection.getMappedType();
			return (R) read(typeToRead.getType(), row);
		}

		ProjectingConversionContext context = new ProjectingConversionContext(getCustomConversions(), this::doReadRow,
				this::doReadTupleValue, this::doReadUdtValue, this::readCollectionOrArray, this::readMap,
				this::getPotentiallyConvertedSimpleRead, projection);

		return doReadProjection(context, new RowValueProvider(row, new DefaultSpELExpressionEvaluator(row, spELContext)),
				projection);
	}

	@SuppressWarnings("unchecked")
	private <R> R doReadProjection(ConversionContext context, ScyllaValueProvider valueProvider,
			EntityProjection<R, ?> projection) {

		ScyllaPersistentEntity<?> entity = getMappingContext()
				.getRequiredPersistentEntity(projection.getActualDomainType());
		TypeInformation<?> mappedType = projection.getActualMappedType();
		ScyllaPersistentEntity<R> mappedEntity = (ScyllaPersistentEntity<R>) getMappingContext()
				.getPersistentEntity(mappedType);

		boolean isInterfaceProjection = mappedType.getType().isInterface();
		if (isInterfaceProjection) {

			PersistentPropertyTranslator propertyTranslator = PersistentPropertyTranslator.create(mappedEntity);
			PersistentPropertyAccessor<?> accessor = PropertyTranslatingPropertyAccessor
					.create(new MapPersistentPropertyAccessor(), propertyTranslator);

			readProperties(context, entity, valueProvider, accessor, Predicates.isTrue());
			return (R) projectionFactory.createProjection(mappedType.getType(), accessor.getBean());
		}

		// DTO projection
		if (mappedEntity == null) {
			throw new MappingException(String.format("No mapping metadata found for %s", mappedType.getType().getName()));
		}

		// create target instance, merge metadata from underlying DTO type
		PersistentPropertyTranslator propertyTranslator = PersistentPropertyTranslator.create(entity,
				Predicates.negate(ScyllaPersistentProperty::hasExplicitColumnName));
		ScyllaValueProvider valueProviderToUse = new TranslatingScyllaValueProvider(propertyTranslator,
				valueProvider);

		InstanceCreatorMetadata<ScyllaPersistentProperty> persistenceCreator = mappedEntity.getInstanceCreatorMetadata();

		ParameterValueProvider<ScyllaPersistentProperty> provider;
		if (persistenceCreator != null && persistenceCreator.hasParameters()) {
			SpELExpressionEvaluator evaluator = new DefaultSpELExpressionEvaluator(valueProviderToUse.getSource(),
					spELContext);
			ParameterValueProvider<ScyllaPersistentProperty> parameterValueProvider = newParameterValueProvider(context,
					entity, valueProviderToUse);
			provider = new ConverterAwareSpELExpressionParameterValueProvider(evaluator, getConversionService(),
					parameterValueProvider, context);
		} else {
			provider = NoOpParameterValueProvider.INSTANCE;
		}

		EntityInstantiator instantiator = instantiators.getInstantiatorFor(mappedEntity);
		R instance = instantiator.createInstance(mappedEntity, provider);

		if (mappedEntity.requiresPropertyPopulation()) {

			PersistentPropertyAccessor<R> accessor = mappedEntity.getPropertyAccessor(instance);
			readProperties(context, mappedEntity, valueProviderToUse, accessor, isConstructorArgument(mappedEntity).negate());
			return accessor.getBean();
		}

		return instance;
	}

	private Object doReadOrProject(ConversionContext context, Row row, TypeInformation<?> typeHint,
			EntityProjection<?, ?> typeDescriptor) {

		if (typeDescriptor.isProjection()) {

			ScyllaValueProvider valueProvider = new RowValueProvider(row,
					new DefaultSpELExpressionEvaluator(row, this.spELContext));
			return doReadProjection(context, valueProvider, typeDescriptor);
		}

		return doReadRow(context, row, typeHint);
	}

	private Object doReadOrProject(ConversionContext context, UdtValue udtValue, TypeInformation<?> typeHint,
			EntityProjection<?, ?> typeDescriptor) {

		if (typeDescriptor.isProjection()) {

			ScyllaValueProvider valueProvider = new UdtValueProvider(udtValue,
					new DefaultSpELExpressionEvaluator(udtValue, this.spELContext));
			return doReadProjection(context, valueProvider, typeDescriptor);
		}

		return doReadUdtValue(context, udtValue, typeHint);
	}

	private Object doReadOrProject(ConversionContext context, TupleValue tupleValue, TypeInformation<?> typeHint,
			EntityProjection<?, ?> typeDescriptor) {

		if (typeDescriptor.isProjection()) {

			ScyllaValueProvider valueProvider = new TupleValueProvider(tupleValue,
					new DefaultSpELExpressionEvaluator(tupleValue, this.spELContext));
			return doReadProjection(context, valueProvider, typeDescriptor);
		}

		return doReadTupleValue(context, tupleValue, typeHint);
	}

	@Override
	public <R> R read(Class<R> type, Object row) {

		if (row instanceof Row) {
			return readRow(type, (Row) row);
		}

		throw new MappingException(String.format("Unknown row object [%s]", ObjectUtils.nullSafeClassName(row)));
	}

	/**
	 * Read a {@link Row} into the requested target {@link Class type}.
	 *
	 * @param type must not be {@literal null}.
	 * @param row must not be {@literal null}.
	 * @return the converted valued.
	 */
	public <R> R readRow(Class<R> type, Row row) {

		Class<R> beanClassLoaderClass = transformClassToBeanClassLoaderClass(type);
		TypeInformation<? extends R> typeInfo = TypeInformation.of(beanClassLoaderClass);

		return doReadRow(getConversionContext(), row, typeInfo);
	}

	<S> S doReadRow(ConversionContext context, Row row, TypeInformation<? extends S> typeHint) {
		return doReadEntity(context, row, expressionEvaluator -> new RowValueProvider(row, expressionEvaluator), typeHint);
	}

	<S> S doReadTupleValue(ConversionContext context, TupleValue tupleValue, TypeInformation<? extends S> typeHint) {
		return doReadEntity(context, tupleValue,
				expressionEvaluator -> new TupleValueProvider(tupleValue, expressionEvaluator), typeHint);
	}

	<S> S doReadUdtValue(ConversionContext context, UdtValue udtValue, TypeInformation<? extends S> typeHint) {
		return doReadEntity(context, udtValue, expressionEvaluator -> new UdtValueProvider(udtValue, expressionEvaluator),
				typeHint);
	}

	private <S> S doReadEntity(ConversionContext context, Object value,
			Function<SpELExpressionEvaluator, ScyllaValueProvider> valueProviderSupplier,
			TypeInformation<? extends S> typeHint) {

		SpELExpressionEvaluator expressionEvaluator = new DefaultSpELExpressionEvaluator(value, this.spELContext);
		ScyllaValueProvider valueProvider = valueProviderSupplier.apply(expressionEvaluator);

		return doReadEntity(context, valueProvider, typeHint);
	}

	/**
	 * Conversion method to materialize an object from a {@link Row}, {@link TupleValue}, or {@link UdtValue}. Can be
	 * overridden by subclasses.
	 *
	 * @param context must not be {@literal null}
	 * @param valueProvider must not be {@literal null}
	 * @param typeHint the {@link TypeInformation} to be used to unmarshall this {@link Row}.
	 * @return the converted object, will never be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	protected <S> S doReadEntity(ConversionContext context, ScyllaValueProvider valueProvider,
			TypeInformation<? extends S> typeHint) {

		Class<?> rawType = typeHint.getType();
		Class<?> rawSourceType = getRawSourceType(valueProvider);

		if (rawSourceType.isAssignableFrom(rawType) && rawSourceType.isInstance(valueProvider.getSource())) {
			return (S) valueProvider.getSource();
		}

		if (getCustomConversions().hasCustomReadTarget(rawSourceType, rawType)
				|| getConversionService().canConvert(rawSourceType, rawType)) {
			return (S) getConversionService().convert(valueProvider.getSource(), rawType);
		}

		ScyllaPersistentEntity<S> entity = (ScyllaPersistentEntity<S>) getMappingContext()
				.getPersistentEntity(typeHint);

		if (entity == null) {
			throw new MappingException(
					String.format("Expected to read %s into type %s but didn't find a PersistentEntity for the latter",
							rawSourceType.getSimpleName(), rawType.getName()));
		}

		return doReadEntity(context, valueProvider, entity);
	}

	private static Class<?> getRawSourceType(ScyllaValueProvider valueProvider) {

		if (valueProvider.getSource() instanceof Row) {
			return Row.class;
		}

		if (valueProvider.getSource() instanceof TupleValue) {
			return TupleValue.class;
		}

		if (valueProvider.getSource() instanceof UdtValue) {
			return UdtValue.class;
		}

		throw new InvalidDataAccessApiUsageException(
				String.format("Unsupported source type: %s", ClassUtils.getDescriptiveType(valueProvider.getSource())));
	}

	private <S> S doReadEntity(ConversionContext context, ScyllaValueProvider valueProvider,
			ScyllaPersistentEntity<S> entity) {

		InstanceCreatorMetadata<ScyllaPersistentProperty> persistenceCreator = entity.getInstanceCreatorMetadata();
		ParameterValueProvider<ScyllaPersistentProperty> provider;

		if (persistenceCreator != null && persistenceCreator.hasParameters()) {
			SpELExpressionEvaluator evaluator = new DefaultSpELExpressionEvaluator(valueProvider.getSource(), spELContext);
			ParameterValueProvider<ScyllaPersistentProperty> parameterValueProvider = newParameterValueProvider(context,
					entity, valueProvider);
			provider = new ConverterAwareSpELExpressionParameterValueProvider(evaluator, getConversionService(),
					parameterValueProvider, context);
		} else {
			provider = NoOpParameterValueProvider.INSTANCE;
		}

		EntityInstantiator instantiator = this.instantiators.getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, provider);

		return populateProperties(context, entity, valueProvider, instance);
	}

	private <S> S populateProperties(ConversionContext context, ScyllaPersistentEntity<?> entity,
									 ScyllaValueProvider valueProvider, S instance) {

		if (!entity.requiresPropertyPopulation()) {
			return instance;
		}

		ConvertingPropertyAccessor<S> propertyAccessor = newConvertingPropertyAccessor(instance, entity);
		readProperties(context, entity, valueProvider, propertyAccessor, isConstructorArgument(entity).negate());
		return propertyAccessor.getBean();
	}

	private void readProperties(ConversionContext context, ScyllaPersistentEntity<?> entity,
								ScyllaValueProvider valueProvider, PersistentPropertyAccessor<?> propertyAccessor,
								Predicate<ScyllaPersistentProperty> propertyFilter) {

		for (ScyllaPersistentProperty property : entity) {

			if (!propertyFilter.test(property)) {
				continue;
			}

			ConversionContext contextToUse = context.forProperty(property.getName());

			if (valueProvider.hasProperty(property)) {
				propertyAccessor.setProperty(property, getReadValue(contextToUse, valueProvider, property));
			}
		}
	}

	@Override
	public Object convertToColumnType(Object obj) {
		return convertToColumnType(obj, TypeInformation.of(obj.getClass()));
	}

	@Override
	public Object convertToColumnType(Object value, ColumnType columnType) {
		// noinspection ConstantConditions
		return value.getClass().isArray() ? value : getWriteValue(value, columnType);
	}

	@Override
	public void write(Object source, Object sink) {

		Assert.notNull(source, "Value must not be null");

		Class<?> beanClassLoaderClass = transformClassToBeanClassLoaderClass(source.getClass());

		ScyllaPersistentEntity<?> entity = getMappingContext().getRequiredPersistentEntity(beanClassLoaderClass);

		write(source, sink, entity);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void write(Object source, Object sink, ScyllaPersistentEntity<?> entity) {

		Assert.notNull(source, "Value must not be null");

		if (entity == null) {
			throw new MappingException("No mapping metadata found for " + source.getClass());
		}

		if (sink instanceof Where) {
			writeWhereFromObject(source, (Where) sink, entity);
		} else if (sink instanceof Map) {
			writeInternal(newConvertingPropertyAccessor(source, entity), (Map<CqlIdentifier, Object>) sink, entity);
		} else if (sink instanceof TupleValue) {
			writeTupleValue(newConvertingPropertyAccessor(source, entity), (TupleValue) sink, entity);
		} else if (sink instanceof UdtValue) {
			writeUDTValue(newConvertingPropertyAccessor(source, entity), (UdtValue) sink, entity);
		} else {
			throw new MappingException(String.format("Unknown write target [%s]", ObjectUtils.nullSafeClassName(sink)));
		}
	}

	private void writeInternal(ConvertingPropertyAccessor<?> accessor, Map<CqlIdentifier, Object> sink,
			ScyllaPersistentEntity<?> entity) {

		for (ScyllaPersistentProperty property : entity) {


			Object value = getWriteValue(property, accessor);

			if (log.isDebugEnabled()) {
				log.debug(
						String.format("doWithProperties Property.type %s, Property.value %s", property.getType().getName(), value));
			}

			if (!property.isWritable()) {
				continue;
			}

			if (log.isDebugEnabled()) {
				log.debug(String.format("Adding map.entry [%s] - [%s]", property.getRequiredColumnName(), value));
			}

			sink.put(property.getRequiredColumnName(), value);
		}
	}

	private void writeWhereFromObject(Object source, Where sink, ScyllaPersistentEntity<?> entity) {

		Assert.notNull(source, "Id source must not be null");

		Object id = extractId(source, entity);

		Assert.notNull(id, () -> String.format("No Id value found in object %s", source));

		ScyllaPersistentProperty idProperty = entity.getIdProperty();
		ScyllaPersistentProperty compositeIdProperty = null;

//		if (idProperty != null && idProperty.isCompositePrimaryKey()) {
//			compositeIdProperty = idProperty;
//		}

		if (id instanceof MapId) {

			ScyllaPersistentEntity<?> whereEntity = compositeIdProperty != null
					? getMappingContext().getRequiredPersistentEntity(compositeIdProperty)
					: entity;

			writeWhere((MapId) id, sink, whereEntity);
			return;
		}

		if (idProperty == null) {
			throw new InvalidDataAccessApiUsageException(
					String.format("Cannot obtain where clauses for entity [%s] using [%s]", entity.getName(), source));
		}

		if (compositeIdProperty != null) {

			if (!ClassUtils.isAssignableValue(compositeIdProperty.getType(), id)) {
				throw new InvalidDataAccessApiUsageException(
						String.format("Cannot use [%s] as composite Id for [%s]", id, entity.getName()));
			}

			ScyllaPersistentEntity<?> compositePrimaryKey = getMappingContext()
					.getRequiredPersistentEntity(compositeIdProperty);
			writeInternal(newConvertingPropertyAccessor(id, compositePrimaryKey), sink, compositePrimaryKey);
			return;
		}

		Class<?> targetType = getTargetType(idProperty);

		sink.put(idProperty.getRequiredColumnName(), getPotentiallyConvertedSimpleValue(id, targetType));
	}

	@Nullable
	public Object extractId(Object source, ScyllaPersistentEntity<?> entity) {

		if (ClassUtils.isAssignableValue(entity.getType(), source)) {
			return getId(source, entity);
		} else if (source instanceof MapId) {
			return source;
		} else if (source instanceof MapIdentifiable) {
			return ((MapIdentifiable) source).getMapId();
		}

		return source;
	}

	private void writeWhere(MapId id, Where sink, ScyllaPersistentEntity<?> entity) {

		Assert.notNull(id, "MapId must not be null");

		for (Entry<String, Object> entry : id.entrySet()) {

			ScyllaPersistentProperty persistentProperty = entity.getPersistentProperty(entry.getKey());

			if (persistentProperty == null) {
				throw new IllegalArgumentException(String.format(
						"MapId contains references [%s] that is an unknown property of [%s]", entry.getKey(), entity.getName()));
			}

			Object writeValue = getWriteValue(entry.getValue(), scyllaTypeResolver.resolve(persistentProperty));

			sink.put(persistentProperty.getRequiredColumnName(), writeValue);
		}
	}

	private void writeTupleValue(ConvertingPropertyAccessor<?> propertyAccessor, TupleValue tupleValue,
			ScyllaPersistentEntity<?> entity) {

		for (ScyllaPersistentProperty property : entity) {

			Object value = getWriteValue(property, propertyAccessor);

			if (log.isDebugEnabled()) {
				log.debug(
						String.format("writeTupleValue Property.type %s, Property.value %s", property.getType().getName(), value));
			}

			if (log.isDebugEnabled()) {
				log.debug(String.format("Adding tuple value [%s] - [%s]", property.getOrdinal(), value));
			}

			TypeCodec<Object> typeCodec = getCodec(property);

			tupleValue.set(property.getRequiredOrdinal(), value, typeCodec);
		}
	}

	private void writeUDTValue(ConvertingPropertyAccessor<?> propertyAccessor, UdtValue udtValue,
			ScyllaPersistentEntity<?> entity) {

		for (ScyllaPersistentProperty property : entity) {

			if (!property.isWritable()) {
				continue;
			}

			// value resolution
			Object value = getWriteValue(property, propertyAccessor);

			if (log.isDebugEnabled()) {
				log.debug(String.format("writeUDTValueWhereFromObject Property.type %s, Property.value %s",
						property.getType().getName(), value));
			}

			if (log.isDebugEnabled()) {
				log.debug(String.format("Adding udt.value [%s] - [%s]", property.getRequiredColumnName(), value));
			}


			TypeCodec<Object> typeCodec = getCodec(property);

			udtValue.set(property.getRequiredColumnName().toString(), value, typeCodec);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object getId(Object object, ScyllaPersistentEntity<?> entity) {

		Assert.notNull(object, "Object instance must not be null");
		Assert.notNull(entity, "ScyllaPersistentEntity must not be null");

		ConvertingPropertyAccessor<?> propertyAccessor = newConvertingPropertyAccessor(object, entity);

		Assert.isTrue(entity.getType().isAssignableFrom(object.getClass()),
				() -> String.format("Given instance of type [%s] is not compatible with expected type [%s]",
						object.getClass().getName(), entity.getType().getName()));

		if (object instanceof MapIdentifiable) {
			return ((MapIdentifiable) object).getMapId();
		}

		// if the class doesn't have an id property, then it's using MapId
		MapId id = BasicMapId.id();

		for (ScyllaPersistentProperty property : entity) {

			if (!property.isPrimaryKeyColumn()) {
				continue;
			}

			id.with(property.getName(), getWriteValue(property, propertyAccessor));
		}

		return id;
	}

	/**
	 * Check custom conversions for type override or fall back to
	 * {@link ColumnTypeResolver#resolve(ScyllaPersistentProperty)}.
	 */
	private Class<?> getTargetType(ScyllaPersistentProperty property) {
		return getCustomConversions().getCustomWriteTarget(property.getType())
				.orElseGet(() -> scyllaTypeResolver.resolve(property).getType());
	}

	/**
	 * Retrieve the value to write for the given {@link ScyllaPersistentProperty} from
	 * {@link ConvertingPropertyAccessor} and perform optionally a conversion of collection element types.
	 *
	 * @param property the property.
	 * @param propertyAccessor the property accessor
	 * @return the return value, may be {@literal null}.
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	private <T> T getWriteValue(ScyllaPersistentProperty property, ConvertingPropertyAccessor<?> propertyAccessor) {

		ColumnType scyllaTypeDescriptor = scyllaTypeResolver.resolve(property);
		Object value = propertyAccessor.getProperty(property, scyllaTypeDescriptor.getType());

		if (getCustomConversions().hasValueConverter(property)) {
			return (T) getCustomConversions().getPropertyValueConversions().getValueConverter(property).write(value,
					new ScyllaConversionContext(new PropertyValueProvider<>() {
						@Nullable
						@Override
						public <T> T getPropertyValue(ScyllaPersistentProperty property) {
							return (T) propertyAccessor.getProperty(property);
						}
					}, property, this, spELContext));
		}

		return (T) getWriteValue(value, scyllaTypeDescriptor);
	}

	/**
	 * Retrieve the value from {@code value} applying the given {@link TypeInformation} and perform optionally a
	 * conversion of collection element types.
	 *
	 * @param value the value, may be {@literal null}.
	 * @param columnType the type information.
	 * @return the return value, may be {@literal null}.
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	private Object getWriteValue(@Nullable Object value, ColumnType columnType) {

		if (value == null) {
			return null;
		}

		Class<?> requestedTargetType = columnType.getType();

		if (getCustomConversions().hasCustomWriteTarget(value.getClass(), requestedTargetType)) {

			Class<?> resolvedTargetType = getCustomConversions().getCustomWriteTarget(value.getClass(), requestedTargetType)
					.orElse(requestedTargetType);

			return getConversionService().convert(value, resolvedTargetType);
		}

		if (getCustomConversions().hasCustomWriteTarget(value.getClass())) {

			Class<?> resolvedTargetType = getCustomConversions().getCustomWriteTarget(value.getClass())
					.orElseThrow(() -> new IllegalStateException(String
							.format("Unable to determined custom write target for value type [%s]", value.getClass().getName())));

			return getConversionService().convert(value, resolvedTargetType);
		}

		if (value instanceof Collection) {
			return writeCollectionInternal((Collection<Object>) value, columnType);
		}

		if (value instanceof Map) {
			return writeMapInternal((Map<Object, Object>) value, columnType);
		}

		TypeInformation<?> type = TypeInformation.of((Class<?>) value.getClass());
		TypeInformation<?> actualType = type.getRequiredActualType();
		BasicScyllaPersistentEntity<?> entity = getMappingContext().getPersistentEntity(actualType.getType());

		if (columnType instanceof ScyllaColumnType scyllaType) {

			if (scyllaType.isTupleType()) {

				if (entity != null && entity.isTupleType()) {

					TupleValue tupleValue = ((TupleType) scyllaType.getDataType()).newValue();
					write(value, tupleValue, entity);
					return tupleValue;
				}

				if (value instanceof TupleValue) {
					return value;
				}
			}

			if (scyllaType.isUserDefinedType()) {

				if (entity != null && entity.isUserDefinedType()) {

					UdtValue udtValue = ((UserDefinedType) scyllaType.getDataType()).newValue();
					write(value, udtValue, entity);
					return udtValue;
				}

				if (value instanceof UdtValue) {
					return value;
				}
			}
		}

		if (getCustomConversions().isSimpleType(value.getClass())) {
			return getPotentiallyConvertedSimpleValue(value, requestedTargetType);
		}

		return value;
	}

	private Object writeCollectionInternal(Collection<Object> source, ColumnType type) {

		Collection<Object> converted = CollectionFactory.createCollection(getCollectionType(type), source.size());
		ColumnType componentType = type.getRequiredComponentType();

		for (Object element : source) {

			ColumnType elementType = componentType;
			if (elementType.getType() == Object.class) {
				elementType = getColumnTypeResolver().resolve(element);
			}

			converted.add(getWriteValue(element, elementType));
		}

		return converted;
	}

	private Object writeMapInternal(Map<Object, Object> source, ColumnType type) {

		Map<Object, Object> converted = CollectionFactory.createMap(type.getType(), source.size());

		ColumnType keyType = type.getRequiredComponentType();
		ColumnType valueType = type.getRequiredMapValueType();

		for (Entry<Object, Object> entry : source.entrySet()) {

			ColumnType elementKeyType = keyType;
			if (elementKeyType.getType() == Object.class) {
				elementKeyType = getColumnTypeResolver().resolve(entry.getKey());
			}

			ColumnType elementValueType = valueType;
			if (elementValueType.getType() == Object.class) {
				elementValueType = getColumnTypeResolver().resolve(entry.getValue());
			}

			converted.put(getWriteValue(entry.getKey(), elementKeyType), getWriteValue(entry.getValue(), elementValueType));
		}

		return converted;
	}

	/**
	 * Performs special enum handling or simply returns the value as is.
	 *
	 * @param value may be {@literal null}.
	 * @param requestedTargetType must not be {@literal null}.
	 * @see ScyllaType
	 */
	@Nullable
	private Object getPotentiallyConvertedSimpleValue(@Nullable Object value, @Nullable Class<?> requestedTargetType) {

		if (value == null) {
			return null;
		}

		// Scylla has no default enum handling - convert it to either a String
		// or, if requested, to a different type
		if (Enum.class.isAssignableFrom(value.getClass())) {
			if (requestedTargetType != null && !requestedTargetType.isEnum()
					&& getConversionService().canConvert(value.getClass(), requestedTargetType)) {

				return getConversionService().convert(value, requestedTargetType);
			}

			return ((Enum<?>) value).name();
		}

		if (requestedTargetType != null && !ClassUtils.isAssignableValue(requestedTargetType, value)) {
			return getConversionService().convert(value, requestedTargetType);
		}

		return value;
	}

	/**
	 * Checks whether we have a custom conversion for the given simple object. Converts the given value if so, applies
	 * {@link Enum} handling or returns the value as is. Can be overridden by subclasses.
	 *
	 */
	protected Object getPotentiallyConvertedSimpleRead(Object value, TypeInformation<?> target) {
		return getPotentiallyConvertedSimpleRead(value, target.getType());
	}

	/**
	 * Checks whether we have a custom conversion for the given simple object. Converts the given value if so, applies
	 * {@link Enum} handling or returns the value as is.
	 *
	 * @param value simple value to convert into a value of type {@code target}.
	 * @param target must not be {@literal null}.
	 * @return the converted value.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object getPotentiallyConvertedSimpleRead(@Nullable Object value, @Nullable Class<?> target) {

		if (value == null || target == null || ClassUtils.isAssignableValue(target, value)) {
			return value;
		}

		if (getCustomConversions().hasCustomReadTarget(value.getClass(), target)) {
			return doConvert(value, target);
		}

		if (Enum.class.isAssignableFrom(target)) {

			if (value instanceof Number) {
				int ordinal = ((Number) value).intValue();
				return target.getEnumConstants()[ordinal];
			}

			return Enum.valueOf((Class<Enum>) target, value.toString());
		}

		return doConvert(value, target);
	}

	@SuppressWarnings("ConstantConditions")
	private <T> T doConvert(Object value, Class<T> target) {
		return getConversionService().convert(value, target);
	}

	private static Class<?> getCollectionType(ColumnType type) {
		return type.getType();
	}

	/**
	 * Retrieve the value to read for the given {@link ScyllaPersistentProperty} from {@link ScyllaValueProvider}
	 * and perform optionally a conversion of collection element types.
	 *
	 * @param context must not be {@literal null}.
	 * @param valueProvider the row.
	 * @param property the property.
	 * @return the return value, may be {@literal null}.
	 */
	@Nullable
	private Object getReadValue(ConversionContext context, ScyllaValueProvider valueProvider,
			ScyllaPersistentProperty property) {

		if (!valueProvider.hasProperty(property)) {
			return null;
		}

		Object value = valueProvider.getPropertyValue(property);

		if (getCustomConversions().hasValueConverter(property)) {
			return getCustomConversions().getPropertyValueConversions().getValueConverter(property).read(value,
					new ScyllaConversionContext(valueProvider, property, this, spELContext));
		}

		return value == null ? null : context.convert(value, property.getTypeInformation());
	}


	/**
	 * Reads the given {@link Collection} into a collection of the given {@link TypeInformation}. Will recursively resolve
	 * nested {@link List}s as well. Can be overridden by subclasses.
	 *
	 * @param source must not be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 * @return the converted {@link Collection} or array, will never be {@literal null}.
	 */
	protected Object readCollectionOrArray(ConversionContext context, Collection<?> source,
			TypeInformation<?> targetType) {

		Assert.notNull(targetType, "Target type must not be null");

		Class<?> collectionType = resolveCollectionType(targetType);
		Class<?> elementType = resolveElementType(targetType);

		Collection<Object> collection = targetType.getType().isArray() ? new ArrayList<>()
				: CollectionFactory.createCollection(collectionType, elementType, source.size());

		if (source.isEmpty()) {
			return getPotentiallyConvertedSimpleRead(collection, collectionType);
		}

		TypeInformation<?> componentType = targetType.getComponentType();
		componentType = componentType == null ? TypeInformation.of(elementType) : componentType;

		for (Object element : source) {
			collection.add(context.convert(element, componentType));
		}

		return getPotentiallyConvertedSimpleRead(collection, targetType.getType());
	}

	private static Class<?> resolveCollectionType(TypeInformation<?> typeInformation) {

		Class<?> collectionType = typeInformation.getType();

		return Collection.class.isAssignableFrom(collectionType) ? collectionType : List.class;
	}

	private static Class<?> resolveElementType(TypeInformation<?> typeInformation) {

		TypeInformation<?> componentType = typeInformation.getComponentType();

		return componentType != null ? componentType.getType() : Object.class;
	}

	/**
	 * Reads the given {@link Map} into a map of the given {@link TypeInformation}. Will recursively resolve nested
	 * {@link Map}s as well. Can be overridden by subclasses.
	 *
	 * @param source must not be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 * @return the converted {@link Collection} or array, will never be {@literal null}.
	 */
	protected Object readMap(ConversionContext context, Map<?, ?> source, TypeInformation<?> targetType) {

		Assert.notNull(targetType, "Target type must not be null");

		TypeInformation<?> keyType = targetType.getComponentType();
		TypeInformation<?> valueType = targetType.getMapValueType();

		Class<?> rawKeyType = keyType != null ? keyType.getType() : null;

		Map<Object, Object> map = CollectionFactory.createMap(resolveMapType(targetType), rawKeyType, source.size());

		if (source.isEmpty()) {
			return map;
		}

		for (Entry<?, ?> entry : source.entrySet()) {

			Object key = entry.getKey();

			if (key != null && rawKeyType != null && !rawKeyType.isAssignableFrom(key.getClass())) {
				key = context.convert(key, keyType);
			}

			Object value = entry.getValue();

			map.put(key, context.convert(value, valueType == null ? TypeInformation.OBJECT : valueType));
		}

		return map;
	}

	private static Class<?> resolveMapType(TypeInformation<?> typeInformation) {

		Class<?> mapType = typeInformation.getType();

		return Map.class.isAssignableFrom(mapType) ? mapType : Map.class;
	}

	static Predicate<ScyllaPersistentProperty> isConstructorArgument(PersistentEntity<?, ?> entity) {
		return entity::isConstructorArgument;
	}

	enum NoOpParameterValueProvider implements ParameterValueProvider<ScyllaPersistentProperty> {

		INSTANCE;

		@Override
		public <T> T getParameterValue(Parameter<T, ScyllaPersistentProperty> parameter) {
			return null;
		}
	}

	/**
	 * Extension of {@link SpELExpressionParameterValueProvider} to recursively trigger value conversion on the raw
	 * resolved SpEL value.
	 */
	private static class ConverterAwareSpELExpressionParameterValueProvider
			extends SpELExpressionParameterValueProvider<ScyllaPersistentProperty> {

		private final ConversionContext context;

		/**
		 * Creates a new {@link ConverterAwareSpELExpressionParameterValueProvider}.
		 *
		 * @param evaluator must not be {@literal null}.
		 * @param conversionService must not be {@literal null}.
		 * @param delegate must not be {@literal null}.
		 * @param context must not be {@literal null}.
		 */
		public ConverterAwareSpELExpressionParameterValueProvider(SpELExpressionEvaluator evaluator,
				ConversionService conversionService, ParameterValueProvider<ScyllaPersistentProperty> delegate,
				ConversionContext context) {

			super(evaluator, conversionService, delegate);
			this.context = context;
		}

		@Override
		protected <T> T potentiallyConvertSpelValue(Object object, Parameter<T, ScyllaPersistentProperty> parameter) {
			return (T) context.convert(object, parameter.getType());
		}
	}

	/**
	 * Conversion context holding references to simple {@link ValueConverter} and {@link ContainerValueConverter}.
	 * Entrypoint for recursive conversion of {@link Row} and other types.
	 *
	 */
	protected static class ConversionContext {

		final CustomConversions conversions;

		final ContainerValueConverter<Row> rowConverter;

		final ContainerValueConverter<TupleValue> tupleConverter;

		final ContainerValueConverter<UdtValue> udtConverter;

		final ContainerValueConverter<Collection<?>> collectionConverter;

		final ContainerValueConverter<Map<?, ?>> mapConverter;

		final ValueConverter<Object> elementConverter;

		public ConversionContext(CustomConversions conversions,
                                 ContainerValueConverter<Row> rowConverter, ContainerValueConverter<TupleValue> tupleConverter,
                                 ContainerValueConverter<UdtValue> udtConverter, ContainerValueConverter<Collection<?>> collectionConverter,
                                 ContainerValueConverter<Map<?, ?>> mapConverter, ValueConverter<Object> elementConverter) {
			this.conversions = conversions;
			this.rowConverter = rowConverter;
			this.tupleConverter = tupleConverter;
			this.udtConverter = udtConverter;
			this.collectionConverter = collectionConverter;
			this.mapConverter = mapConverter;
			this.elementConverter = elementConverter;
		}

		/**
		 * Obtain the {@link ConversionContext} for the property identified by the given name.
		 *
		 * @param name must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		ConversionContext forProperty(String name) {
			return this;
		}

		/**
		 * Converts a source object into {@link TypeInformation target}.
		 *
		 * @param source must not be {@literal null}.
		 * @param typeHint must not be {@literal null}.
		 * @return the converted object.
		 */
		@SuppressWarnings("unchecked")
		public <S extends Object> S convert(Object source, TypeInformation<? extends S> typeHint) {

			Assert.notNull(typeHint, "TypeInformation must not be null");

			if (conversions.hasCustomReadTarget(source.getClass(), typeHint.getType())) {
				return (S) elementConverter.convert(source, typeHint);
			}

			if (source instanceof Collection) {

				Class<?> rawType = typeHint.getType();
				if (!Object.class.equals(rawType)) {
					if (!rawType.isArray() && !ClassUtils.isAssignable(Iterable.class, rawType)) {
						throw new MappingException(String.format(
								"Cannot convert %1$s of type %2$s into an instance of %3$s; Implement a custom Converter<%2$s, %3$s> and register it with the CustomConversions",
								source, source.getClass(), rawType));
					}
				}

				if (typeHint.isCollectionLike() || typeHint.getType().isAssignableFrom(Collection.class)) {
					return (S) collectionConverter.convert(this, (Collection<?>) source, typeHint);
				}
			}

			if (typeHint.isMap()) {
				return (S) mapConverter.convert(this, (Map<?, ?>) source, typeHint);
			}

			if (source instanceof Row) {
				return (S) rowConverter.convert(this, (Row) source, typeHint);
			}

			if (source instanceof TupleValue) {
				return (S) tupleConverter.convert(this, (TupleValue) source, typeHint);
			}

			if (source instanceof UdtValue) {
				return (S) udtConverter.convert(this, (UdtValue) source, typeHint);
			}

			return (S) elementConverter.convert(source, typeHint);
		}

		/**
		 * Converts a simple {@code source} value into {@link TypeInformation the target type}.
		 *
		 * @param <T>
		 */
		interface ValueConverter<T> {

			Object convert(T source, TypeInformation<?> typeHint);

		}

		/**
		 * Converts a container {@code source} value into {@link TypeInformation the target type}. Containers may
		 * recursively apply conversions for entities, collections, maps, etc.
		 *
		 * @param <T>
		 */
		interface ContainerValueConverter<T> {

			Object convert(ConversionContext context, T source, TypeInformation<?> typeHint);

		}

	}

	/**
	 * Scylla-specific {@link ParameterValueProvider} considering {@link CqlName @CqlName} and {@link Element @Element}
	 * annotations on constructor parameters.
	 *
	 */
	class ScyllaPersistentEntityParameterValueProvider implements ParameterValueProvider<ScyllaPersistentProperty> {

		private final ScyllaPersistentEntity<?> entity;
		private final ScyllaValueProvider provider;
		private final ConversionContext context;
		private final @Nullable Object parent;

		public ScyllaPersistentEntityParameterValueProvider(ScyllaPersistentEntity<?> entity,
															ScyllaValueProvider provider, ConversionContext context, @Nullable Object parent) {
			this.entity = entity;
			this.provider = provider;
			this.context = context;
			this.parent = parent;
		}

		@Nullable
		@SuppressWarnings("unchecked")
		public <T> T getParameterValue(Parameter<T, ScyllaPersistentProperty> parameter) {

			InstanceCreatorMetadata<ScyllaPersistentProperty> creatorMetadata = entity.getInstanceCreatorMetadata();

			if (creatorMetadata != null && creatorMetadata.isParentParameter(parameter)) {
				return (T) parent;
			}

			String name = parameter.getName();

			if (name == null) {
				throw new MappingException(String.format("Parameter %s does not have a name", parameter));
			}

			ScyllaPersistentProperty property = entity.getProperty(parameter);

			if (property == null) {

				throw new MappingException(String.format("No property %s found on entity %s to bind constructor parameter to",
						name, entity.getType()));
			}

			return (T) getReadValue(context.forProperty(property.getName()), provider, property);
		}

	}

	private record PropertyTranslatingPropertyAccessor<T>(PersistentPropertyAccessor<T> delegate,
			PersistentPropertyTranslator propertyTranslator) implements PersistentPropertyAccessor<T> {

		static <T> PersistentPropertyAccessor<T> create(PersistentPropertyAccessor<T> delegate,
				PersistentPropertyTranslator propertyTranslator) {
			return new PropertyTranslatingPropertyAccessor<>(delegate, propertyTranslator);
		}

		@Override
		public void setProperty(PersistentProperty property, @Nullable Object value) {
			delegate.setProperty(translate(property), value);
		}

		@Override
		public Object getProperty(PersistentProperty<?> property) {
			return delegate.getProperty(translate(property));
		}

		@Override
		public T getBean() {
			return delegate.getBean();
		}

		private ScyllaPersistentProperty translate(PersistentProperty<?> property) {
			return propertyTranslator.translate((ScyllaPersistentProperty) property);
		}
	}

	static class TranslatingScyllaValueProvider implements ScyllaValueProvider {

		private final PersistentPropertyTranslator translator;
		private final ScyllaValueProvider delegate;

		public TranslatingScyllaValueProvider(PersistentPropertyTranslator translator, ScyllaValueProvider delegate) {
			this.translator = translator;
			this.delegate = delegate;
		}

		@Override
		public boolean hasProperty(ScyllaPersistentProperty property) {
			return delegate.hasProperty(translator.translate(property));
		}

		@Nullable
		@Override
		public <T> T getPropertyValue(ScyllaPersistentProperty property) {
			return delegate.getPropertyValue(translator.translate(property));
		}

		@Override
		public Object getSource() {
			return delegate.getSource();
		}

	}

	class ProjectingConversionContext extends ConversionContext {

		private final EntityProjection<?, ?> projection;

		public ProjectingConversionContext(CustomConversions conversions, ContainerValueConverter<Row> rowConverter,
				ContainerValueConverter<TupleValue> tupleConverter, ContainerValueConverter<UdtValue> udtConverter,
				ContainerValueConverter<Collection<?>> collectionConverter, ContainerValueConverter<Map<?, ?>> mapConverter,
				ValueConverter<Object> elementConverter, EntityProjection<?, ?> projection) {
			super(conversions, (context, source, typeHint) -> doReadOrProject(context, source, typeHint, projection),
					(context, source, typeHint) -> doReadOrProject(context, source, typeHint, projection),
					(context, source, typeHint) -> doReadOrProject(context, source, typeHint, projection), collectionConverter,
					mapConverter, elementConverter);
			this.projection = projection;
		}

		@Override
		ConversionContext forProperty(String name) {

			EntityProjection<?, ?> property = projection.findProperty(name);
			if (property == null) {
				return new ConversionContext(conversions, MappingScyllaConverter.this::doReadRow,
						MappingScyllaConverter.this::doReadTupleValue, MappingScyllaConverter.this::doReadUdtValue,
						collectionConverter, mapConverter, elementConverter);
			}

			return new ProjectingConversionContext(conversions, rowConverter, tupleConverter, udtConverter,
					collectionConverter, mapConverter, elementConverter, property);
		}
	}

	static class MapPersistentPropertyAccessor implements PersistentPropertyAccessor<Map<String, Object>> {

		Map<String, Object> map = new LinkedHashMap<>();

		@Override
		public void setProperty(PersistentProperty<?> persistentProperty, @Nullable Object o) {
			map.put(persistentProperty.getName(), o);
		}

		@Override
		public Object getProperty(PersistentProperty<?> persistentProperty) {
			return map.get(persistentProperty.getName());
		}

		@Override
		public Map<String, Object> getBean() {
			return map;
		}
	}

}
