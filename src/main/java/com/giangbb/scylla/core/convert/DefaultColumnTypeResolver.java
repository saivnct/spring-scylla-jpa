/*
 * Copyright 2020-2024 the original author or authors.
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

import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.AnnotatedType;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import com.giangbb.scylla.core.mapping.ScyllaPersistentEntity;
import com.giangbb.scylla.core.mapping.ScyllaPersistentProperty;
import com.giangbb.scylla.core.mapping.ScyllaSimpleTypeHolder;
import com.giangbb.scylla.core.mapping.ScyllaType;
import com.giangbb.scylla.core.mapping.ScyllaType.Name;
import com.giangbb.scylla.core.mapping.Frozen;
import com.giangbb.scylla.core.mapping.UserTypeResolver;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.PropertyValueConversions;
import org.springframework.data.convert.PropertyValueConverter;
import org.springframework.data.convert.ValueConversionContext;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

/**
 * Default {@link ColumnTypeResolver} implementation backed by {@link CustomConversions} and {@link CodecRegistry}.
 *
 * @author Giangbb
 */
class DefaultColumnTypeResolver implements ColumnTypeResolver {

	private final Log log = LogFactory.getLog(getClass());

	private final MappingContext<? extends ScyllaPersistentEntity<?>, ? extends ScyllaPersistentProperty> mappingContext;
	private final UserTypeResolver userTypeResolver;
	private final Supplier<CodecRegistry> codecRegistry;
	private final Supplier<CustomConversions> customConversions;
	private final Map<ScyllaPersistentProperty, ScyllaColumnType> columnTypeCache = new ConcurrentHashMap<>();
	private final Map<TypeInformation<?>, ScyllaColumnType> typeInformationColumnTypeCache = new ConcurrentHashMap<>();

	/**
	 * Creates a new {@link DefaultColumnTypeResolver}.
	 *
	 * @param mappingContext
	 * @param userTypeResolver
	 * @param codecRegistry
	 * @param customConversions
	 */
	public DefaultColumnTypeResolver(
			MappingContext<? extends ScyllaPersistentEntity<?>, ? extends ScyllaPersistentProperty> mappingContext,
			UserTypeResolver userTypeResolver, Supplier<CodecRegistry> codecRegistry,
			Supplier<CustomConversions> customConversions) {
		this.mappingContext = mappingContext;
		this.userTypeResolver = userTypeResolver;
		this.codecRegistry = codecRegistry;
		this.customConversions = customConversions;
	}

	@Override
	public ScyllaColumnType resolve(ScyllaPersistentProperty property) {

		Assert.notNull(property, "Property must not be null");

		ScyllaColumnType scyllaColumnType = columnTypeCache.get(property);
		if (scyllaColumnType == null) {
			// avoid recursive update
			scyllaColumnType = doResolve(property);
			columnTypeCache.put(property, scyllaColumnType);
		}

		return scyllaColumnType;
	}

	private ScyllaColumnType doResolve(ScyllaPersistentProperty property) {

		if (property.isAnnotationPresent(ScyllaType.class)) {

			ScyllaType annotation = property.getRequiredAnnotation(ScyllaType.class);

			if (annotation.type() == Name.UDT && ObjectUtils.isEmpty(annotation.userTypeName())) {
				throw new InvalidDataAccessApiUsageException(
						String.format("Expected user type name in property ['%s'] of type ['%s'] in entity [%s]",
								property.getName(), property.getType(), property.getOwner().getName()));
			}

			if ((annotation.type() == Name.LIST || annotation.type() == Name.SET) && annotation.typeArguments().length != 1) {

				throw new InvalidDataAccessApiUsageException(String.format(
						"Expected [%d] type arguments for property ['%s'] of type ['%s'] in entity [%s]; actual was [%d]", 1,
						property.getName(), property.getType(), property.getOwner().getName(), annotation.typeArguments().length));
			}

			if (annotation.type() == Name.MAP && annotation.typeArguments().length != 2) {

				throw new InvalidDataAccessApiUsageException(String.format(
						"Expected [%d] type arguments for property ['%s'] of type ['%s'] in entity [%s]; actual was [%d]", 2,
						property.getName(), property.getType(), property.getOwner().getName(), annotation.typeArguments().length));
			}

			return resolve(annotation);
		}

		PropertyValueConversions pvc = customConversions.get().getPropertyValueConversions();
		TypeInformation<?> typeInformation = property.getTypeInformation();

		if (pvc != null && pvc.hasValueConverter(property)) {

			PropertyValueConverter<Object, Object, ValueConversionContext<ScyllaPersistentProperty>> converter = pvc
					.getValueConverter(property);
			ResolvableType resolvableType = ResolvableType.forClass(converter.getClass());
			ResolvableType storeType = resolvableType.as(PropertyValueConverter.class).getGeneric(1);
			Class<?> storeTypeClass = storeType.resolve();

			if (storeTypeClass == Object.class || storeTypeClass == null) {

				if (log.isDebugEnabled()) {
					log.debug(String.format(
							"PropertyValueConverter %s for Property %s.%s resolves to Object.class. Falling back to the property type %s.",
							converter, property.getOwner().getName(), property.getName(), typeInformation));
				}
			} else {
				typeInformation = TypeInformation.of(storeType);
			}
		}

		return resolve(typeInformation, getFrozenInfo(property));
	}

	private FrozenIndicator getFrozenInfo(ScyllaPersistentProperty property) {

		AnnotatedType annotatedType = property.findAnnotatedType(Frozen.class);

		if (annotatedType == null) {
			return FrozenIndicator.NOT_FROZEN;
		}

		return getFrozenIndicator(annotatedType);
	}

	private FrozenIndicator getFrozenIndicator(AnnotatedType annotatedType) {

		FrozenIndicator frozen = FrozenIndicator.frozen(isFrozen(annotatedType));

		if (annotatedType instanceof AnnotatedParameterizedType) {

			AnnotatedParameterizedType apt = (AnnotatedParameterizedType) annotatedType;
			AnnotatedType[] annotatedTypes = apt.getAnnotatedActualTypeArguments();

			for (AnnotatedType type : annotatedTypes) {
				frozen.addNested(getFrozenIndicator(type));
			}
		}

		return frozen;
	}

	private boolean isFrozen(AnnotatedType type) {
		return AnnotatedElementUtils.hasAnnotation(type, Frozen.class);
	}

	@Override
	public ScyllaColumnType resolve(TypeInformation<?> typeInformation) {

		Assert.notNull(typeInformation, "TypeInformation must not be null");

		ScyllaColumnType scyllaColumnType = typeInformationColumnTypeCache.get(typeInformation);
		if (scyllaColumnType == null) {
			// avoid recursive update
			scyllaColumnType = resolve(typeInformation, FrozenIndicator.NOT_FROZEN);
			typeInformationColumnTypeCache.put(typeInformation, scyllaColumnType);
		}

		return scyllaColumnType;
	}

	private ScyllaColumnType resolve(TypeInformation<?> typeInformation, FrozenIndicator frozen) {

		return getCustomWriteTarget(typeInformation)
				.map(it -> createScyllaTypeDescriptor(tryResolve(it), TypeInformation.of(it)))
				.orElseGet(() -> typeInformation.getType().isEnum() ? ColumnType.create(String.class, DataTypes.TEXT)
						: createScyllaTypeDescriptor(typeInformation, frozen));
	}

	private Optional<Class<?>> getCustomWriteTarget(TypeInformation<?> typeInformation) {
		return customConversions.get().getCustomWriteTarget(typeInformation.getType());
	}

	@Nullable
	private DataType tryResolve(Class<?> type) {

		if (TupleValue.class.isAssignableFrom(type)) {
			return null;
		}

		if (UdtValue.class.isAssignableFrom(type)) {
			return null;
		}

		try {
			return getCodecRegistry().codecFor(type).getCqlType();
		} catch (CodecNotFoundException cause) {
			if (log.isDebugEnabled()) {
				log.debug(String.format("Cannot resolve Codec for %s", type.getName()), cause);
			}
			return null;
		}
	}

	@Override
	public ScyllaColumnType resolve(ScyllaType annotation) {

		Name type = annotation.type();

		switch (type) {
			case MAP:
				assertTypeArguments(annotation.typeArguments().length, 2);

				ScyllaColumnType keyType = createScyllaTypeDescriptor(getRequiredDataType(annotation, 0));
				ScyllaColumnType valueType = createScyllaTypeDescriptor(getRequiredDataType(annotation, 1));

				return ColumnType.mapOf(keyType, valueType);

			case LIST:
			case SET:
				assertTypeArguments(annotation.typeArguments().length, 1);

				DataType componentType = getRequiredDataType(annotation, 0);

				if (type == Name.SET) {
					return ColumnType.setOf(createScyllaTypeDescriptor(componentType));
				}

				return ColumnType.listOf(createScyllaTypeDescriptor(componentType));

			case TUPLE:

				DataType[] dataTypes = Arrays.stream(annotation.typeArguments()).map(ScyllaSimpleTypeHolder::getDataTypeFor)
						.toArray(DataType[]::new);

				return ColumnType.tupleOf(DataTypes.tupleOf(dataTypes));
			case UDT:

				if (ObjectUtils.isEmpty(annotation.userTypeName())) {
					throw new InvalidDataAccessApiUsageException(
							"Cannot resolve user type for @ScyllaType(type=UDT) without userTypeName");
				}

				return createScyllaTypeDescriptor(getUserType(annotation.userTypeName()));
			default:
				return createScyllaTypeDescriptor(ScyllaSimpleTypeHolder.getRequiredDataTypeFor(type));
		}
	}

	@Override
	public ColumnType resolve(@Nullable Object value) {

		if (value != null) {

			TypeInformation<?> typeInformation = TypeInformation.of(value.getClass());

			return getCustomWriteTarget(typeInformation).map(it -> {
				return (ColumnType) createScyllaTypeDescriptor(tryResolve(it), typeInformation);
			}).orElseGet(() -> {

				if (typeInformation.getType().isEnum()) {
					return ColumnType.create(String.class, DataTypes.TEXT);
				}

				if (value instanceof Map) {
					return ColumnType.mapOf(DefaultColumnType.OBJECT, DefaultColumnType.OBJECT);
				}

				if (value instanceof List) {
					return ColumnType.listOf(DefaultColumnType.OBJECT);
				}

				if (value instanceof Set) {
					return ColumnType.setOf(DefaultColumnType.OBJECT);
				}

				if (value instanceof UdtValue) {
					return ColumnType.udtOf(((UdtValue) value).getType());
				}

				if (value instanceof TupleValue) {
					return ColumnType.tupleOf(((TupleValue) value).getType());
				}

				ScyllaPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(typeInformation);

				if (persistentEntity != null) {

					if (persistentEntity.isUserDefinedType() || persistentEntity.isTupleType()) {
						return resolve(persistentEntity.getTypeInformation());
					}
				}

				return ColumnType.create(typeInformation.getType());
			});
		}

		return DefaultColumnType.OBJECT;
	}

	private ScyllaColumnType createScyllaTypeDescriptor(DataType dataType) {

		GenericType<Object> javaType = getCodecRegistry().codecFor(dataType).getJavaType();
		return ColumnType.create(javaType.getRawType(), dataType);
	}

	private ScyllaColumnType createScyllaTypeDescriptor(@Nullable DataType dataType,
														TypeInformation<?> typeInformation) {

		if (typeInformation.isCollectionLike() || typeInformation.isMap()) {

			if (dataType instanceof ListType) {

				TypeInformation<?> component = typeInformation.getComponentType();
				DataType elementType = ((ListType) dataType).getElementType();

				if (component != null) {
					return ColumnType.listOf(createScyllaTypeDescriptor(elementType, component));
				}

				Class<?> componentType = resolveToJavaType(elementType);
				return ColumnType.listOf(ColumnType.create(componentType, elementType));
			}

			if (dataType instanceof SetType) {

				TypeInformation<?> component = typeInformation.getComponentType();
				DataType elementType = ((SetType) dataType).getElementType();

				if (component != null) {
					return ColumnType.setOf(createScyllaTypeDescriptor(elementType, component));
				}

				Class<?> componentType = resolveToJavaType(elementType);
				return ColumnType.setOf(ColumnType.create(componentType, elementType));
			}

			if (dataType instanceof MapType) {

				TypeInformation<?> mapKeyType = typeInformation.getComponentType();
				TypeInformation<?> mapValueType = typeInformation.getMapValueType();

				MapType mapType = (MapType) dataType;

				ScyllaColumnType keyDescriptor = null;
				ScyllaColumnType valueDescriptor = null;

				if (mapKeyType != null) {
					keyDescriptor = createScyllaTypeDescriptor(mapType.getKeyType(), mapKeyType);
				}

				if (mapValueType != null) {
					valueDescriptor = createScyllaTypeDescriptor(mapType.getValueType(), mapValueType);
				}

				if (keyDescriptor == null) {
					keyDescriptor = ColumnType.create(resolveToJavaType(mapType.getKeyType()), mapType.getKeyType());
				}

				if (valueDescriptor == null) {
					valueDescriptor = ColumnType.create(resolveToJavaType(mapType.getValueType()), mapType.getValueType());
				}

				return ColumnType.mapOf(keyDescriptor, valueDescriptor);
			}
		}

		if (dataType == null) {
			return new UnresolvableScyllaType(typeInformation);
		}

		return new DefaultScyllaColumnType(typeInformation, dataType);
	}

	private ScyllaColumnType createScyllaTypeDescriptor(TypeInformation<?> typeInformation,
														FrozenIndicator frozen) {

		if (List.class.isAssignableFrom(typeInformation.getType())) {
			return ColumnType.listOf(resolve(typeInformation.getRequiredComponentType(), frozen.getFrozen(0)),
					frozen.isFrozen());
		}

		if (Set.class.isAssignableFrom(typeInformation.getType())) {
			return ColumnType.setOf(resolve(typeInformation.getRequiredComponentType(), frozen.getFrozen(0)),
					frozen.isFrozen());
		}

		if (typeInformation.isMap()) {

			FrozenIndicator frozenKey = frozen.getFrozen(0);
			FrozenIndicator frozenValue = frozen.getFrozen(1);

			return ColumnType.mapOf(resolve(typeInformation.getRequiredComponentType(), frozenKey),
					resolve(typeInformation.getRequiredMapValueType(), frozenValue), frozen.isFrozen());
		}

		ScyllaPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(typeInformation);

		if (persistentEntity != null) {

			if (persistentEntity.isUserDefinedType()) {
				return new DefaultScyllaColumnType(typeInformation, getUserType(persistentEntity, frozen.isFrozen()));
			}

			if (persistentEntity.isTupleType()) {

				DataType[] componentTypes = StreamSupport.stream(persistentEntity.spliterator(), false) //
						.map(this::resolve) //
						.map(ScyllaColumnType::getDataType) //
						.toArray(DataType[]::new);

				return new DefaultScyllaColumnType(typeInformation, DataTypes.tupleOf(componentTypes));
			}

			return new UnresolvableScyllaType(typeInformation);
		}

		DataType dataType = tryResolve(typeInformation.getType());

		return dataType == null ? new UnresolvableScyllaType(typeInformation)
				: new DefaultScyllaColumnType(typeInformation, dataType);
	}

	private DataType getRequiredDataType(ScyllaType annotation, int typeIndex) {

		Name typeName = annotation.typeArguments()[typeIndex];
		return typeName == Name.UDT ? getUserType(annotation.userTypeName())
				: ScyllaSimpleTypeHolder.getRequiredDataTypeFor(typeName);
	}

	private Class<?> resolveToJavaType(DataType dataType) {
		TypeCodec<Object> codec = getCodecRegistry().codecFor(dataType);
		return codec.getJavaType().getRawType();
	}

	private CodecRegistry getCodecRegistry() {
		return codecRegistry.get();
	}

	private UserDefinedType getUserType(ScyllaPersistentEntity<?> persistentEntity, boolean frozen) {
		return getUserType(persistentEntity.getTableName()).copy(frozen);
	}

	private UserDefinedType getUserType(String userTypeName) {
		return getUserType(CqlIdentifier.fromCql(userTypeName));
	}

	private UserDefinedType getUserType(CqlIdentifier userTypeName) {

		UserDefinedType type = userTypeResolver.resolveType(userTypeName);

		if (type == null) {
			throw new MappingException(String.format("User type [%s] not found", userTypeName));
		}

		return type;
	}

	private static void assertTypeArguments(int args, int expected) {

		if (args != expected) {
			throw new InvalidDataAccessApiUsageException(
					String.format("Expected [%d] type arguments actual was [%d]", expected, args));
		}
	}

	static class UnresolvableScyllaType extends DefaultScyllaColumnType {

		public UnresolvableScyllaType(TypeInformation<?> type, ColumnType... parameters) {
			super(type, Lazy.empty(), parameters);
		}

		@Override
		public DataType getDataType() {
			throw new MappingException(String.format("Cannot resolve DataType for %s", getType().getName()));
		}

		@Override
		public boolean isTupleType() {
			return false;
		}

		@Override
		public boolean isUserDefinedType() {
			return false;
		}
	}

}
