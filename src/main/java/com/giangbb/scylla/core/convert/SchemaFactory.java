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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import com.giangbb.scylla.core.cql.keyspace.CreateIndexSpecification;
import com.giangbb.scylla.core.cql.keyspace.CreateTableSpecification;
import com.giangbb.scylla.core.cql.keyspace.CreateUserTypeSpecification;
import com.giangbb.scylla.core.mapping.ScyllaPersistentEntity;
import com.giangbb.scylla.core.mapping.ScyllaPersistentProperty;
import com.giangbb.scylla.core.mapping.ScyllaMappingContext;
import com.giangbb.scylla.core.mapping.UserTypeResolver;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.giangbb.scylla.core.cql.keyspace.CreateTableSpecification.createTable;

/**
 * Factory for Scylla Schema objects such as user-defined types, tables and indexes.
 *
 * @author Giangbb
 * @see CreateUserTypeSpecification
 * @see CreateTableSpecification
 * @see CreateIndexSpecification
 * @see ScyllaMappingContext
 */
public class SchemaFactory {

	private final MappingContext<? extends ScyllaPersistentEntity<?>, ScyllaPersistentProperty> mappingContext;

	private final ColumnTypeResolver typeResolver;


	/**
	 * Creates a new {@link SchemaFactory} given {@link ScyllaConverter}.
	 *
	 * @param converter must not be null.
	 */
	public SchemaFactory(ScyllaConverter converter) {

		Assert.notNull(converter, "ScyllaConverter must not be null");

		this.mappingContext = converter.getMappingContext();
		this.typeResolver = new DefaultColumnTypeResolver(mappingContext, ShallowUserTypeResolver.INSTANCE,
				converter::getCodecRegistry, converter::getCustomConversions);
	}

	/**
	 * Creates a new {@link SchemaFactory} given {@link MappingContext}, {@link CustomConversions} and
	 * {@link CodecRegistry}.
	 *
	 * @param mappingContext must not be null.
	 * @param customConversions must not be null.
	 * @param codecRegistry must not be null.
	 */
	public SchemaFactory(
			MappingContext<? extends ScyllaPersistentEntity<?>, ScyllaPersistentProperty> mappingContext,
			CustomConversions customConversions, CodecRegistry codecRegistry) {

		Assert.notNull(mappingContext, "MappingContext must not be null");
		Assert.notNull(customConversions, "CustomConversions must not be null");
		Assert.notNull(codecRegistry, "CodecRegistry must not be null");

		this.mappingContext = mappingContext;
		this.typeResolver = new DefaultColumnTypeResolver(mappingContext, ShallowUserTypeResolver.INSTANCE,
				() -> codecRegistry, () -> customConversions);
	}

	/**
	 * Returns a {@link CreateTableSpecification} for the given entity, including all mapping information.
	 *
	 * @param entityType must not be {@literal null}.
	 * @return the {@link CreateTableSpecification} derived from {@code entityType}.
	 */
	public CreateTableSpecification getCreateTableSpecificationFor(Class<?> entityType) {

		Assert.notNull(entityType, "Entity type must not be null");

		return getCreateTableSpecificationFor(mappingContext.getRequiredPersistentEntity(entityType));
	}

	/**
	 * Returns a {@link CreateTableSpecification} for the given entity, including all mapping information.
	 *
	 * @param entity must not be {@literal null}.
	 * @return the {@link CreateTableSpecification} derived from {@link ScyllaPersistentEntity}.
	 */
	public CreateTableSpecification getCreateTableSpecificationFor(ScyllaPersistentEntity<?> entity) {

		Assert.notNull(entity, "ScyllaPersistentEntity must not be null");

		return getCreateTableSpecificationFor(entity, entity.getTableName());
	}

	/**
	 * Returns a {@link CreateTableSpecification} for the given entity using {@link CqlIdentifier table name}, including
	 * all mapping information.
	 *
	 * @param entity must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return
	 */
	public CreateTableSpecification getCreateTableSpecificationFor(ScyllaPersistentEntity<?> entity,
																   CqlIdentifier tableName) {

		Assert.notNull(tableName, "Table name must not be null");
		Assert.notNull(entity, "ScyllaPersistentEntity must not be null");

		CreateTableSpecification specification = createTable(tableName);

		for (ScyllaPersistentProperty property : entity) {

			DataType type = UserTypeUtil.potentiallyFreeze(getDataType(property));

			if (property.isPartitionKeyColumn()) {
				specification.partitionKeyColumn(property.getRequiredColumnName(), type);
			} else if (property.isClusterKeyColumn()) {
				specification.clusteredKeyColumn(property.getRequiredColumnName(), type, property.getClusteringKeyOrdering());
			} else if (property.isStaticColumn()) {
				specification.staticColumn(property.getRequiredColumnName(), type);
			} else {
				specification.column(property.getRequiredColumnName(), type);
			}
		}

		if (specification.getPartitionKeyColumns().isEmpty()) {
			throw new MappingException(String.format("No partition key columns found in entity [%s]", entity.getType()));
		}

		return specification;
	}

	private DataType getDataType(ScyllaPersistentProperty property) {

		try {
			return typeResolver.resolve(property).getDataType();
		} catch (MappingException e) {

			throw new MappingException(String.format(
					"Cannot resolve DataType for type [%s] for property [%s] in entity [%s]; Consider registering a Converter or annotating the property with @ScyllaType",
					property.getType(), property.getName(), property.getOwner().getName()), e);
		}
	}

	/**
	 * Returns {@link CreateIndexSpecification index specifications} derived from {@link ScyllaPersistentEntity}.
	 *
	 * @param entityType must not be {@literal null}.
	 * @return the {@link CreateTableSpecification} derived from {@code entityType}.
	 */
	public List<CreateIndexSpecification> getCreateIndexSpecificationsFor(Class<?> entityType) {

		Assert.notNull(entityType, "Entity type must not be null");

		return getCreateIndexSpecificationsFor(mappingContext.getRequiredPersistentEntity(entityType));
	}

	/**
	 * Returns {@link CreateIndexSpecification index specifications} derived from {@link ScyllaPersistentEntity}.
	 *
	 * @param entity must not be {@literal null}.
	 * @return
	 */
	public List<CreateIndexSpecification> getCreateIndexSpecificationsFor(ScyllaPersistentEntity<?> entity) {

		Assert.notNull(entity, "ScyllaPersistentEntity must not be null");

		return getCreateIndexSpecificationsFor(entity, entity.getTableName());
	}

	/**
	 * Returns {@link CreateIndexSpecification index specifications} derived from {@link ScyllaPersistentEntity} using
	 * {@link CqlIdentifier table name}.
	 *
	 * @param entity must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return
	 */
	public List<CreateIndexSpecification> getCreateIndexSpecificationsFor(ScyllaPersistentEntity<?> entity,
																		  CqlIdentifier tableName) {

		Assert.notNull(entity, "ScyllaPersistentEntity must not be null");
		Assert.notNull(tableName, "Table name must not be null");

		List<CreateIndexSpecification> indexes = new ArrayList<>();

		for (ScyllaPersistentProperty property : entity) {
			indexes.addAll(IndexSpecificationFactory.createIndexSpecifications(property));
		}

		indexes.forEach(it -> it.tableName(entity.getTableName()));

		return indexes;
	}

	/**
	 * Returns a {@link CreateUserTypeSpecification} for the given entity, including all mapping information.
	 *
	 * @param entity must not be {@literal null}.
	 */
	public CreateUserTypeSpecification getCreateUserTypeSpecificationFor(ScyllaPersistentEntity<?> entity) {

		Assert.notNull(entity, "ScyllaPersistentEntity must not be null");

		CreateUserTypeSpecification specification = CreateUserTypeSpecification.createType(entity.getTableName());

		for (ScyllaPersistentProperty property : entity) {
			// Use frozen literal to not resolve types from Scylla; At this stage, they might be not created yet.
			specification.field(property.getRequiredColumnName(), UserTypeUtil.potentiallyFreeze(getDataType(property)));
		}

		if (specification.getFields().isEmpty()) {
			throw new MappingException(String.format("No fields in user type [%s]", entity.getType()));
		}

		return specification;
	}

	enum ShallowUserTypeResolver implements UserTypeResolver {
		INSTANCE;

		@Override
		public UserDefinedType resolveType(CqlIdentifier typeName) {
			return new ShallowUserDefinedType(typeName, false);
		}
	}

	static class ShallowUserDefinedType implements UserDefinedType {

		private final CqlIdentifier name;
		private final boolean frozen;

		public ShallowUserDefinedType(String name, boolean frozen) {
			this(CqlIdentifier.fromInternal(name), frozen);
		}

		public ShallowUserDefinedType(CqlIdentifier name, boolean frozen) {
			this.name = name;
			this.frozen = frozen;
		}

		@Override
		public CqlIdentifier getKeyspace() {
			return null;
		}

		@Override
		public CqlIdentifier getName() {
			return name;
		}

		@Override
		public boolean isFrozen() {
			return frozen;
		}

		@Override
		public List<CqlIdentifier> getFieldNames() {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public int firstIndexOf(CqlIdentifier id) {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public int firstIndexOf(String name) {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public List<DataType> getFieldTypes() {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public UserDefinedType copy(boolean newFrozen) {
			return new ShallowUserDefinedType(this.name, newFrozen);
		}

		@Override
		public UdtValue newValue() {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public UdtValue newValue(@edu.umd.cs.findbugs.annotations.NonNull Object... fields) {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public AttachmentPoint getAttachmentPoint() {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public boolean isDetached() {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public void attach(@edu.umd.cs.findbugs.annotations.NonNull AttachmentPoint attachmentPoint) {
			throw new UnsupportedOperationException(
					"This implementation should only be used internally, this is likely a driver bug");
		}

		@Override
		public boolean equals(@Nullable Object o) {
			if (this == o)
				return true;
			if (!(o instanceof UserDefinedType that))
				return false;
			return isFrozen() == that.isFrozen() && Objects.equals(getName(), that.getName());
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, frozen);
		}

		@Override
		public String toString() {
			return "UDT(" + name.asCql(true) + ")";
		}
	}
}
