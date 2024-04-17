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
package com.giangbb.scylla.core;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import com.giangbb.scylla.core.cql.generator.DropTableCqlGenerator;
import com.giangbb.scylla.core.cql.generator.DropUserTypeCqlGenerator;
import com.giangbb.scylla.core.cql.keyspace.DropTableSpecification;
import com.giangbb.scylla.core.cql.keyspace.DropUserTypeSpecification;
import com.giangbb.scylla.core.mapping.ScyllaMappingContext;
import com.giangbb.scylla.core.mapping.ScyllaPersistentEntity;
import com.giangbb.scylla.core.mapping.ScyllaType;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Schema drop support for Scylla based on {@link ScyllaMappingContext} and {@link ScyllaPersistentEntity}.
 * This class generates CQL to drop user types (UDT) and tables.
 *
 * @author Giangbb
 * @see com.giangbb.scylla.core.mapping.Table
 * @see com.giangbb.scylla.core.mapping.UserDefinedType
 * @see ScyllaType
 */
public class ScyllaPersistentEntitySchemaDropper {
    protected final Log log = LogFactory.getLog(getClass());

    protected static final boolean DEFAULT_DROP_TABLE_IF_EXISTS = false;

	private final CqlSession cqlSession;

	private final ScyllaMappingContext mappingContext;

	/**
	 * Create a new {@link ScyllaPersistentEntitySchemaDropper} for the given {@link CqlSession} and {@link ScyllaMappingContext}.
	 *
	 * @param cqlSession must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	public ScyllaPersistentEntitySchemaDropper(CqlSession cqlSession, ScyllaMappingContext mappingContext) {

		Assert.notNull(cqlSession, "CqlSession must not be null");
		Assert.notNull(mappingContext, "ScyllaMappingContext must not be null");

		this.cqlSession = cqlSession;
		this.mappingContext = mappingContext;
	}

	private KeyspaceMetadata getKeyspaceMetadata() {
		return this.cqlSession.getKeyspace().flatMap(it -> this.cqlSession.getMetadata().getKeyspace(it)).orElseThrow(() -> {
			return new IllegalStateException("Metadata for keyspace not available");
		});
	}

    private Optional<TableMetadata> getTableMetadata(CqlIdentifier keyspace, CqlIdentifier tableName) {
		Assert.notNull(keyspace, "Keyspace name must not be null");
		Assert.notNull(tableName, "Table name must not be null");

		return this.cqlSession.getMetadata().getKeyspace(keyspace).flatMap(it -> it.getTable(tableName));
	}


	/**
	 * Drop tables that exist in the keyspace.
	 *
	 * @param dropUnused {@literal true} to drop unused tables. Table usage is determined by existing table mappings.
	 */
	public void dropTables(boolean dropUnused) {
		this.getKeyspaceMetadata() //
				.getTables() //
				.values() //
				.stream() //
				.map(RelationMetadata::getName) //
				.filter(table -> dropUnused || this.mappingContext.usesTable(table)) //
				.forEach(this::dropTable);
	}

    private void dropTable(CqlIdentifier tableName) {
        dropTable(DEFAULT_DROP_TABLE_IF_EXISTS, tableName);
    }

    private void dropTable(boolean ifExists, CqlIdentifier tableName) {
		Assert.notNull(tableName, "Type name must not be null");

		if (this.log.isInfoEnabled()) {
			this.log.info(String.format("dropTable for %s", tableName.toString()));
		}


        String dropTableCql = DropTableCqlGenerator.toCql(DropTableSpecification.dropTable(tableName).ifExists(ifExists));
        PreparedStatement preparedDropTable = this.cqlSession.prepare(dropTableCql);
        this.cqlSession.execute(preparedDropTable.bind());
    }

	/**
	 * Drop user types that exist in the keyspace.
	 *
	 * @param dropUnused {@literal true} to drop unused types before creation. Type usage is determined from existing
	 *          mapped {@link com.giangbb.scylla.core.mapping.UserDefinedType}s and UDT names on field
	 *          specifications.
	 */
	public void dropUserTypes(boolean dropUnused) {

		Set<CqlIdentifier> canRecreate = this.mappingContext.getUserDefinedTypeEntities().stream()
				.map(ScyllaPersistentEntity::getTableName).collect(Collectors.toSet());

		Collection<UserDefinedType> userTypes = this.getKeyspaceMetadata().getUserDefinedTypes()
				.values();

		getUserTypesToDrop(userTypes) //
				.stream() //
				.filter(it -> canRecreate.contains(it) || (dropUnused && !mappingContext.usesUserType(it))) //
				.forEach(this::dropUserType);
	}

    private void dropUserType(CqlIdentifier typeName) {
        Assert.notNull(typeName, "Type name must not be null");

		if (this.log.isInfoEnabled()) {
			this.log.info(String.format("dropUserType for %s", typeName.toString()));
		}

        String dropUserTypeCql = DropUserTypeCqlGenerator.toCql(DropUserTypeSpecification.dropType(typeName));
        PreparedStatement preparedDropUserType = this.cqlSession.prepare(dropUserTypeCql);
        this.cqlSession.execute(preparedDropUserType.bind());
    }

	/**
	 * Create {@link List} of {@link CqlIdentifier} with User-Defined type names to drop considering dependencies between
	 * UDTs.
	 *
	 * @return {@link List} of {@link CqlIdentifier}.
	 */
	private List<CqlIdentifier> getUserTypesToDrop(Collection<UserDefinedType> knownUserTypes) {

		List<CqlIdentifier> toDrop = new ArrayList<>();

		UserTypeDependencyGraphBuilder builder = new UserTypeDependencyGraphBuilder();
		knownUserTypes.forEach(builder::addUserType);

		UserTypeDependencyGraph dependencyGraph = builder.build();

		Set<CqlIdentifier> globalSeen = new LinkedHashSet<>();

		knownUserTypes.forEach(userType -> {
			toDrop.addAll(dependencyGraph.getDropOrder(userType.getName(), globalSeen::add));
		});

		return toDrop;
	}

	/**
	 * Builder for {@link UserTypeDependencyGraph}. Introspects {@link UserDefinedType} for dependencies to other user types to
	 * build a dependency graph between user types.
	 *
	 */
	static class UserTypeDependencyGraphBuilder {

		// Maps user types to other types they are referenced in.
		private final MultiValueMap<CqlIdentifier, CqlIdentifier> dependencies = new LinkedMultiValueMap<>();

		/**
		 * Add {@link UserDefinedType} to the builder and inspect its dependencies.
		 *
		 * @param userType must not be {@literal null.}
		 */
		void addUserType(UserDefinedType userType) {

			Set<CqlIdentifier> seen = new LinkedHashSet<>();
			visitTypes(userType, seen::add);
		}

		/**
		 * Build the {@link UserTypeDependencyGraph}.
		 *
		 * @return the {@link UserTypeDependencyGraph}.
		 */
		UserTypeDependencyGraph build() {
			return new UserTypeDependencyGraph(new LinkedMultiValueMap<>(dependencies));
		}

		/**
		 * Visit a {@link UserDefinedType} and its fields.
		 *
		 * @param userType
		 * @param typeFilter
		 */
		private void visitTypes(UserDefinedType userType, Predicate<CqlIdentifier> typeFilter) {

			CqlIdentifier typeName = userType.getName();

			if (!typeFilter.test(typeName)) {
				return;
			}

			for (DataType fieldType : userType.getFieldTypes()) {

				if (fieldType instanceof UserDefinedType) {

					addDependency((UserDefinedType) fieldType, typeName, typeFilter);

					return;
				}

				doWithTypeArguments(fieldType, it -> {

					if (it instanceof UserDefinedType) {
						addDependency((UserDefinedType) it, typeName, typeFilter);
					}
				});
			}
		}

		private void addDependency(UserDefinedType userType, CqlIdentifier requiredBy,
				Predicate<CqlIdentifier> typeFilter) {

			dependencies.add(userType.getName(), requiredBy);

			visitTypes(userType, typeFilter);
		}

		private static void doWithTypeArguments(DataType type, Consumer<DataType> callback) {

			if (type instanceof MapType) {

				MapType mapType = (MapType) type;

				callback.accept(mapType.getKeyType());
				doWithTypeArguments(mapType.getKeyType(), callback);

				callback.accept(mapType.getValueType());
				doWithTypeArguments(mapType.getValueType(), callback);
			}

			if (type instanceof ListType) {

				ListType listType = (ListType) type;

				callback.accept(listType.getElementType());
				doWithTypeArguments(listType.getElementType(), callback);
			}

			if (type instanceof SetType) {

				SetType setType = (SetType) type;

				callback.accept(setType.getElementType());
				doWithTypeArguments(setType.getElementType(), callback);
			}

			if (type instanceof TupleType) {

				TupleType tupleType = (TupleType) type;

				tupleType.getComponentTypes().forEach(nested -> {
					callback.accept(nested);
					doWithTypeArguments(nested, callback);
				});
			}
		}
	}

	/**
	 * Dependency graph representing user type field dependencies to other user types.
	 *
	 */
	static class UserTypeDependencyGraph {

		private final MultiValueMap<CqlIdentifier, CqlIdentifier> dependencies;

		UserTypeDependencyGraph(MultiValueMap<CqlIdentifier, CqlIdentifier> dependencies) {
			this.dependencies = dependencies;
		}

		/**
		 * Returns the names of user types in the order they need to be dropped including type {@code typeName}.
		 *
		 * @param typeName
		 * @param typeFilter
		 * @return
		 */
		List<CqlIdentifier> getDropOrder(CqlIdentifier typeName, Predicate<CqlIdentifier> typeFilter) {

			List<CqlIdentifier> toDrop = new ArrayList<>();

			if (typeFilter.test(typeName)) {

				List<CqlIdentifier> dependants = dependencies.getOrDefault(typeName, Collections.emptyList());
				dependants.stream().map(dependant -> getDropOrder(dependant, typeFilter)).forEach(toDrop::addAll);

				toDrop.add(typeName);
			}

			return toDrop;
		}
	}
}
