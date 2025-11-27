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
package com.giangbb.scylla.core;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import com.giangbb.scylla.core.convert.SchemaFactory;
import com.giangbb.scylla.core.cql.generator.CreateIndexCqlGenerator;
import com.giangbb.scylla.core.cql.generator.CreateTableCqlGenerator;
import com.giangbb.scylla.core.cql.generator.CreateUserTypeCqlGenerator;
import com.giangbb.scylla.core.cql.keyspace.CreateIndexSpecification;
import com.giangbb.scylla.core.cql.keyspace.CreateTableSpecification;
import com.giangbb.scylla.core.cql.keyspace.CreateUserTypeSpecification;
import com.giangbb.scylla.core.mapping.*;
import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Schema creation support for Scylla based on {@link ScyllaMappingContext} and {@link ScyllaPersistentEntity}.
 * This class generates CQL to create user types (UDT) and tables.
 *
 * @author Giangbb
 * @see com.giangbb.scylla.core.mapping.Table
 * @see com.giangbb.scylla.core.mapping.UserDefinedType
 * @see ScyllaType
 */
public class ScyllaPersistentEntitySchemaCreator {
	protected final Log log = LogFactory.getLog(getClass());

	private final ScyllaMappingContext mappingContext;
	private final SchemaFactory schemaFactory;
	private final CqlSession cqlSession;

	/**
	 * Create a new {@link ScyllaPersistentEntitySchemaCreator} for the given  {@link CqlSession} and {@link ScyllaMappingContext} and {@link SchemaFactory}
	 *
	 * @param cqlSession must not be {@literal null}.
	 * @param scyllaMappingContext must not be {@literal null}.
	 * @param schemaFactory must not be {@literal null}.
	 */
	public ScyllaPersistentEntitySchemaCreator(CqlSession cqlSession, ScyllaMappingContext scyllaMappingContext, SchemaFactory schemaFactory) {
		Assert.notNull(cqlSession, "CqlSession must not be null");
		Assert.notNull(scyllaMappingContext, "ScyllaMappingContext must not be null");
		Assert.notNull(schemaFactory, "SchemaFactory must not be null");
		this.cqlSession = cqlSession;
		this.mappingContext = scyllaMappingContext;
		this.schemaFactory = schemaFactory;
	}


	/**
	 * Create tables from types known to {@link ScyllaMappingContext}.
	 *
	 * @param ifNotExists {@literal true} to create tables using {@code IF NOT EXISTS}.
	 */
	public void createTables(boolean ifNotExists) {

		createTableSpecifications(ifNotExists).stream()
				.map(createTableSpecification -> {
//					if (this.log.isInfoEnabled()) {
//						this.log.info(String.format("CreateTableSpecification for %s", createTableSpecification.getName().toString()));
//					}
					return CreateTableCqlGenerator.toCql(createTableSpecification);
				})
				.forEach(cql -> {
//					if (this.log.isInfoEnabled()) {
//						this.log.info(String.format("CreateTableSpecification cql: %s", cql));
//					}

					PreparedStatement preparedCreateTable = this.cqlSession.prepare(cql);
					this.cqlSession.execute(preparedCreateTable.bind());
				});
	}

	/**
	 * Create {@link List} of {@link CreateTableSpecification}.
	 *
	 * @param ifNotExists {@literal true} to create tables using {@code IF NOT EXISTS}.
	 * @return {@link List} of {@link CreateTableSpecification}.
	 */
	public List<CreateTableSpecification> createTableSpecifications(boolean ifNotExists) {

		return this.mappingContext.getTableEntities() //
				.stream() //
				.map(entity -> this.schemaFactory.getCreateTableSpecificationFor(entity).ifNotExists(ifNotExists)) //
				.collect(Collectors.toList());
	}

	/**
	 * Create indexes from types known to {@link ScyllaMappingContext}.
	 *
	 * @param ifNotExists {@literal true} to create tables using {@code IF NOT EXISTS}.
	 */
	public void createIndexes(boolean ifNotExists) {

		createIndexSpecifications(ifNotExists).stream()
				.map(createIndexSpecification -> {
//					if (this.log.isInfoEnabled()) {
//						this.log.info(String.format("CreateIndexSpecification for %s", createIndexSpecification.getColumnName().toString()));
//					}
					return CreateIndexCqlGenerator.toCql(createIndexSpecification);
				})
				.forEach(cql -> {
//					if (this.log.isInfoEnabled()) {
//						this.log.info(String.format("CreateIndexSpecification cql: %s", cql));
//					}

					PreparedStatement preparedCreateIndex = this.cqlSession.prepare(cql);
					this.cqlSession.execute(preparedCreateIndex.bind());
				});
	}

	/**
	 * Create {@link List} of {@link CreateIndexSpecification}.
	 *
	 * @param ifNotExists {@literal true} to create indexes using {@code IF NOT EXISTS}.
	 * @return {@link List} of {@link CreateIndexSpecification}.
	 */
	public List<CreateIndexSpecification> createIndexSpecifications(boolean ifNotExists) {

		return this.mappingContext.getTableEntities() //
				.stream() //
				.flatMap(entity -> this.schemaFactory.getCreateIndexSpecificationsFor(entity).stream()) //
				.peek(it -> it.ifNotExists(ifNotExists)) //
				.collect(Collectors.toList());
	}

	/**
	 * Create user types from types known to {@link ScyllaMappingContext}.
	 *
	 * @param ifNotExists {@literal true} to create types using {@code IF NOT EXISTS}.
	 */
	public void createUserTypes(boolean ifNotExists) {

		createUserTypeSpecifications(ifNotExists).stream()
				.map(createUserTypeSpecification -> {
					if (this.log.isInfoEnabled()) {
						this.log.info(String.format("CreateUserTypeSpecification for %s", createUserTypeSpecification.getName().toString()));
					}
					return CreateUserTypeCqlGenerator.toCql(createUserTypeSpecification);
				})
				.forEach(cql -> {
//					if (this.log.isInfoEnabled()) {
//						this.log.info(String.format("CreateUserTypeSpecification cql: %s", cql));
//					}

					PreparedStatement preparedCreateUDT = this.cqlSession.prepare(cql);
					this.cqlSession.execute(preparedCreateUDT.bind());
				});
	}

	/**
	 * Create {@link List} of {@link CreateUserTypeSpecification}.
	 *
	 * @param ifNotExists {@literal true} to create types using {@code IF NOT EXISTS}.
	 * @return {@link List} of {@link CreateUserTypeSpecification}.
	 */
	public List<CreateUserTypeSpecification> createUserTypeSpecifications(boolean ifNotExists) {
		if (this.log.isInfoEnabled()) {
			this.log.info("Collecting createUserTypeSpecifications ...");
		}

		List<? extends ScyllaPersistentEntity<?>> entities = new ArrayList<>(
				this.mappingContext.getUserDefinedTypeEntities());

		Map<CqlIdentifier, ScyllaPersistentEntity<?>> byTableName = entities.stream()
				.collect(Collectors.toMap(ScyllaPersistentEntity::getTableName, entity -> entity));

		UserDefinedTypeSet udts = new UserDefinedTypeSet();

		entities.forEach(entity -> {
			udts.add(entity.getTableName());
			visitUserTypes(entity, udts);
		});


		List<CqlIdentifier> orderedUdtCqlIdentifier = udts.getCreationCqlIdentifierOrdered();

		List<CreateUserTypeSpecification> specifications = orderedUdtCqlIdentifier.stream().map(identifier -> this.schemaFactory.getCreateUserTypeSpecificationFor(byTableName.get(identifier)).ifNotExists(ifNotExists)).toList();

		if (this.log.isInfoEnabled()) {
			this.log.info(String.format("Collected %d createUserTypeSpecifications: %s", specifications.size(), specifications.stream().map(s -> s.getName().toString()).toList()));
		}

		return specifications;
	}

	private void visitUserTypes(ScyllaPersistentEntity<?> entity, UserDefinedTypeSet udts) {

		for (ScyllaPersistentProperty property : entity) {

			BasicScyllaPersistentEntity<?> propertyType = this.mappingContext.getPersistentEntity(property);

			if (propertyType == null) {
				continue;
			}

			if (propertyType.isUserDefinedType()) {
				if (udts.add(propertyType.getTableName())) {
					visitUserTypes(propertyType, udts);
				}
				udts.addDependency(entity.getTableName(), propertyType.getTableName());
			}
		}
	}

	/**
	 * Object to record dependencies and report them in the order of creation.
	 */
	static class UserDefinedTypeSet{

		private final Set<CqlIdentifier> seen = new HashSet<>();
		private final List<DependencyNode> creationOrder = new ArrayList<>();

		public boolean add(CqlIdentifier cqlIdentifier) {

			if (seen.add(cqlIdentifier)) {
				creationOrder.add(new DependencyNode(cqlIdentifier));
				return true;
			}

			return false;
		}

		public DependencyNode getDependencyNode(CqlIdentifier cqlIdentifier){
			for (DependencyNode node : creationOrder) {
				if (node.matches(cqlIdentifier)) {
					return node;
				}
			}
			return null;
		}

		public List<CqlIdentifier> getCreationCqlIdentifierOrdered(){
			List<CqlIdentifier> cqlIdentifiersOrdered = creationOrder.stream() //
					.sorted((left, right) -> {
						if (left.dependsOn(right.getIdentifier())) {
							return 1;
						}else if (right.dependsOn(left.getIdentifier())) {
							return -1;
						}
						return 0;
					}) //
					.map(DependencyNode::getIdentifier).toList();

			Set<String> seenCheck = new HashSet<>();
			for (CqlIdentifier cqlIdentifier : cqlIdentifiersOrdered) {
				DependencyNode node = getDependencyNode(cqlIdentifier);
				if (node == null) {
					throw new IllegalStateException("DependencyNode not found after ordering List CqlIdentifiers for: " + cqlIdentifier);
				}
				for (CqlIdentifier dependency : node.dependsOn) {
					if (!seenCheck.contains(dependency.toString())) {
						throw new IllegalStateException("Dependency ordering failed for: " + cqlIdentifier + " depends on " + dependency);
					}
				}
				seenCheck.add(cqlIdentifier.toString());
			}
			return cqlIdentifiersOrdered;
		}

		/**
		 * Updates the dependency order.
		 *
		 * @param typeToCreate the client of {@code dependsOn}.
		 * @param dependsOn the dependency required by {@code typeToCreate}.
		 */
		void addDependency(CqlIdentifier typeToCreate, CqlIdentifier dependsOn) {

			for (DependencyNode toCreate : creationOrder) {
				if (toCreate.matches(typeToCreate)) {
					toCreate.addDependency(dependsOn);
				}
			}
		}

		static class DependencyNode {

			private final CqlIdentifier identifier;
			private final List<CqlIdentifier> dependsOn = new ArrayList<>();

			DependencyNode(CqlIdentifier identifier) {
				this.identifier = identifier;
			}

			public CqlIdentifier getIdentifier() {
				return identifier;
			}

			boolean matches(CqlIdentifier typeToCreate) {
				return identifier.equals(typeToCreate);
			}

			void addDependency(CqlIdentifier dependsOn) {
				this.dependsOn.add(dependsOn);
			}

			boolean dependsOn(CqlIdentifier identifier) {
				return this.dependsOn.contains(identifier);
			}
		}
	}
}
