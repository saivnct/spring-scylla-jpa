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
package com.giangbb.scylla.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import com.giangbb.scylla.SessionFactory;
import com.giangbb.scylla.core.ScyllaPersistentEntitySchemaCreator;
import com.giangbb.scylla.core.ScyllaPersistentEntitySchemaDropper;
import com.giangbb.scylla.core.convert.SchemaFactory;
import com.giangbb.scylla.core.convert.ScyllaConverter;
import com.giangbb.scylla.core.cql.session.DefaultSessionFactory;
import com.giangbb.scylla.core.mapping.ScyllaMappingContext;

/**
 * Factory to create and configure a Scylla {@link SessionFactory} with support for executing CQL and initializing
 * the database schema (a.k.a. keyspace). This factory bean apply {@link SchemaAction schema actions} such as creating user-defined types and tables.
 *
 * @author Giangbb
 */
public class SessionFactoryFactoryBean extends AbstractFactoryBean<SessionFactory> {
	protected final Log log = LogFactory.getLog(getClass());

	protected static final boolean DEFAULT_CREATE_IF_NOT_EXISTS = false;
	protected static final boolean DEFAULT_DROP_TABLES = false;
	protected static final boolean DEFAULT_DROP_UNUSED_TABLES = false;

	private ScyllaConverter converter;

	private CqlSession session;

	private SchemaFactory schemaFactory;


	private SchemaAction schemaAction = SchemaAction.NONE;

	private boolean suspendLifecycleSchemaRefresh = false;

	public CqlSession getSession() {
		return session;
	}

	public ScyllaConverter getConverter() {
		return converter;
	}

	/**
	 * Set the {@link ScyllaConverter} to use. Schema actions will derive table and user type information from the
	 * {@link ScyllaMappingContext} inside {@code converter}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	public void setConverter(ScyllaConverter converter) {

		Assert.notNull(converter, "ScyllaConverter must not be null");

		this.converter = converter;

		this.schemaFactory = new SchemaFactory(converter);
	}

	/**
	 * Set the {@link SchemaAction}.
	 *
	 * @param schemaAction must not be {@literal null}.
	 */
	public void setSchemaAction(SchemaAction schemaAction) {

		Assert.notNull(schemaAction, "SchemaAction must not be null");

		this.schemaAction = schemaAction;
	}

	/**
	 * Set whether to suspend schema refresh settings during {@link #afterPropertiesSet()} and {@link #destroy()}
	 * lifecycle callbacks. Disabled by default to use schema metadata settings of the session configuration. When enabled
	 * (set to {@code true}), then schema refresh during lifecycle methods is suspended until finishing schema actions to
	 * avoid periodic schema refreshes for each DDL statement.
	 * <p>
	 * Suspending schema refresh can be useful to delay schema agreement until the entire schema is created. Note that
	 * disabling schema refresh may interfere with schema actions. {@link SchemaAction#RECREATE_DROP_UNUSED} and
	 * mapping-based schema creation rely on schema metadata.
	 *
	 * @param suspendLifecycleSchemaRefresh {@code true} to suspend the schema refresh during lifecycle callbacks;
	 *          {@code false} otherwise to retain the session schema refresh configuration.
	 */
	public void setSuspendLifecycleSchemaRefresh(boolean suspendLifecycleSchemaRefresh) {
		this.suspendLifecycleSchemaRefresh = suspendLifecycleSchemaRefresh;
	}

	/**
	 * Set the {@link CqlSession} to use.
	 *
	 * @param session must not be {@literal null}.
	 */
	public void setSession(CqlSession session) {

		Assert.notNull(session, "Session must not be null");

		this.session = session;
	}

	@Override
	@SuppressWarnings("all")
	public void afterPropertiesSet() throws Exception {

		Assert.state(this.session != null, "Session was not properly initialized");
		Assert.state(this.converter != null, "Converter was not properly initialized");

		super.afterPropertiesSet();

		Runnable schemaActionRunnable = () -> {
			performSchemaAction();
		};

		if (this.suspendLifecycleSchemaRefresh) {
			SchemaUtils.withSuspendedSchemaRefresh(this.session, schemaActionRunnable);
		} else {
			SchemaUtils.withSchemaRefresh(this.session, schemaActionRunnable);
		}
	}

	@Override
	protected SessionFactory createInstance() {
		return new DefaultSessionFactory(this.session);
	}

	@Override
	@SuppressWarnings("all")
	public void destroy() throws Exception {

		Runnable schemaActionRunnable = () -> {
			// no-op
		};

		if (suspendLifecycleSchemaRefresh) {
			SchemaUtils.withSuspendedAsyncSchemaRefresh(this.session, schemaActionRunnable);
		} else {
			schemaActionRunnable.run();
		}
	}

	@Nullable
	@Override
	public Class<?> getObjectType() {
		return SessionFactory.class;
	}

	/**
	 * Perform the configured {@link SchemaAction} using {@link ScyllaMappingContext} metadata.
	 */
	protected void performSchemaAction() {

		boolean create = false;
		boolean drop = DEFAULT_DROP_TABLES;
		boolean dropUnused = DEFAULT_DROP_UNUSED_TABLES;
		boolean ifNotExists = DEFAULT_CREATE_IF_NOT_EXISTS;

		switch (this.schemaAction) {
			case RECREATE_DROP_UNUSED:
				dropUnused = true;
			case RECREATE:
				drop = true;
			case CREATE_IF_NOT_EXISTS:
				ifNotExists = SchemaAction.CREATE_IF_NOT_EXISTS.equals(this.schemaAction);
			case CREATE:
				create = true;
			case NONE:
			default:
				// do nothing
		}

		if (create) {
			createTables(drop, dropUnused, ifNotExists);
		}

		registerEnumeratedTypes();
	}

	/**
	 * Perform schema actions.
	 *
	 * @param drop {@literal true} to drop types/tables.
	 * @param dropUnused {@literal true} to drop unused types/tables (i.e. types/tables not know to be used by
	 *          {@link ScyllaMappingContext}).
	 * @param ifNotExists {@literal true} to perform creations fail-safe by adding {@code IF NOT EXISTS} to each creation
	 *          statement.
	 */
	protected void createTables(boolean drop, boolean dropUnused, boolean ifNotExists) {
		performSchemaActions(drop, dropUnused, ifNotExists);
	}

	@SuppressWarnings("all")
	private void performSchemaActions(boolean drop, boolean dropUnused, boolean ifNotExists) {
		ScyllaPersistentEntitySchemaCreator schemaCreator = new ScyllaPersistentEntitySchemaCreator(this.session, this.converter.getMappingContext(), this.schemaFactory);

		if (drop) {
			ScyllaPersistentEntitySchemaDropper schemaDropper = new ScyllaPersistentEntitySchemaDropper(this.session, this.converter.getMappingContext());
			schemaDropper.dropTables(dropUnused);
			schemaDropper.dropUserTypes(dropUnused);
		}

		schemaCreator.createUserTypes(ifNotExists);
		schemaCreator.createTables(ifNotExists);
		schemaCreator.createIndexes(ifNotExists);
	}


	private void registerEnumeratedTypes() {
		MutableCodecRegistry registry = (MutableCodecRegistry) converter.getCodecRegistry();

		converter.getMappingContext().getPersistentEntities().forEach(persistentEntity -> {
			persistentEntity.forEach(property -> {
				Class<?> type = property.getType();
				if (type.isEnum()){
					if (this.log.isInfoEnabled()) {
						this.log.info(String.format("register enum codec for %s", type.getName()));
					}

					Class<? extends Enum> enumClass = type.asSubclass(Enum.class);
					TypeCodec<Enum<?>> enumCodec = ExtraTypeCodecs.enumNamesOf(enumClass);

					registry.register(enumCodec);
				}
			});

		});

	}
}
