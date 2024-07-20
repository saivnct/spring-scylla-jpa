/*
 * Copyright 2013-2024 the original author or authors.
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
package com.giangbb.scylla.config;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.lang.Nullable;
import com.giangbb.scylla.ScyllaManagedTypes;
import com.giangbb.scylla.SessionFactory;
import com.giangbb.scylla.core.ScyllaTemplate;
import com.giangbb.scylla.core.convert.MappingScyllaConverter;
import com.giangbb.scylla.core.convert.ScyllaConverter;
import com.giangbb.scylla.core.convert.ScyllaCustomConversions;
import com.giangbb.scylla.core.mapping.ScyllaMappingContext;
import com.giangbb.scylla.core.mapping.SimpleUserTypeResolver;
import com.giangbb.scylla.core.mapping.Table;
import com.giangbb.scylla.core.mapping.UserTypeResolver;
import com.giangbb.scylla.core.cql.session.init.KeyspacePopulator;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

/**
 * Base class for Spring Data Scylla configuration using JavaConfig.
 *
 * @author Giangbb
 */
@Configuration
@SuppressWarnings("unused")
public abstract class AbstractScyllaConfiguration
		extends AbstractSessionConfiguration
		implements BeanClassLoaderAware {

	private @Nullable ClassLoader beanClassLoader;

	/**
	 * Creates a {@link ScyllaConverter} using the configured {@link #scyllaMappingContext(ScyllaManagedTypes)}. Will apply all specified
	 * {@link #customConversions()}.
	 *
	 * @return {@link ScyllaConverter} used to convert Java and Scylla value types during the mapping process.
	 * @see #scyllaMappingContext(ScyllaManagedTypes)
	 * @see #customConversions()
	 */
	@Bean
	public ScyllaConverter scyllaConverter() {
		CqlSession cqlSession = getRequiredSession();

		MappingScyllaConverter converter = new MappingScyllaConverter(
				requireBeanOfType(ScyllaMappingContext.class));

		converter.setCodecRegistry(cqlSession.getContext().getCodecRegistry());
		converter.setUserTypeResolver(userTypeResolver(cqlSession));
		converter.setCustomConversions(requireBeanOfType(ScyllaCustomConversions.class));

		return converter;
	}

	/**
	 * Returns the given {@link ScyllaManagedTypes} object holding the initial entity set.
	 *
	 * @return new instance of {@link ScyllaManagedTypes}.
	 * @throws ClassNotFoundException
	 */
	@Bean
	public ScyllaManagedTypes scyllaManagedTypes() throws ClassNotFoundException {
		return ScyllaManagedTypes.fromIterable(getInitialEntitySet());
	}

	/**
	 * Return the {@link MappingContext} instance to map Entities to {@link Object Java Objects}.
	 *
	 * @see ScyllaMappingContext
	 */
	@Bean
	public ScyllaMappingContext scyllaMappingContext(ScyllaManagedTypes scyllaManagedTypes) {
		CqlSession cqlSession = getRequiredSession();

		ScyllaMappingContext mappingContext = new ScyllaMappingContext(userTypeResolver(cqlSession));

		CustomConversions customConversions = requireBeanOfType(ScyllaCustomConversions.class);

		getBeanClassLoader().ifPresent(mappingContext::setBeanClassLoader);

		mappingContext.setCodecRegistry(cqlSession.getContext().getCodecRegistry());
		mappingContext.setCustomConversions(customConversions);
		mappingContext.setManagedTypes(scyllaManagedTypes);
		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());

		return mappingContext;
	}

	/**
	 * Creates a {@link ScyllaTemplate}.
	 */
	@Bean
	public ScyllaTemplate scyllaTemplate() {
		return new ScyllaTemplate(requireBeanOfType(SessionFactoryFactoryBean.class));
	}

	/**
	 * Creates a {@link SessionFactoryFactoryBean} that provides a {@link SessionFactory}. The lifecycle of
	 * {@link SessionFactoryFactoryBean} initializes the {@link #getSchemaAction() schema} in the
	 * {@link #getKeyspaceName() configured keyspace}.
	 *
	 * @return the {@link SessionFactoryFactoryBean}.
	 * @see #scyllaConverter()
	 * @see #getKeyspaceName()
	 * @see #getSchemaAction()
	 */
	@Bean
	public SessionFactoryFactoryBean scyllaSessionFactory(CqlSession cqlSession) {

		SessionFactoryFactoryBean bean = new SessionFactoryFactoryBean();

		// Initialize the CqlSession reference first since it is required, or must not be null!
		bean.setSession(cqlSession);
		bean.setConverter(getRequiredConverter());
		bean.setSchemaAction(getSchemaAction());

		return bean;
	}

	/**
	 * Returns the initialized {@link CqlSession} instance.
	 *
	 * @return the {@link CqlSession}.
	 * @throws IllegalStateException if the session factory is not initialized.
	 */
	protected SessionFactory getRequiredSessionFactory() {
		return requireBeanOfType(SessionFactory.class);
	}


	/**
	 * Register custom {@link Converter}s in a {@link CustomConversions} object if required. These
	 * {@link CustomConversions} will be registered with the {@link #scyllaConverter()} and {@link #scyllaMappingContext(ScyllaManagedTypes)}
	 * . Returns an empty {@link CustomConversions} instance by default.
	 *
	 * @return must not be {@literal null}.
	 */
	@Bean
	public ScyllaCustomConversions customConversions() {
		return ScyllaCustomConversions.create(config -> {});
	}

	/**
	 * Configures the Java {@link ClassLoader} used to resolve Scylla application entity {@link Class types}.
	 *
	 * @param classLoader Java {@link ClassLoader} used to resolve Scylla application entity {@link Class types}; may
	 *          be {@literal null}.
	 * @see ClassLoader
	 */
	@Override
	public void setBeanClassLoader(@Nullable ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	/**
	 * Returns the configured Java {@link ClassLoader} used to resolve Scylla application entity {@link Class types}.
	 *
	 * @return the Java {@link ClassLoader} used to resolve Scylla application entity {@link Class types}.
	 * @see ClassLoader
	 * @see Optional
	 */
	protected Optional<ClassLoader> getBeanClassLoader() {
		return Optional.ofNullable(this.beanClassLoader);
	}

	/**
	 * Base packages to scan for entities annotated with {@link Table} annotations. By default, returns the package name
	 * of {@literal this} ({@code this.getClass().getPackage().getName()}. This method must never return {@literal null}.
	 */
	public String[] getEntityBasePackages() {
		return new String[] { getClass().getPackage().getName() };
	}

	/**
	 * Return the {@link Set} of initial entity classes. Scans by default the class path using
	 * {@link #getEntityBasePackages()}. Can be overridden by subclasses to skip class path scanning and return a fixed
	 * set of entity classes.
	 *
	 * @return {@link Set} of initial entity classes.
	 * @throws ClassNotFoundException if the entity scan fails.
	 * @see #getEntityBasePackages()
	 * @see ScyllaEntityClassScanner
	 */
	protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
		ScyllaEntityClassScanner scanner = new ScyllaEntityClassScanner();
		scanner.setBeanClassLoader(this.beanClassLoader);
		scanner.setEntityBasePackages(Arrays.asList(getEntityBasePackages()));
		return scanner.scanForEntityClasses();
	}

	/**
	 * The {@link SchemaAction} to perform at application startup. Defaults to {@link SchemaAction#NONE}.
	 *
	 * @see SchemaAction
	 */
	public SchemaAction getSchemaAction() {
		return SchemaAction.NONE;
	}


	/**
	 * Creates a {@link KeyspacePopulator} to cleanup the keyspace.
	 *
	 * @return the {@link KeyspacePopulator} or {@code null} if none configured.
	 * @see com.giangbb.scylla.core.cql.session.init.ResourceKeyspacePopulator
	 */
	@Nullable
	protected KeyspacePopulator keyspaceCleaner() {
		return null;
	}

	/**
	 * Creates a {@link KeyspacePopulator} to initialize the keyspace.
	 *
	 * @return the {@link KeyspacePopulator} or {@code null} if none configured.
	 * @see com.giangbb.scylla.core.cql.session.init.ResourceKeyspacePopulator
	 */
	@Nullable
	protected KeyspacePopulator keyspacePopulator() {
		return null;
	}
	
	

	/**
	 * Creates a new {@link UserTypeResolver} from the given {@link CqlSession}. Uses by default the configured
	 * {@link #getKeyspaceName() keyspace name}.
	 *
	 * @param cqlSession the Scylla {@link CqlSession} to use.
	 * @return a new {@link SimpleUserTypeResolver}.
	 */
	protected UserTypeResolver userTypeResolver(CqlSession cqlSession) {
		return new SimpleUserTypeResolver(cqlSession, CqlIdentifier.fromCql(getKeyspaceName()));
	}
}
