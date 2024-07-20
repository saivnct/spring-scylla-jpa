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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import com.giangbb.scylla.core.cql.keyspace.CreateKeyspaceSpecification;
import com.giangbb.scylla.core.cql.keyspace.DropKeyspaceSpecification;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import com.giangbb.scylla.SessionFactory;
import com.giangbb.scylla.core.convert.ScyllaConverter;
import com.giangbb.scylla.core.cql.session.DefaultSessionFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Spring {@link Configuration} class used to configure a Scylla client application {@link CqlSession} connected to a
 * Scylla cluster. Enables a Scylla Keyspace to be specified along with the ability to execute arbitrary CQL on
 * startup as well as shutdown.
 *
 * @author Giangbb
 * @see BeanFactoryAware
 * @see Configuration
 */
@Configuration
public abstract class AbstractSessionConfiguration implements BeanFactoryAware {
	protected final Log log = LogFactory.getLog(getClass());

	private @Nullable BeanFactory beanFactory;

	/**
	 * Configures a reference to the {@link BeanFactory}.
	 *
	 * @param beanFactory reference to the {@link BeanFactory}.
	 * @throws BeansException if the {@link BeanFactory} could not be initialized.
	 * @see BeanFactory
	 */
	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	/**
	 * Returns the configured reference to the {@link BeanFactory}.
	 *
	 * @return the configured reference to the {@link BeanFactory}.
	 * @throws IllegalStateException if the {@link BeanFactory} reference was not configured.
	 * @see BeanFactory
	 */
	protected BeanFactory getBeanFactory() {

		Assert.state(this.beanFactory != null, "BeanFactory not initialized");

		return this.beanFactory;
	}

	/**
	 * Gets a required bean of the provided {@link Class type} from the {@link BeanFactory}.
	 *
	 * @param <T> {@link Class parameterized class type} of the bean.
	 * @param beanType {@link Class type} of the bean.
	 * @return a required bean of the given {@link Class type} from the {@link BeanFactory}.
	 * @see BeanFactory#getBean(Class)
	 * @see #getBeanFactory()
	 */
	protected <T> T requireBeanOfType(@NonNull Class<T> beanType) {
		return getBeanFactory().getBean(beanType);
	}

	/**
	 * Return the name of the keyspace to connect to.
	 *
	 * @return must not be {@literal null}.
	 */
	protected abstract String getKeyspaceName();

	/**
	 * Return the ConsistencyLV to connect to.
	 *
	 */
	protected String getConsistencyLV(){
		return null;
	}

	/**
	 * Returns the local data center name used for
	 * {@link com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy}, defaulting to {@code datacenter1}.
	 * Typically required when connecting a Scylla cluster. Not required when using an Astra connection bundle.
	 *
	 * @return the local data center name. Can be {@literal null} when using an Astra connection bundle.
	 */
	@Nullable
	protected String getLocalDataCenter() {
		return CqlSessionFactoryBean.DEFAULT_DATA_CENTER;
	}

	protected String getContactPoints() {
		return CqlSessionFactoryBean.DEFAULT_CONTACT_POINTS;
	}

	protected int getPort() {
		return CqlSessionFactoryBean.DEFAULT_PORT;
	}

	@Nullable
	protected String getUsername() {
		return null;
	}

	@Nullable
	protected String getPassword() {
		return null;
	}

	@Nullable
	protected String getSessionName() {
		return null;
	}
	/**
	 * Returns the {@link CompressionType}.
	 *
	 * @return the {@link CompressionType}, may be {@literal null}.
	 */
	@Nullable
	protected CompressionType getCompressionType() {
		return null;
	}

	@Nullable
	protected Duration getMetaDataSchemaRequestTimeout() {
		return null;
	}

	@Nullable
	protected Duration getConnectionInitTimeout() {
		return null;
	}

	@Nullable
	protected Duration getRequestTimeout() {
		return null;
	}


	/**
	 * Returns the initialized {@link CqlSession} instance.
	 *
	 * @return the {@link CqlSession}.
	 * @throws IllegalStateException if the session factory is not initialized.
	 */
	protected CqlSession getRequiredSession() {
		return requireBeanOfType(CqlSession.class);
	}


	protected ScyllaConverter getRequiredConverter() {
		return requireBeanOfType(ScyllaConverter.class);
	}

	/**
	 * Returns the initialized {@link CqlSession} instance.
	 *
	 * @return the {@link CqlSession}.
	 * @throws IllegalStateException if the session factory is not initialized.
	 */
	protected SessionFactory getRequiredSessionFactory() {
		ObjectProvider<SessionFactory> beanProvider = getBeanFactory().getBeanProvider(SessionFactory.class);

		return beanProvider.getIfAvailable(() -> new DefaultSessionFactory(requireBeanOfType(CqlSession.class)));
	}


	/**
	 * Returns the {@link SessionBuilderConfigurer}.
	 *
	 * @return the {@link SessionBuilderConfigurer}; may be {@literal null}.
	 */
	@Nullable
	protected SessionBuilderConfigurer getSessionBuilderConfigurer() {
		return null;
	}

	/**
	 * Returns the {@link DriverConfigLoaderBuilderConfigurer}. The configuration gets applied after applying
	 * {@link System#getProperties() System Properties} config overrides and before
	 * {@link #getDriverConfigurationResource() the driver config file}.
	 *
	 * @return the {@link DriverConfigLoaderBuilderConfigurer}; may be {@literal null}.
	 */
	@Nullable
	protected DriverConfigLoaderBuilderConfigurer getDriverConfigLoaderBuilderConfigurer() {
		return null;
	}

	/**
	 * Returns the {@link Resource} pointing to a driver configuration file. The configuration file is applied after
	 * applying {@link System#getProperties() System Properties} and the configuration built by this configuration class.
	 *
	 * @return the {@link Resource}; may be {@literal null} if none provided.
	 */
	@Nullable
	protected Resource getDriverConfigurationResource() {
		return null;
	}

	/**
	 * Returns the list of keyspace creations to be run right after initialization.
	 *
	 * @return the list of keyspace creations, may be empty but never {@code null}.
	 */
	protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
		return Collections.emptyList();
	}

	/**
	 * Returns the list of keyspace drops to be run before shutdown.
	 *
	 * @return the list of keyspace drops, may be empty but never {@code null}.
	 */
	protected List<DropKeyspaceSpecification> getKeyspaceDrops() {
		return Collections.emptyList();
	}


	/**
	 * Creates a {@link CqlSessionFactoryBean} that provides a Scylla {@link CqlSession}.
	 *
	 * @return the {@link CqlSessionFactoryBean}.
	 * @see #getKeyspaceName()
	 */
	@Bean
	public CqlSessionFactoryBean scyllaSession() {
		CqlSessionFactoryBean bean = new CqlSessionFactoryBean();
		bean.setContactPoints(getContactPoints());
		bean.setKeyspaceCreations(getKeyspaceCreations());
		bean.setKeyspaceDrops(getKeyspaceDrops());
		bean.setKeyspaceName(getKeyspaceName());
		bean.setLocalDatacenter(getLocalDataCenter());
		bean.setPort(getPort());
		bean.setUsername(getUsername());
		bean.setPassword(getPassword());
		bean.setSessionBuilderConfigurer(getSessionBuilderConfigurerWrapper());

		return bean;
	}

	private SessionBuilderConfigurer getSessionBuilderConfigurerWrapper() {
		SessionBuilderConfigurer sessionConfigurer = getSessionBuilderConfigurer();
		DriverConfigLoaderBuilderConfigurer driverConfigLoaderConfigurer = getDriverConfigLoaderBuilderConfigurer();
		Resource driverConfigFile = getDriverConfigurationResource();

		return sessionBuilder -> {
			ProgrammaticDriverConfigLoaderBuilder builder = new DefaultProgrammaticDriverConfigLoaderBuilder(() -> {

				ScyllaDriverOptions options = new ScyllaDriverOptions();

				if (StringUtils.hasText(getSessionName())) {
					options.add(DefaultDriverOption.SESSION_NAME, getSessionName());
				}

				CompressionType compressionType = getCompressionType();

				if (compressionType != null) {
					options.add(DefaultDriverOption.PROTOCOL_COMPRESSION, compressionType);
				}

				ConfigFactory.invalidateCaches();

				Config config = ConfigFactory.defaultOverrides() //
						.withFallback(options.build());

				if (driverConfigFile != null) {
					try {
						config = config
								.withFallback(ConfigFactory.parseReader(new InputStreamReader(driverConfigFile.getInputStream())));
					} catch (IOException e) {
						throw new IllegalStateException(String.format("Cannot parse driver config file %s", driverConfigFile), e);
					}
				}

				return config.withFallback(ConfigFactory.defaultReference());

			}, DefaultDriverConfigLoader.DEFAULT_ROOT_PATH);

			if (driverConfigLoaderConfigurer != null) {
				driverConfigLoaderConfigurer.configure(builder);
			}

//			if (StringUtils.hasLength(getUsername()) && StringUtils.hasLength(getPassword())) {
//				builder = builder
//                    .withString(
//                            DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class.getName()
//                    )
//                    .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, getUsername())
//                    .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, getPassword());
//        	}


			if (getConsistencyLV() != null && !getConsistencyLV().isEmpty()){
				builder.withString(DefaultDriverOption.REQUEST_CONSISTENCY, getConsistencyLV());
			}


			if (getMetaDataSchemaRequestTimeout() != null){
				builder
						.withDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, getMetaDataSchemaRequestTimeout());
			}

			if (getConnectionInitTimeout() != null){
				builder
						.withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, getConnectionInitTimeout());
			}

			if (getRequestTimeout() != null){
				builder
						.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, getRequestTimeout());
			}

			sessionBuilder.withConfigLoader(builder.build());

			if (sessionConfigurer != null) {
				return sessionConfigurer.configure(sessionBuilder);
			}

			return sessionBuilder;
		};
	}

	private static class ScyllaDriverOptions {

		private final Map<String, String> options = new LinkedHashMap<>();

		private ScyllaDriverOptions add(DriverOption option, String value) {
			this.options.put(option.getPath(), value);
			return this;
		}

		private ScyllaDriverOptions add(DriverOption option, Enum<?> value) {
			return add(option, value.name());
		}

		private Config build() {
			return ConfigFactory.parseMap(this.options, "Environment");
		}

	}
}
