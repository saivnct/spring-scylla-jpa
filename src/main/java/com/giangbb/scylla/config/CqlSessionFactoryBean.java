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
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import com.giangbb.scylla.core.convert.ScyllaConverter;
import com.giangbb.scylla.core.cql.ScyllaExceptionTranslator;
import com.giangbb.scylla.core.mapping.ScyllaMappingContext;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Factory for creating and configuring a Scylla {@link CqlSession}, which is a thread-safe singleton. As such, it is
 * sufficient to have one {@link CqlSession} per application and keyspace.
 *
 * @author Giangbb
 */
public class CqlSessionFactoryBean
		implements FactoryBean<CqlSession>, InitializingBean, DisposableBean, PersistenceExceptionTranslator {

	public static final String SCYLLA_SYSTEM_KEYSPACE = "system";
	public static final String DEFAULT_CONTACT_POINTS = "localhost";
	public static final String DEFAULT_DATA_CENTER = "datacenter1";
	public static final int DEFAULT_PORT = 9042;

	private static final ScyllaExceptionTranslator EXCEPTION_TRANSLATOR = new ScyllaExceptionTranslator();

	protected final Log log = LogFactory.getLog(getClass());

	private int port = DEFAULT_PORT;

	private @Nullable ScyllaConverter converter;

	private @Nullable CqlSession session;
	private @Nullable CqlSession systemSession;


	private boolean suspendLifecycleSchemaRefresh = false;

	private @Nullable SessionBuilderConfigurer sessionBuilderConfigurer;

	private IntFunction<Collection<InetSocketAddress>> contactPoints = port -> createInetSocketAddresses(
			DEFAULT_CONTACT_POINTS, port);

	private @Nullable String keyspaceName;
	private @Nullable String localDatacenter;
	private @Nullable String password;
	private @Nullable String username;

	/**
	 * Null-safe operation to determine whether the Scylla {@link CqlSession} is connected or not.
	 *
	 * @return a boolean value indicating whether the Scylla {@link CqlSession} is connected.
	 * @see com.datastax.oss.driver.api.core.session.Session#isClosed()
	 * @see #getObject()
	 */
	public boolean isConnected() {

		CqlSession session = getObject();

		return !(session == null || session.isClosed());
	}

	/**
	 * Set a comma-delimited string of the contact points (hosts) to connect to. Default is {@code localhost}; see
	 * {@link #DEFAULT_CONTACT_POINTS}. Contact points may use the form {@code host:port}, or a simple {@code host} to use
	 * the configured {@link #setPort(int) port}.
	 *
	 * @param contactPoints the contact points used by the new cluster, must not be {@literal null}.
	 */
	public void setContactPoints(String contactPoints) {

		Assert.hasText(contactPoints, "Contact points must not be empty");

		this.contactPoints = port -> createInetSocketAddresses(contactPoints, port);
	}

	/**
	 * Set a collection of the contact points (hosts) to connect to. Default is {@code localhost}; see
	 * {@link #DEFAULT_CONTACT_POINTS}.
	 *
	 * @param contactPoints the contact points used by the new cluster, must not be {@literal null}. Use
	 *          {@link InetSocketAddress#createUnresolved(String, int) unresolved addresses} to delegate hostname
	 *          resolution to the driver.
	 */
	public void setContactPoints(Collection<InetSocketAddress> contactPoints) {

		Assert.notNull(contactPoints, "Contact points must not be null");

		this.contactPoints = unusedPort -> contactPoints;
	}

	/**
	 * Sets the name of the local datacenter.
	 *
	 * @param localDatacenter a String indicating the name of the local datacenter.
	 */
	public void setLocalDatacenter(@Nullable String localDatacenter) {
		this.localDatacenter = localDatacenter;
	}

	/**
	 * Set the port for the contact points. Default is {@code 9042}, see {@link #DEFAULT_PORT}.
	 *
	 * @param port the port used by the new cluster.
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Set the username to use.
	 *
	 * @param username The username to set.
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * Set the password to use.
	 *
	 * @param password The password to set.
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Set the {@link ScyllaConverter} to use. Schema actions will derive table and user type information from the
	 * {@link ScyllaMappingContext} inside {@code converter}.
	 *
	 * @param converter must not be {@literal null}.
	 * @deprecated Use {@link SessionFactoryFactoryBean} with
	 *             {@link SessionFactoryFactoryBean#setConverter(ScyllaConverter)} instead.
	 */
	@Deprecated
	public void setConverter(ScyllaConverter converter) {

		Assert.notNull(converter, "ScyllaConverter must not be null");

		this.converter = converter;
	}

	/**
	 * @return the configured {@link ScyllaConverter}.
	 */
	@Nullable
	public ScyllaConverter getConverter() {
		return this.converter;
	}


	/**
	 * Sets the name of the Scylla Keyspace to connect to. Passing {@literal null} will cause the Scylla System
	 * Keyspace to be used.
	 *
	 * @param keyspaceName a String indicating the name of the Keyspace in which to connect.
	 * @see #getKeyspaceName()
	 */
	public void setKeyspaceName(@Nullable String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	/**
	 * Gets the name of the Scylla Keyspace to connect to.
	 *
	 * @return the name of the Scylla Keyspace to connect to as a String.
	 * @see #setKeyspaceName(String)
	 */
	@Nullable
	protected String getKeyspaceName() {
		return this.keyspaceName;
	}


	/**
	 * @return the {@link ScyllaMappingContext}.
	 */
	protected ScyllaMappingContext getMappingContext() {

		ScyllaConverter converter = getConverter();

		Assert.state(converter != null, "ScyllaConverter was not properly initialized");

		return converter.getMappingContext();
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
	 * Returns a reference to the connected Scylla {@link CqlSession}.
	 *
	 * @return a reference to the connected Scylla {@link CqlSession}.
	 * @throws IllegalStateException if the Scylla {@link CqlSession} was not properly initialized.
	 * @see CqlSession
	 */
	protected CqlSession getSession() {
		CqlSession session = getObject();
		Assert.state(session != null, "Session was not properly initialized");
		return session;
	}

	/**
	 * Sets the {@link SessionBuilderConfigurer} to configure the
	 * {@link com.datastax.oss.driver.api.core.session.SessionBuilder}.
	 *
	 * @param sessionBuilderConfigurer
	 */
	public void setSessionBuilderConfigurer(@Nullable SessionBuilderConfigurer sessionBuilderConfigurer) {
		this.sessionBuilderConfigurer = sessionBuilderConfigurer;
	}


	@Override
	public void afterPropertiesSet() {

		CqlSessionBuilder sessionBuilder = buildBuilder();

		this.systemSession = buildSystemSession(sessionBuilder);

		initializeCluster(this.systemSession);

		this.session = buildSession(sessionBuilder);

		initializeSchema(this.systemSession, this.session);
	}

	private void initializeSchema(CqlSession systemSession, CqlSession session) {

		Runnable schemaActionRunnable = () -> {
			// no-op
		};

		List<CompletionStage<?>> futures = new ArrayList<>(2);

		if (this.suspendLifecycleSchemaRefresh) {
			futures.add(SchemaUtils.withSuspendedAsyncSchemaRefresh(session, schemaActionRunnable));
		} else {
			futures.add(SchemaUtils.withAsyncSchemaRefresh(session, schemaActionRunnable));
		}

		if (systemSession.isSchemaMetadataEnabled()) {
			futures.add(systemSession.refreshSchemaAsync());
		}

		futures.forEach(CompletableFutures::getUninterruptibly);
	}

	protected CqlSessionBuilder buildBuilder() {
		Collection<InetSocketAddress> addresses = contactPoints.apply(this.port);
		Assert.notEmpty(addresses, "At least one server is required");


		CqlSessionBuilder sessionBuilder = createBuilder();

		addresses.forEach(sessionBuilder::addContactPoint);

		if (StringUtils.hasText(this.username)) {
			sessionBuilder.withAuthCredentials(this.username, this.password);
		}

		if (StringUtils.hasText(this.localDatacenter)) {
			sessionBuilder.withLocalDatacenter(this.localDatacenter);
		}

		return this.sessionBuilderConfigurer != null ? this.sessionBuilderConfigurer.configure(sessionBuilder)
				: sessionBuilder;
	}

	CqlSessionBuilder createBuilder() {
		return CqlSession.builder();
	}

	/**
	 * Build the Scylla {@link CqlSession System Session}.
	 *
	 * @param sessionBuilder {@link CqlSessionBuilder} used to a build a Scylla {@link CqlSession}.
	 * @return the built Scylla {@link CqlSession System Session}.
	 * @see CqlSessionBuilder
	 * @see CqlSession
	 */
	protected CqlSession buildSystemSession(CqlSessionBuilder sessionBuilder) {
		return sessionBuilder.withKeyspace(SCYLLA_SYSTEM_KEYSPACE).build();
	}

	/**
	 * Build a {@link CqlSession Session} to the user-defined {@literal Keyspace} or the default {@literal Keyspace} if
	 * the user did not specify a {@literal Keyspace} by {@link String name}.
	 *
	 * @param sessionBuilder {@link CqlSessionBuilder} used to a build a Scylla {@link CqlSession}.
	 * @return the built {@link CqlSession} to the user-defined {@literal Keyspace}.
	 * @see CqlSessionBuilder
	 * @see CqlSession
	 */
	protected CqlSession buildSession(CqlSessionBuilder sessionBuilder) {

		if (StringUtils.hasText(getKeyspaceName())) {
			sessionBuilder.withKeyspace(getKeyspaceName());
		}

		return sessionBuilder.build();
	}

	private void initializeCluster(CqlSession session) {
		Runnable schemaActionRunnable = () -> {
			// no-op
		};

		if (this.suspendLifecycleSchemaRefresh) {
			SchemaUtils.withSuspendedAsyncSchemaRefresh(session, schemaActionRunnable);
		} else {
			schemaActionRunnable.run();
		}
	}




	@Override
	public CqlSession getObject() {
		return this.session;
	}

	@Override
	public Class<? extends CqlSession> getObjectType() {
		return CqlSession.class;
	}

	@Nullable
	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException e) {
		return EXCEPTION_TRANSLATOR.translateExceptionIfPossible(e);
	}

	@Override
	public void destroy() {

		if (this.session != null) {

			Runnable schemaActionRunnable = () -> {
				// no-op
			};

			Runnable systemSchemaActionRunnable = () -> {
				//no-op
			};

			if (this.suspendLifecycleSchemaRefresh) {
				SchemaUtils.withSuspendedAsyncSchemaRefresh(this.session, schemaActionRunnable);
				SchemaUtils.withSuspendedAsyncSchemaRefresh(this.systemSession, systemSchemaActionRunnable);
			} else {
				schemaActionRunnable.run();
				systemSchemaActionRunnable.run();
			}

			closeSession();
			closeSystemSession();
		}
	}

	/**
	 * Close the regular session object.
	 */
	protected void closeSession() {
		this.session.close();
	}

	/**
	 * Close the system session object.
	 */
	protected void closeSystemSession() {
		this.systemSession.close();
	}

	/**
	 * Executes the given, raw Scylla CQL scripts. The {@link CqlSession} must be connected when this method is called.
	 *
	 * @see CqlSession#execute(String)
	 */
	private void executeCql(Stream<String> cql, CqlSession session) {

		cql.forEach(query -> {
			if (this.log.isInfoEnabled()) {
				this.log.info(String.format("Executing CQL [%s]", query));
			}
			session.execute(query);
		});
	}


	private static Collection<InetSocketAddress> createInetSocketAddresses(String contactPoints, int defaultPort) {

		return StringUtils.commaDelimitedListToSet(contactPoints) //
				.stream() //
				.map(contactPoint -> HostAndPort.createWithDefaultPort(contactPoint, defaultPort)) //
				.map(hostAndPort -> InetSocketAddress.createUnresolved(hostAndPort.getHost(), hostAndPort.getPort())) //
				.collect(Collectors.toList());
	}

	/**
	 * Value object to encapsulate host and port.
	 */
	private static class HostAndPort {

		private final String host;

		private final int port;

		private HostAndPort(String host, int port) {
			this.host = host;
			this.port = port;
		}

		/**
		 * Create a {@link HostAndPort} from a contact point. Contact points may contain a port or can be specified
		 * port-less. Contact points may be:
		 * <ul>
		 * <li>Plain IPv4 addresses ({@code 1.2.3.4})</li>
		 * <li>Hostnames ({@code foo.bar.baz})</li>
		 * <li>IPv6 without brackets {@code 1:2::3}</li>
		 * <li>IPv6 with brackets {@code [1:2::3]}</li>
		 * <li>IPv4 addresses with port ({@code 1.2.3.4:1234})</li>
		 * <li>Hostnames with port ({@code foo.bar.baz:1234})</li>
		 * <li>IPv6 with brackets and port {@code [1:2::3]:1234}</li>
		 * </ul>
		 *
		 * @param contactPoint must not be {@literal null}.
		 * @param defaultPort
		 * @return the host and port representation.
		 */
		static HostAndPort createWithDefaultPort(String contactPoint, int defaultPort) {

			int i = contactPoint.lastIndexOf(':');

			if (i == -1 || !isValidPort(contactPoint.substring(i + 1))) {
				return new HostAndPort(contactPoint, defaultPort);
			}

			String[] hostAndPort = contactPoint.split(":");
			String host;
			int port = defaultPort;

			if (hostAndPort.length != 2) {

				int bracketEnd = contactPoint.indexOf(']');
				if (contactPoint.startsWith("[") && bracketEnd != -1) {

					// IPv6 as resource identifier enclosed with brackets [ ]
					host = contactPoint.substring(0, bracketEnd + 1);
					String remainder = contactPoint.substring(bracketEnd + 1);

					if (remainder.startsWith(":")) {
						port = Integer.parseInt(remainder.substring(1));
					}
				} else {
					// everything else
					host = contactPoint;
				}
			} else {
				host = hostAndPort[0];
				port = Integer.parseInt(hostAndPort[1]);
			}
			return new HostAndPort(host, port);
		}

		private static boolean isValidPort(String value) {
			try {
				int i = Integer.parseInt(value);
				return i > 0 && i < 65535;
			} catch (NumberFormatException ex) {
				return false;
			}
		}

		public String getHost() {
			return host;
		}

		public int getPort() {
			return port;
		}
	}
}
