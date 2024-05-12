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
package com.giangbb.scylla.core.cql;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.*;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import com.giangbb.scylla.*;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.util.*;

/**
 * Simple {@link PersistenceExceptionTranslator} for Scylla.
 * <p>
 * Convert the given runtime exception to an appropriate exception from the {@code org.springframework.dao} hierarchy.
 * Preserves exception if it's already a {@link DataAccessException} and ignores non {@link DriverException}s returning
 * {@literal null}. Falls back to {@link ScyllaUncategorizedException} in case there's no mapping to a more detailed
 * exception.
 *
 * @author Giangbb
 */
@SuppressWarnings("unchecked")
public class ScyllaExceptionTranslator implements CqlExceptionTranslator {

	private static final Set<String> CONNECTION_FAILURE_TYPES = new HashSet<>(
			Arrays.asList("NoHostAvailableException", "ConnectionException", "OperationTimedOutException",
					"TransportException", "BusyConnectionException", "BusyPoolException"));

	private static final Set<String> RESOURCE_FAILURE_TYPES = new HashSet<>(
			Arrays.asList("ReadFailureException", "WriteFailureException", "FunctionExecutionException"));

	@Override
	@Nullable
	public DataAccessException translateExceptionIfPossible(RuntimeException exception) {

		if (exception instanceof DataAccessException) {
			return (DataAccessException) exception;
		}

		return translate(null, null, exception);
	}

	@Override
	public DataAccessException translate(@Nullable String task, @Nullable String cql, RuntimeException exception) {

		String message = buildMessage(task, cql, exception);

		// Remember: subclasses must come before superclasses, otherwise the
		// superclass would match before the subclass!

		if (exception instanceof AuthenticationException) {
			return new ScyllaAuthenticationException(((AuthenticationException) exception).getEndPoint(), message,
					exception);
		}

		if (exception instanceof DriverTimeoutException driverTimeoutException) {
			return new ScyllaDriverTimeoutException(driverTimeoutException.getMessage(), driverTimeoutException);
		}

		if (exception instanceof ReadTimeoutException) {
			return new ScyllaReadTimeoutException(((ReadTimeoutException) exception).wasDataPresent(), message, exception);
		}

		if (exception instanceof WriteTimeoutException) {

			WriteType writeType = ((WriteTimeoutException) exception).getWriteType();
			return new ScyllaWriteTimeoutException(writeType == null ? null : writeType.name(), message, exception);
		}

		if (exception instanceof TruncateException) {
			return new ScyllaTruncateException(message, exception);
		}

		if (exception instanceof UnavailableException) {

			UnavailableException ux = (UnavailableException) exception;
			return new ScyllaInsufficientReplicasAvailableException(ux.getRequired(), ux.getAlive(), message, exception);
		}

		if (exception instanceof OverloadedException || exception instanceof BootstrappingException) {
			return new TransientDataAccessResourceException(message, exception);
		}
		if (exception instanceof AlreadyExistsException) {

			AlreadyExistsException aex = (AlreadyExistsException) exception;
			return new ScyllaSchemaElementExistsException(aex.getMessage(), aex);
		}

		if (exception instanceof InvalidConfigurationInQueryException) {
			return new ScyllaInvalidConfigurationInQueryException(message, exception);
		}

		if (exception instanceof InvalidQueryException) {
			return new ScyllaInvalidQueryException(message, exception);
		}

		if (exception instanceof SyntaxError) {
			return new ScyllaQuerySyntaxException(message, exception);
		}

		if (exception instanceof UnauthorizedException) {
			return new ScyllaUnauthorizedException(message, exception);
		}

		if (exception instanceof AllNodesFailedException) {
			return new ScyllaConnectionFailureException(((AllNodesFailedException) exception).getErrors(), message,
					exception);
		}

		String exceptionType = ClassUtils.getShortName(ClassUtils.getUserClass(exception.getClass()));

		if (CONNECTION_FAILURE_TYPES.contains(exceptionType)) {

			Map<Node, Throwable> errorMap = Collections.emptyMap();

			if (exception instanceof CoordinatorException) {
				CoordinatorException cx = (CoordinatorException) exception;
				errorMap = Collections.singletonMap(cx.getCoordinator(), exception);
			}

			return new ScyllaConnectionFailureException(errorMap, message, exception);
		}

		if (RESOURCE_FAILURE_TYPES.contains(exceptionType)) {
			return new DataAccessResourceFailureException(message, exception);
		}

		if (exception instanceof DriverException
				|| (exception.getClass().getName().startsWith("com.datastax.oss.driver"))) {
			// unknown or unhandled exception
			return new ScyllaUncategorizedException(message, exception);
		}
		return null;
	}

	/**
	 * Build a message {@code String} for the given {@link DriverException}.
	 * <p>
	 * To be called by translator subclasses when creating an instance of a generic
	 * {@link DataAccessException} class.
	 *
	 * @param task readable text describing the task being attempted
	 * @param cql the CQL statement that caused the problem (may be {@literal null})
	 * @param ex the offending {@code DriverException}
	 * @return the message {@code String} to use
	 */
	protected String buildMessage(@Nullable String task, @Nullable String cql, RuntimeException ex) {

		if (StringUtils.hasText(task) || StringUtils.hasText(cql)) {
			return task + "; CQL [" + cql + "]; " + ex.getMessage();
		}

		return ex.getMessage();
	}
}
