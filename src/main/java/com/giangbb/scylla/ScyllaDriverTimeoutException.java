package com.giangbb.scylla;

import org.springframework.dao.QueryTimeoutException;

/**
 * This exception is thrown when driver has timed out during any interaction with the Scylla coordinator node.
 *
 * @author Giangbb
 */
public class ScyllaDriverTimeoutException extends QueryTimeoutException {

	/**
	 * Constructor for {@link ScyllaDriverTimeoutException}.
	 *
	 * @param message the detail message.
	 * @param cause the root cause from the underlying data access API.
	 */
	public ScyllaDriverTimeoutException(String message, Throwable cause) {
		super(message, cause);
	}
}
