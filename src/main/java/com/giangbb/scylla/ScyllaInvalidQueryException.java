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
package com.giangbb.scylla;

import org.springframework.dao.InvalidDataAccessApiUsageException;

import java.io.Serial;

/**
 * Spring data access exception for a Scylla query that's syntactically correct but invalid.
 *
 * @author Giangbb
 */
public class ScyllaInvalidQueryException extends InvalidDataAccessApiUsageException {

	@Serial private static final long serialVersionUID = 4594321191806182918L;

	/**
	 * Constructor for {@link ScyllaInvalidQueryException}.
	 *
	 * @param msg the detail message.
	 */
	public ScyllaInvalidQueryException(String msg) {
		super(msg);
	}

	/**
	 * Constructor for {@link ScyllaInvalidQueryException}.
	 *
	 * @param msg the detail message.
	 * @param cause the root cause from the underlying data access API.
	 */
	public ScyllaInvalidQueryException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
