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

import org.springframework.dao.TransientDataAccessException;

import java.io.Serial;

/**
 * Spring data access exception for Scylla when insufficient replicas are available for a given consistency level.
 *
 * @author Giangbb
 */
public class ScyllaInsufficientReplicasAvailableException extends TransientDataAccessException {

	@Serial private static final long serialVersionUID = 6415130674604814905L;

	private int numberRequired;
	private int numberAlive;

	/**
	 * Constructor for {@link ScyllaInsufficientReplicasAvailableException}.
	 *
	 * @param msg the detail message.
	 */
	public ScyllaInsufficientReplicasAvailableException(String msg) {
		super(msg);
	}

	/**
	 * Constructor for {@link ScyllaInsufficientReplicasAvailableException}.
	 *
	 * @param numberRequired the required number of replicas.
	 * @param msg the detail message.
	 * @param cause the root cause from the underlying data access API.
	 */
	public ScyllaInsufficientReplicasAvailableException(int numberRequired, int numberAlive, String msg,
														Throwable cause) {
		super(msg, cause);
		this.numberRequired = numberRequired;
		this.numberAlive = numberAlive;
	}

	public int getNumberRequired() {
		return numberRequired;
	}

	public int getNumberAlive() {
		return numberAlive;
	}
}
