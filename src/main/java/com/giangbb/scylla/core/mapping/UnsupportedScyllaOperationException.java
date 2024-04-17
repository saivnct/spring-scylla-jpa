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
package com.giangbb.scylla.core.mapping;

import org.springframework.dao.InvalidDataAccessApiUsageException;

import java.io.Serial;

/**
 * @author Giangbb
 */
public class UnsupportedScyllaOperationException extends InvalidDataAccessApiUsageException {

	@Serial private static final long serialVersionUID = 4921001859094231277L;

	public UnsupportedScyllaOperationException(String msg) {
		super(msg);
	}

	public UnsupportedScyllaOperationException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
