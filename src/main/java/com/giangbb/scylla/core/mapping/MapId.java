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

import org.springframework.lang.Nullable;

import java.util.Map;

/**
 * Interface that represents the id of a persistent entity, where the keys correspond to the entity's JavaBean
 * properties.
 *
 * @author Giangbb
 */
public interface MapId extends Map<String, Object> {

	/**
	 * Builder method that adds the value for the named property, then returns {@code this}.
	 *
	 * @param name The property name containing the value.
	 * @param value The property value.
	 * @return {@code this}
	 */
	MapId with(String name, @Nullable Object value);
}
