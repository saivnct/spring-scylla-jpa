/*
 * Copyright 2018-2024 the original author or authors.
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

import java.lang.annotation.*;

/**
 * Annotation to define an ordinal element index within a tuple. Ordinals map to fields or property accessors within a
 * domain class. Element indexes within a tuple must be unique and consecutive beginning with the first index at
 * {@literal 0}.
 *
 * @author Giangbb
 * @see Tuple
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER })
public @interface Element {

	/**
	 * @return ordinal index within a tuple. First index is {@literal 0}, must be greater or equal to zero.
	 */
	int value();
}
