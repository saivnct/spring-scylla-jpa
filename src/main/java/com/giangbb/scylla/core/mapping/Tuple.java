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
 * Identifies a domain object as Scylla Tuple. Tuples use ordered fields to map their value to the actual property.
 * <p>
 * A mapped tuple type is typically annotated with {@code @Tuple} and its properties/accessors are annotated with
 * {@code @Element(0), @Element(1), ..., @Element(n)}.
 * <p>
 * Example usage:
 *
 * <pre class="code">
 * &#64;Tuple
 * class Address {
 *
 * 	&#64;Element(0) String street;
 *
 * 	&#64;Element(1) &#64;ScyllaType(type = Name.ASCII) String city;
 *
 * 	&#64;Element(2) int sortOrder;
 * }
 * </pre>
 *
 * @author Giangbb
 * @see Element
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface Tuple {
}
