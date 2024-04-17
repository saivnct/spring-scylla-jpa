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

import org.springframework.data.mapping.MappingException;
import org.springframework.data.util.TypeInformation;

import java.util.Comparator;

/**
 * Scylla Tuple-specific {@link org.springframework.data.mapping.PersistentEntity} for a mapped tuples. Mapped tuples
 * are nested level entities that can be referred from a {@link ScyllaPersistentEntity}.
 *
 * @author Giangbb
 * @see Tuple
 * @see Element
 */
public class BasicScyllaPersistentTupleEntity<T> extends BasicScyllaPersistentEntity<T> {

	/**
	 * Creates a new {@link BasicScyllaPersistentTupleEntity} given {@link TypeInformation}.
	 *
	 * @param information must not be {@literal null}.
	 */
	public BasicScyllaPersistentTupleEntity(TypeInformation<T> information) {

		super(information, ScyllaPersistentTupleMetadataVerifier.INSTANCE, TuplePropertyComparator.INSTANCE);
	}

	@Override
	public void verify() throws MappingException {

		super.verify();

		ScyllaPersistentTupleMetadataVerifier.INSTANCE.verify(this);
	}

	@Override
	public boolean isTupleType() {
		return true;
	}

	/**
	 * {@link ScyllaPersistentProperty} comparator using to sort properties by their
	 * {@link ScyllaPersistentProperty#getRequiredOrdinal()}.
	 *
	 * @see Element
	 */
	enum TuplePropertyComparator implements Comparator<ScyllaPersistentProperty> {

		INSTANCE;

		@Override
		public int compare(ScyllaPersistentProperty propertyOne, ScyllaPersistentProperty propertyTwo) {
			return Integer.compare(propertyOne.getRequiredOrdinal(), propertyTwo.getRequiredOrdinal());
		}
	}
}
