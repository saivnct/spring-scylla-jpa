/*
 * Copyright 2013-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.giangbb.scylla.core.mapping;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.giangbb.scylla.core.cql.PrimaryKeyType;

import java.util.Comparator;

/**
 * {@link Comparator} implementation that orders {@link ScyllaPersistentProperty} instances.
 * <p>
 * Composite primary key properties and primary key properties sort before non-primary key properties. Ordering rules:
 * <ul>
 * <li>Clustering key columns, compare by ordinal/name/ordering</li>
 * <li>Partition key columns, compare by ordinal/name/ordering</li>
 * <li>Regular columns, compared by column name (see {@link String#compareTo(String)}).</li>
 * </ul>
 *
 * @author Giangbb
 * @see Comparator
 * @see ScyllaPersistentProperty
 */
public enum ScyllaPersistentPropertyComparator implements Comparator<ScyllaPersistentProperty> {

	/**
	 * Comparator instance.
	 */
	INSTANCE;

	@Override
	public int compare(ScyllaPersistentProperty left, ScyllaPersistentProperty right) {

		boolean leftIsClusteringColumn = left.isClusterKeyColumn();
		boolean rightIsClusteringColumn = right.isClusterKeyColumn();
		if (leftIsClusteringColumn && rightIsClusteringColumn) {
			ClusteringColumn leftAnnotation = left.findAnnotation(ClusteringColumn.class);
			ClusteringColumn rightAnnotation = right.findAnnotation(ClusteringColumn.class);
			if (leftAnnotation == null || rightAnnotation == null) {
				return 0;
			}

			return Integer.compare(leftAnnotation.value(), rightAnnotation.value());
		}

		boolean leftIsPartitionKeyColumn = left.isPartitionKeyColumn();
		boolean rightIsPartitionKeyColumn = right.isPartitionKeyColumn();
		if (leftIsPartitionKeyColumn && rightIsPartitionKeyColumn) {
			PartitionKey leftAnnotation = left.findAnnotation(PartitionKey.class);
			PartitionKey rightAnnotation = right.findAnnotation(PartitionKey.class);
			if (leftAnnotation == null || rightAnnotation == null) {
				return 0;
			}
			return Integer.compare(leftAnnotation.value(), rightAnnotation.value());
		}

		boolean leftIsKey = leftIsClusteringColumn || leftIsPartitionKeyColumn;
		boolean rightIsKey = rightIsClusteringColumn || rightIsPartitionKeyColumn;

		if (leftIsKey && rightIsKey){
			//both is key
			PrimaryKeyType leftType = leftIsPartitionKeyColumn ? PrimaryKeyType.PARTITIONED : PrimaryKeyType.CLUSTERED;
			PrimaryKeyType rightType = rightIsPartitionKeyColumn ? PrimaryKeyType.PARTITIONED : PrimaryKeyType.CLUSTERED;
			return leftType.compareTo(rightType);
		}else if (leftIsKey) {
			return -1;
		} else if (rightIsKey) {
			return 1;
		}

		Element leftAnnotation = left.findAnnotation(Element.class);
		Element rightAnnotation = right.findAnnotation(Element.class);

		if (leftAnnotation != null && rightAnnotation != null) {
			return Integer.compare(leftAnnotation.value(), rightAnnotation.value());
		}

		// else, neither property is a composite primary key nor a primary key; there is nothing more so from that
		// perspective, columns are equal.
		return 0;
	}
}
