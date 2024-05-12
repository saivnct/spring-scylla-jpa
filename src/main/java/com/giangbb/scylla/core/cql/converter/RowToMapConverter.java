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
package com.giangbb.scylla.core.cql.converter;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;

import java.util.HashMap;
import java.util.Map;

/**
 * Converter to convert {@link Row}s to a {@link Map} of {@link String}/{@link Object} representation.
 *
 * @author Giangbb
 */
@ReadingConverter
public enum RowToMapConverter implements Converter<Row, Map<String, Object>> {

	INSTANCE;

	@Override
	public Map<String, Object> convert(Row row) {

		ColumnDefinitions cols = row.getColumnDefinitions();
		Map<String, Object> map = new HashMap<>(cols.size());

		cols.forEach(columnDefinition -> {
			map.put(columnDefinition.getName().toString(),
					row.isNull(columnDefinition.getName()) ? null : row.getObject(columnDefinition.getName()));

		});

		return map;
	}
}
