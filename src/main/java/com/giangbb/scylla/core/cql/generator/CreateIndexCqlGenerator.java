/*
 * Copyright 2013-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.giangbb.scylla.core.cql.generator;

import com.giangbb.scylla.core.cql.keyspace.CqlStringUtils;
import com.giangbb.scylla.core.cql.keyspace.CreateIndexSpecification;
import com.giangbb.scylla.core.cql.keyspace.CreateIndexSpecification.ColumnFunction;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * CQL generator for generating a {@code CREATE INDEX} statement.
 *
 * @author Giangbb
 */
public class CreateIndexCqlGenerator extends IndexNameCqlGenerator<CreateIndexSpecification> {

	public CreateIndexCqlGenerator(CreateIndexSpecification specification) {
		super(specification);
	}

	public static String toCql(CreateIndexSpecification specification) {
		return new CreateIndexCqlGenerator(specification).toCql();
	}

	@Override
	public StringBuilder toCql(StringBuilder cql) {

		cql.append("CREATE").append(spec().isCustom() ? " CUSTOM" : "").append(" INDEX")
				.append(spec().getIfNotExists() ? " IF NOT EXISTS" : "");

		if (spec().getName() != null) {
			cql.append(" ").append(spec().getName().asCql(true));
		}

		cql.append(" ON ").append(spec().getTableName().asCql(true)).append(" (");

		if (spec().getColumnFunction() != ColumnFunction.NONE) {
			cql.append(spec().getColumnFunction().name()).append("(").append(spec().getColumnName().asCql(true)).append(")");
		} else {
			cql.append(spec().getColumnName().asCql(true));
		}

		cql.append(")");

		if (spec().isCustom()) {
			cql.append(" USING ").append("'").append(spec().getUsing()).append("'");
		}

		Map<String, String> options = spec().getOptions();

		if (!options.isEmpty()) {

			List<String> entries = new ArrayList<>(options.size());

			options.forEach((key, value) -> entries
					.add(String.format("'%s': '%s'", CqlStringUtils.escapeSingle(key), CqlStringUtils.escapeSingle(value))));

			StringBuilder optionsCql = new StringBuilder(" WITH OPTIONS = ").append("{");

			optionsCql.append(StringUtils.collectionToDelimitedString(entries, ", "));
			optionsCql.append("}");

			cql.append(optionsCql);
		}

		cql.append(";");

		return cql;
	}
}
