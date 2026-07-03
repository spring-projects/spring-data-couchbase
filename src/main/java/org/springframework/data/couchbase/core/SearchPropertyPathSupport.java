/*
 * Copyright 2026-present the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.core;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.core.TypedPropertyPath;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.mapping.PersistentPropertyPath;

import com.couchbase.client.java.search.sort.SearchSort;

final class SearchPropertyPathSupport {

	private SearchPropertyPathSupport() {
	}

	static <P> String getMappedFieldPath(CouchbaseConverter converter, TypedPropertyPath<P, ?> property) {
		PersistentPropertyPath<CouchbasePersistentProperty> path = converter.getMappingContext()
				.getPersistentPropertyPath(property);
		return path.toDotPath(CouchbasePersistentProperty::getFieldName);
	}

	@SafeVarargs
	static <P> String[] getMappedFieldPaths(CouchbaseConverter converter, TypedPropertyPath<P, ?> property,
				TypedPropertyPath<P, ?>... additionalProperties) {

		List<String> fields = new ArrayList<>(additionalProperties.length + 1);
		fields.add(getMappedFieldPath(converter, property));

		for (TypedPropertyPath<P, ?> additionalProperty : additionalProperties) {
			fields.add(getMappedFieldPath(converter, additionalProperty));
		}

		return fields.toArray(String[]::new);
	}

	@SafeVarargs
	static <P> SearchSort[] toSearchSorts(CouchbaseConverter converter, TypedPropertyPath<P, ?> property,
				TypedPropertyPath<P, ?>... additionalProperties) {

		String[] fields = getMappedFieldPaths(converter, property, additionalProperties);
		SearchSort[] result = new SearchSort[fields.length];

		for (int i = 0; i < fields.length; i++) {
			result[i] = SearchSort.byField(fields[i]);
		}

		return result;
	}
}
