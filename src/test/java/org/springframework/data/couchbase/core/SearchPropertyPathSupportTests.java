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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;

import org.junit.jupiter.api.Test;
import org.springframework.data.core.PropertyPath;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.mapping.Field;

class SearchPropertyPathSupportTests {

	private final MappingCouchbaseConverter converter;

	SearchPropertyPathSupportTests() {
		CouchbaseMappingContext mappingContext = new CouchbaseMappingContext();
		mappingContext.setInitialEntitySet(Set.of(SearchDocument.class, SearchAddress.class));
		mappingContext.afterPropertiesSet();
		this.converter = new MappingCouchbaseConverter(mappingContext);
	}

	@Test
	void resolvesAlternativeFieldNames() {
		assertEquals("nickname",
				SearchPropertyPathSupport.getMappedFieldPath(converter, PropertyPath.of(SearchDocument::getMiddlename)));
	}

	@Test
	void resolvesNestedPropertyPaths() {
		assertEquals("address.city", SearchPropertyPathSupport.getMappedFieldPath(converter,
				PropertyPath.of(SearchDocument::getAddress).then(SearchAddress::getCity)));
	}

	@Test
	void resolvesMultipleMappedFieldPaths() {
		assertArrayEquals(new String[] { "nickname", "address.city" }, SearchPropertyPathSupport
				.getMappedFieldPaths(converter, PropertyPath.of(SearchDocument::getMiddlename),
						PropertyPath.of(SearchDocument::getAddress).then(SearchAddress::getCity)));
	}

	@Document
	static class SearchDocument {

		@Id String id;

		@Field("nickname") String middlename;

		SearchAddress address;

		String getMiddlename() {
			return middlename;
		}

		SearchAddress getAddress() {
			return address;
		}
	}

	@Document
	static class SearchAddress {

		String city;

		String getCity() {
			return city;
		}
	}
}
