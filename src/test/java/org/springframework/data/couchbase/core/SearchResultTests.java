/*
 * Copyright 2025-present the original author or authors
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

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SearchResult}.
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
class SearchResultTests {

	@Test
	void constructorWithNullsDefaultsToEmptyCollections() {
		SearchResult<String> result = new SearchResult<>(null, null, null, null);
		assertNotNull(result.entities());
		assertTrue(result.entities().isEmpty());
		assertNotNull(result.rows());
		assertTrue(result.rows().isEmpty());
		assertNull(result.metaData());
		assertNotNull(result.facets());
		assertTrue(result.facets().isEmpty());
	}

	@Test
	void entitiesReturnedCorrectly() {
		List<String> entities = Arrays.asList("a", "b", "c");
		SearchResult<String> result = new SearchResult<>(entities, null, null, null);
		assertEquals(3, result.entities().size());
		assertEquals("a", result.entities().get(0));
	}

	@Test
	void totalRowsWithNullMetaDataFallsBackToEntitySize() {
		SearchResult<String> result = new SearchResult<>(Arrays.asList("a", "b"), null, null, null);
		assertEquals(2, result.totalRows());
	}

	@Test
	void maxScoreWithNullMetaDataReturnsZero() {
		SearchResult<String> result = new SearchResult<>(Collections.emptyList(), null, null, null);
		assertEquals(0.0, result.maxScore());
	}

	@Test
	void facetsReturnedCorrectly() {
		SearchResult<String> result = new SearchResult<>(null, null, null, Collections.emptyMap());
		assertTrue(result.facets().isEmpty());
	}
}
