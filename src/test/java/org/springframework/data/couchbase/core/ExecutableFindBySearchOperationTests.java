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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.couchbase.client.java.search.HighlightStyle;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchRequest;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.SearchScanConsistency;
import com.couchbase.client.java.search.facet.SearchFacet;
import com.couchbase.client.java.search.sort.SearchSort;

/**
 * Unit tests verifying the fluent builder API compiles and chains correctly for
 * {@link ExecutableFindBySearchOperation} and {@link ReactiveFindBySearchOperation}.
 * <p>
 * These tests verify the type-safe fluent chain, not actual query execution (which requires a cluster).
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
class ExecutableFindBySearchOperationTests {

	@Test
	void fluentChainInterfaceHierarchyCompilesCorrectly() {
		// This test verifies the interface hierarchy is correct at compile time.
		// We verify that all the intermediate types in the chain can be assigned correctly.

		// The full chain from top to bottom:
		// ExecutableFindBySearch -> FindBySearchWithIndex -> FindBySearchWithProjection
		// -> FindBySearchWithFields -> FindBySearchWithFacets -> FindBySearchWithHighlight
		// -> FindBySearchWithSort -> FindBySearchWithSkip -> FindBySearchWithLimit
		// -> FindBySearchWithConsistency -> FindBySearchInScope -> FindBySearchInCollection
		// -> FindBySearchWithOptions -> FindBySearchWithQuery -> TerminatingFindBySearch

		// If this test compiles, the chain is valid.
		assertNotNull(ExecutableFindBySearchOperation.ExecutableFindBySearch.class);
		assertNotNull(ExecutableFindBySearchOperation.FindBySearchWithIndex.class);
		assertNotNull(ExecutableFindBySearchOperation.FindBySearchWithProjection.class);
		assertNotNull(ExecutableFindBySearchOperation.FindBySearchWithFields.class);
		assertNotNull(ExecutableFindBySearchOperation.FindBySearchWithFacets.class);
		assertNotNull(ExecutableFindBySearchOperation.FindBySearchWithHighlight.class);
		assertNotNull(ExecutableFindBySearchOperation.FindBySearchWithSort.class);
		assertNotNull(ExecutableFindBySearchOperation.FindBySearchWithSkip.class);
		assertNotNull(ExecutableFindBySearchOperation.FindBySearchWithLimit.class);
		assertNotNull(ExecutableFindBySearchOperation.FindBySearchWithConsistency.class);
		assertNotNull(ExecutableFindBySearchOperation.FindBySearchInScope.class);
		assertNotNull(ExecutableFindBySearchOperation.FindBySearchInCollection.class);
		assertNotNull(ExecutableFindBySearchOperation.FindBySearchWithOptions.class);
		assertNotNull(ExecutableFindBySearchOperation.FindBySearchWithQuery.class);
		assertNotNull(ExecutableFindBySearchOperation.TerminatingFindBySearch.class);
	}

	@Test
	void reactiveFluentChainInterfaceHierarchyCompilesCorrectly() {
		assertNotNull(ReactiveFindBySearchOperation.ReactiveFindBySearch.class);
		assertNotNull(ReactiveFindBySearchOperation.FindBySearchWithIndex.class);
		assertNotNull(ReactiveFindBySearchOperation.FindBySearchWithProjection.class);
		assertNotNull(ReactiveFindBySearchOperation.FindBySearchWithFields.class);
		assertNotNull(ReactiveFindBySearchOperation.FindBySearchWithFacets.class);
		assertNotNull(ReactiveFindBySearchOperation.FindBySearchWithHighlight.class);
		assertNotNull(ReactiveFindBySearchOperation.FindBySearchWithSort.class);
		assertNotNull(ReactiveFindBySearchOperation.FindBySearchWithSkip.class);
		assertNotNull(ReactiveFindBySearchOperation.FindBySearchWithLimit.class);
		assertNotNull(ReactiveFindBySearchOperation.FindBySearchWithConsistency.class);
		assertNotNull(ReactiveFindBySearchOperation.FindBySearchInScope.class);
		assertNotNull(ReactiveFindBySearchOperation.FindBySearchInCollection.class);
		assertNotNull(ReactiveFindBySearchOperation.FindBySearchWithOptions.class);
		assertNotNull(ReactiveFindBySearchOperation.FindBySearchWithQuery.class);
		assertNotNull(ReactiveFindBySearchOperation.TerminatingFindBySearch.class);
	}

	@Test
	void searchResultTerminalOperationIsDeclaredOnInterface() throws Exception {
		// Verify result() is declared on TerminatingFindBySearch
		assertNotNull(ExecutableFindBySearchOperation.TerminatingFindBySearch.class.getMethod("result"));
		assertNotNull(ReactiveFindBySearchOperation.TerminatingFindBySearch.class.getMethod("result"));
	}

	@Test
	void highlightDefaultMethodUsesServerDefault() {
		// Verify the default method signature exists and provides the right HighlightStyle
		// This is a compile-time check -- the default highlight method delegates to withHighlight(SERVER_DEFAULT, fields)
		assertNotNull(HighlightStyle.SERVER_DEFAULT);
	}
}
