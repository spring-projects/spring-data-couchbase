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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.couchbase.client.java.search.SearchMetaData;
import com.couchbase.client.java.search.result.SearchFacetResult;
import com.couchbase.client.java.search.result.SearchRow;

/**
 * Wrapper for FTS search results that provides access to hydrated entities along with FTS metadata,
 * facet results, and raw search rows.
 *
 * @param <T> the entity type
 * @author Emilien Bevierre
 * @since 6.2
 */
public class SearchResult<T> {

	private final List<T> entities;
	private final List<SearchRow> rows;
	private final SearchMetaData metaData;
	private final Map<String, SearchFacetResult> facets;

	public SearchResult(List<T> entities, List<SearchRow> rows, SearchMetaData metaData,
			Map<String, SearchFacetResult> facets) {
		this.entities = entities != null ? entities : Collections.emptyList();
		this.rows = rows != null ? rows : Collections.emptyList();
		this.metaData = metaData;
		this.facets = facets != null ? facets : Collections.emptyMap();
	}

	/**
	 * The hydrated entities, resolved from search row document IDs via KV GET.
	 */
	public List<T> entities() {
		return entities;
	}

	/**
	 * The raw FTS search rows (with scores, fragments, locations, etc.).
	 */
	public List<SearchRow> rows() {
		return rows;
	}

	/**
	 * The FTS search metadata including metrics (total rows, max score, execution time).
	 */
	public SearchMetaData metaData() {
		return metaData;
	}

	/**
	 * Facet results keyed by facet name, if facets were requested via SearchOptions.
	 */
	public Map<String, SearchFacetResult> facets() {
		return facets;
	}

	/**
	 * Convenience: the total number of matches as reported by the FTS service.
	 * Note this may differ from {@code entities().size()} when limit/skip are used.
	 */
	public long totalRows() {
		return metaData != null ? metaData.metrics().totalRows() : entities.size();
	}

	/**
	 * Convenience: the maximum score across all results.
	 */
	public double maxScore() {
		return metaData != null ? metaData.metrics().maxScore() : 0.0;
	}
}
