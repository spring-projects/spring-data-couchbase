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

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.SimpleCouchbaseClientFactory;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.repository.Collection;
import org.springframework.data.couchbase.repository.Scope;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.DeserializationFeature;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.SearchRequest;
import com.couchbase.client.java.search.facet.SearchFacet;
import com.couchbase.client.java.search.result.SearchRow;
import com.couchbase.client.java.search.sort.SearchSort;

/**
 * FTS integration tests against travel-sample.inventory.airport.
 * <p>
 * Requires a Couchbase cluster with the travel-sample bucket loaded and FTS enabled.
 * <p>
 * A scope-level FTS index is created automatically on first run and reused on subsequent runs.
 * Since the scope-level FTS index covers all collections in the inventory scope, queries use a
 * conjunction with {@code type:airport} to restrict results to airport documents only.
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
class CouchbaseTemplateFtsIntegrationTests extends JavaIntegrationTests {

	private static final String INDEX_NAME = "sd-fts-airport-idx";
	private static final String TS_BUCKET = "travel-sample";

	private static CouchbaseClientFactory travelSampleClientFactory;
	private static CouchbaseTemplate template;
	private static ReactiveCouchbaseTemplate reactiveTemplate;

	private static final SearchQuery AIRPORT_TYPE_FILTER = SearchQuery.term("airport").field("type");

	@BeforeAll
	static void setupFtsTest() {
		callSuperBeforeAll(new Object() {});

		JacksonTransformers.MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		travelSampleClientFactory = new SimpleCouchbaseClientFactory(connectionString(), authenticator(), TS_BUCKET);
		Cluster cluster = travelSampleClientFactory.getCluster();
		cluster.bucket(TS_BUCKET).waitUntilReady(Duration.ofSeconds(30));

		CouchbaseMappingContext mappingContext = new CouchbaseMappingContext();
		mappingContext.setAutoIndexCreation(false);
		mappingContext.afterPropertiesSet();
		MappingCouchbaseConverter converter = new MappingCouchbaseConverter(mappingContext, "t");
		JacksonTranslationService translationService = new JacksonTranslationService();
		translationService.afterPropertiesSet();

		template = new CouchbaseTemplate(travelSampleClientFactory, converter, translationService);
		reactiveTemplate = new ReactiveCouchbaseTemplate(travelSampleClientFactory, converter, translationService);

		ensureFtsIndex(cluster);
	}

	@AfterAll
	public static void tearDownFtsTest() {
		// Index is intentionally NOT dropped: re-indexing 30k+ docs on each run is too slow.
		if (travelSampleClientFactory != null) {
			try {
				travelSampleClientFactory.close();
			} catch (Exception e) {
				LOGGER.warn("Failed to close travel-sample client factory: {}", e.getMessage());
			}
		}
		callSuperAfterAll(new Object() {});
	}

	private static SearchRequest airportSearch(SearchQuery query) {
		return SearchRequest.create(SearchQuery.conjuncts(query, AIRPORT_TYPE_FILTER));
	}

	@Test
	void searchByMatchQueryReturnsHydratedAirports() {
		List<TsAirport> results = template.findBySearch(TsAirport.class)
				.withIndex(INDEX_NAME)
				.matching(airportSearch(SearchQuery.match("France").field("country")))
				.all();

		assertFalse(results.isEmpty(), "Expected airports in France");
		for (TsAirport airport : results) {
			assertEquals("France", airport.country);
		}
	}

	@Test
	void searchByQueryStringReturnsMatchingAirports() {
		List<TsAirport> results = template.findBySearch(TsAirport.class)
				.withIndex(INDEX_NAME)
				.matching(airportSearch(SearchQuery.queryString("+country:\"United States\" +faa:SFO")))
				.all();

		assertFalse(results.isEmpty(), "Expected SFO airport");
		assertTrue(results.stream().anyMatch(a -> "SFO".equals(a.faa)));
	}

	@Test
	void searchCountForKnownCountry() {
		long count = template.findBySearch(TsAirport.class)
				.withIndex(INDEX_NAME)
				.matching(airportSearch(SearchQuery.match("United States").field("country")))
				.count();

		assertTrue(count > 100, "Expected many US airports in travel-sample");
	}

	@Test
	void searchExistsForKnownAirport() {
		boolean exists = template.findBySearch(TsAirport.class)
				.withIndex(INDEX_NAME)
				.matching(airportSearch(SearchQuery.match("SFO").field("faa")))
				.exists();

		assertTrue(exists, "SFO should exist in travel-sample");
	}

	@Test
	void searchFirstReturnsSingleEntity() {
		TsAirport airport = template.findBySearch(TsAirport.class)
				.withIndex(INDEX_NAME)
				.matching(airportSearch(SearchQuery.match("United Kingdom").field("country")))
				.firstValue();

		assertNotNull(airport, "Expected at least one UK airport");
		assertEquals("United Kingdom", airport.country);
	}

	@Test
	void searchWithLimitAndSkipForPagination() {
		SearchRequest request = airportSearch(SearchQuery.match("France").field("country"));

		List<TsAirport> page1 = template.findBySearch(TsAirport.class)
				.withIndex(INDEX_NAME)
				.withSkip(0)
				.withLimit(5)
				.matching(request)
				.all();

		List<TsAirport> page2 = template.findBySearch(TsAirport.class)
				.withIndex(INDEX_NAME)
				.withSkip(5)
				.withLimit(5)
				.matching(request)
				.all();

		assertEquals(5, page1.size(), "First page should have 5 results");
		assertFalse(page2.isEmpty(), "Second page should have results");
		page2.forEach(a2 -> assertFalse(page1.stream().anyMatch(a1 -> a1.key.equals(a2.key)),
				"Pages should not overlap"));
	}

	@Test
	void searchRawRowsProvideScoresAndIds() {
		List<SearchRow> rows = template.findBySearch(TsAirport.class)
				.withIndex(INDEX_NAME)
				.matching(airportSearch(SearchQuery.match("international").field("airportname")))
				.rows();

		assertFalse(rows.isEmpty(), "Expected raw rows for 'international' airports");
		for (SearchRow row : rows) {
			assertNotNull(row.id(), "Row should have a document ID");
			assertTrue(row.score() > 0, "Row should have a positive score");
		}
	}

	@Test
	void searchResultCombinesEntitiesAndMetadata() {
		Map<String, SearchFacet> facets = Map.of("countries", SearchFacet.term("country", 5));
		SearchResult<TsAirport> result = template.findBySearch(TsAirport.class)
				.withIndex(INDEX_NAME)
				.withFacets(facets)
				.withLimit(3)
				.matching(airportSearch(SearchQuery.matchAll()))
				.result();

		assertNotNull(result);
		assertEquals(3, result.entities().size(), "Should have 3 hydrated entities");
		assertTrue(result.totalRows() > 3, "Total rows should exceed the limit");
		assertNotNull(result.metaData(), "Metadata should be present");
		assertFalse(result.facets().isEmpty(), "Facet results should be present");
		assertTrue(result.facets().containsKey("countries"));
	}

	@Test
	void searchWithSortByScoreDescending() {
		List<SearchRow> rows = template.findBySearch(TsAirport.class)
				.withIndex(INDEX_NAME)
				.withSort(SearchSort.byScore().desc(true))
				.withLimit(10)
				.matching(airportSearch(SearchQuery.match("international").field("airportname")))
				.rows();

		assertFalse(rows.isEmpty());
		for (int i = 1; i < rows.size(); i++) {
			assertTrue(rows.get(i - 1).score() >= rows.get(i).score(),
					"Results should be sorted by score descending");
		}
	}

	@Test
	void reactiveSearchReturnsHydratedEntities() {
		List<TsAirport> results = reactiveTemplate.findBySearch(TsAirport.class)
				.withIndex(INDEX_NAME)
				.withLimit(5)
				.matching(airportSearch(SearchQuery.match("United Kingdom").field("country")))
				.all()
				.collectList()
				.block();

		assertNotNull(results);
		assertFalse(results.isEmpty(), "Expected UK airports");
		for (TsAirport airport : results) {
			assertEquals("United Kingdom", airport.country);
		}
	}

	// --- Domain entity for travel-sample airports ---

	@Scope("inventory")
	@Collection("airport")
	static class TsAirport {
		@Id String key;
		String airportname;
		String city;
		String country;
		String faa;
		String icao;
		String tz;
	}

	// --- Helpers ---

	private static void ensureFtsIndex(Cluster cluster) {
		com.couchbase.client.java.Scope scope = cluster.bucket(TS_BUCKET).scope("inventory");
		try {
			scope.searchIndexes().getIndex(INDEX_NAME);
			LOGGER.info("FTS index '{}' already exists, reusing.", INDEX_NAME);
		} catch (com.couchbase.client.core.error.IndexNotFoundException ex) {
			SearchIndex index = new SearchIndex(INDEX_NAME, TS_BUCKET);
			scope.searchIndexes().upsertIndex(index);
			LOGGER.info("Created FTS index '{}'", INDEX_NAME);
		}
		waitForFtsIndex(scope);
	}

	private static void waitForFtsIndex(com.couchbase.client.java.Scope scope) {
		// Phase 1: wait for the index to be queryable
		for (int i = 0; i < 30; i++) {
			try {
				scope.search(INDEX_NAME, SearchRequest.create(SearchQuery.matchAll()));
				break;
			} catch (Exception ex) {
				if (i == 29)
					throw new RuntimeException("FTS index did not become queryable in time", ex);
				sleepMs(2000);
			}
		}
		// Phase 2: wait for documents to be indexed
		for (int i = 0; i < 60; i++) {
			try {
				long docCount = scope.searchIndexes().getIndexedDocumentsCount(INDEX_NAME);
				if (docCount > 0) {
					LOGGER.info("FTS index '{}' ready with {} indexed documents", INDEX_NAME, docCount);
					return;
				}
			} catch (Exception ignored) {}
			sleepMs(2000);
		}
		LOGGER.warn("FTS index '{}' may not have finished indexing", INDEX_NAME);
	}
}
